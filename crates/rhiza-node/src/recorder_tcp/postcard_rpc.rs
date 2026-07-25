use std::{
    fmt,
    future::Future,
    sync::{mpsc, Arc},
    thread,
    time::{Duration, Instant},
};

use postcard_rpc::{
    endpoint,
    header::{VarHeader, VarKey, VarSeq, VarSeqKind},
    host_client::{HostClient, HostErr, WireRx, WireSpawn, WireTx},
    standard_icd::{WireError, ERROR_PATH},
    Endpoint,
};
use rhiza_core::{LogHash, StoredCommand};
use rhiza_quepaxa::{
    DecisionProof, Error, Membership, ReadFenceObservation, ReadFenceRequest, RecordRequest,
    RecordSummary, RecorderRpc,
};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use super::{
    decode_exact, frame_length, read_frame_async, response_matches, response_operation,
    write_value_async_with_timeout, Hello, HelloReply, Operation, RecorderRequestBody,
    RecorderResponseBody, RecorderTlsClientConfig, RecorderTlsServerConfig, CALL_TIMEOUT,
    CONNECT_TIMEOUT, MAX_SERVER_CONNECTIONS, WIRE_VERSION,
};
use crate::{
    map_quorum_record_transport_error, validate_recorder_tcp_endpoint, PeerConfig,
    DEFAULT_PEER_CONCURRENCY, QUORUM_RECORD_REQUEST_TIMEOUT, READ_FENCE_REQUEST_TIMEOUT,
};

const POSTCARD_RPC_WIRE_VERSION: u16 = WIRE_VERSION + 1;
const POSTCARD_RPC_TLS_ALPN: &[u8] = b"rhiza-recorder-prpc/3";
const LANE_IN_FLIGHT: usize = 8;
const BRIDGE_DEPTH: usize = 128;

type OpaqueRequest = (u32, Vec<u8>);
type OpaqueResponse = Vec<u8>;

endpoint!(
    IdentityEndpoint,
    OpaqueRequest,
    OpaqueResponse,
    "rhiza/recorder/private/v2/identity"
);
endpoint!(
    StoreCommandEndpoint,
    OpaqueRequest,
    OpaqueResponse,
    "rhiza/recorder/private/v2/store-command"
);
endpoint!(
    FetchCommandEndpoint,
    OpaqueRequest,
    OpaqueResponse,
    "rhiza/recorder/private/v2/fetch-command"
);
endpoint!(
    RecordEndpoint,
    OpaqueRequest,
    OpaqueResponse,
    "rhiza/recorder/private/v2/record"
);
endpoint!(
    InstallDecisionProofEndpoint,
    OpaqueRequest,
    OpaqueResponse,
    "rhiza/recorder/private/v2/install-decision-proof"
);
endpoint!(
    InspectDecisionProofEndpoint,
    OpaqueRequest,
    OpaqueResponse,
    "rhiza/recorder/private/v2/inspect-decision-proof"
);
endpoint!(
    InspectRecordSummaryEndpoint,
    OpaqueRequest,
    OpaqueResponse,
    "rhiza/recorder/private/v2/inspect-record-summary"
);
endpoint!(
    ObserveReadFenceEndpoint,
    OpaqueRequest,
    OpaqueResponse,
    "rhiza/recorder/private/v3/read-fence"
);

#[derive(Clone)]
pub struct RecorderPostcardRpcTlsServerConfig {
    inner: Arc<rustls::ServerConfig>,
}

impl fmt::Debug for RecorderPostcardRpcTlsServerConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecorderPostcardRpcTlsServerConfig")
            .finish_non_exhaustive()
    }
}

impl RecorderPostcardRpcTlsServerConfig {
    pub fn from_pem(certificate_chain_pem: &[u8], private_key_pem: &[u8]) -> Result<Self, String> {
        let framed = RecorderTlsServerConfig::from_pem(certificate_chain_pem, private_key_pem)?;
        let mut config = (*framed.inner).clone();
        config.alpn_protocols = vec![POSTCARD_RPC_TLS_ALPN.to_vec()];
        Ok(Self {
            inner: Arc::new(config),
        })
    }
}

#[derive(Clone)]
pub struct RecorderPostcardRpcTlsClientConfig {
    inner: Arc<rustls::ClientConfig>,
    server_name: rustls::pki_types::ServerName<'static>,
}

impl fmt::Debug for RecorderPostcardRpcTlsClientConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecorderPostcardRpcTlsClientConfig")
            .field("server_name", &self.server_name)
            .finish_non_exhaustive()
    }
}

impl RecorderPostcardRpcTlsClientConfig {
    pub fn from_ca_pem(ca_bundle_pem: &[u8], server_name: &str) -> Result<Self, String> {
        let framed = RecorderTlsClientConfig::from_ca_pem(ca_bundle_pem, server_name)?;
        let mut config = (*framed.inner).clone();
        config.alpn_protocols = vec![POSTCARD_RPC_TLS_ALPN.to_vec()];
        Ok(Self {
            inner: Arc::new(config),
            server_name: framed.server_name,
        })
    }
}

pub async fn serve_recorder_postcard_rpc<R, F>(
    listener: tokio::net::TcpListener,
    recorder: R,
    peers: Vec<PeerConfig>,
    recovery_generation: u64,
    shutdown: F,
) -> Result<(), String>
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
    F: Future<Output = ()> + Send,
{
    serve_recorder_postcard_rpc_inner(
        listener,
        recorder,
        peers,
        recovery_generation,
        None,
        shutdown,
    )
    .await
}

pub async fn serve_recorder_postcard_rpc_tls<R, F>(
    listener: tokio::net::TcpListener,
    recorder: R,
    peers: Vec<PeerConfig>,
    recovery_generation: u64,
    tls: RecorderPostcardRpcTlsServerConfig,
    shutdown: F,
) -> Result<(), String>
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
    F: Future<Output = ()> + Send,
{
    serve_recorder_postcard_rpc_inner(
        listener,
        recorder,
        peers,
        recovery_generation,
        Some(tls.inner),
        shutdown,
    )
    .await
}

async fn serve_recorder_postcard_rpc_inner<R, F>(
    listener: tokio::net::TcpListener,
    recorder: R,
    peers: Vec<PeerConfig>,
    recovery_generation: u64,
    tls: Option<Arc<rustls::ServerConfig>>,
    shutdown: F,
) -> Result<(), String>
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
    F: Future<Output = ()> + Send,
{
    let peers: Arc<[PeerConfig]> = peers.into();
    let slots = Arc::new(tokio::sync::Semaphore::new(DEFAULT_PEER_CONCURRENCY));
    let connections = Arc::new(tokio::sync::Semaphore::new(MAX_SERVER_CONNECTIONS));
    let mut tasks = tokio::task::JoinSet::new();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            () = &mut shutdown => break,
            completed = tasks.join_next(), if !tasks.is_empty() => {
                if let Some(Err(error)) = completed {
                    tracing::warn!(%error, "recorder postcard-rpc connection task failed");
                }
            }
            accepted = listener.accept() => {
                let (stream, peer_address) = accepted
                    .map_err(|error| format!("recorder postcard-rpc TCP accept failed: {error}"))?;
                let Ok(connection) = connections.clone().try_acquire_owned() else {
                    continue;
                };
                let _ = stream.set_nodelay(true);
                let recorder = recorder.clone();
                let peers = peers.clone();
                let slots = Arc::clone(&slots);
                let tls = tls.clone();
                tasks.spawn(async move {
                    let _connection = connection;
                    let result = async {
                        if let Some(config) = tls {
                            let acceptor = tokio_rustls::TlsAcceptor::from(config);
                            let tls_stream =
                                tokio::time::timeout(CONNECT_TIMEOUT, acceptor.accept(stream))
                                    .await
                                    .map_err(|_| {
                                        "recorder postcard-rpc TLS handshake timed out".to_string()
                                    })?
                                    .map_err(|_| {
                                        "recorder postcard-rpc TLS handshake failed".to_string()
                                    })?;
                            if tls_stream.get_ref().1.alpn_protocol()
                                != Some(POSTCARD_RPC_TLS_ALPN)
                            {
                                return Err(
                                    "recorder postcard-rpc TLS ALPN negotiation failed".to_string(),
                                );
                            }
                            serve_postcard_rpc_connection(
                                tls_stream,
                                recorder,
                                peers,
                                recovery_generation,
                                slots,
                            )
                            .await
                        } else {
                            serve_postcard_rpc_connection(
                                stream,
                                recorder,
                                peers,
                                recovery_generation,
                                slots,
                            )
                            .await
                        }
                    }
                    .await;
                    if let Err(error) = &result {
                        tracing::debug!(
                            peer = %peer_address,
                            %error,
                            "recorder postcard-rpc connection closed"
                        );
                    }
                    result
                });
            }
        }
    }
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    let _drained = slots
        .acquire_many_owned(u32::try_from(DEFAULT_PEER_CONCURRENCY).unwrap_or(u32::MAX))
        .await
        .map_err(|_| "recorder operation semaphore closed during shutdown".to_string())?;
    Ok(())
}

async fn serve_postcard_rpc_connection<R, S>(
    mut stream: S,
    recorder: R,
    peers: Arc<[PeerConfig]>,
    recovery_generation: u64,
    slots: Arc<tokio::sync::Semaphore>,
) -> Result<(), String>
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let hello_bytes = tokio::time::timeout(CALL_TIMEOUT, read_frame_async(&mut stream))
        .await
        .map_err(|_| "recorder postcard-rpc HELLO timed out".to_string())??;
    let hello: Hello = decode_exact(&hello_bytes)?;
    if hello.version != POSTCARD_RPC_WIRE_VERSION
        || hello.recovery_generation != recovery_generation
        || !crate::peer_credentials_authenticated(&hello.node_id, &hello.token, &peers)
    {
        let _ = write_value_async_with_timeout(
            &mut stream,
            &HelloReply::Rejected,
            "recorder postcard-rpc HELLO rejection",
        )
        .await;
        return Err("recorder postcard-rpc HELLO rejected".into());
    }
    let identity_recorder = recorder.clone();
    let recorder_id = tokio::task::spawn_blocking(move || identity_recorder.recorder_id())
        .await
        .map_err(|error| format!("recorder identity task failed: {error}"))?
        .map_err(|error| error.to_string())?;
    write_value_async_with_timeout(
        &mut stream,
        &HelloReply::Accepted {
            version: POSTCARD_RPC_WIRE_VERSION,
            recorder_id,
        },
        "recorder postcard-rpc HELLO response",
    )
    .await?;
    let authenticated_peer_id = hello.node_id;

    let (mut reader, writer) = tokio::io::split(stream);
    let writer = Arc::new(tokio::sync::Mutex::new(writer));
    let mut calls = tokio::task::JoinSet::new();
    loop {
        while let Some(completed) = calls.try_join_next() {
            match completed {
                Ok(Ok(())) => {}
                Ok(Err(error)) => return Err(error),
                Err(error) => {
                    return Err(format!(
                        "recorder postcard-rpc response task failed: {error}"
                    ));
                }
            }
        }
        // A framed read consumes a length prefix and then its payload. Keeping
        // it outside `select!` prevents response completion from cancelling a
        // partially consumed frame and desynchronizing the connection.
        let bytes = match read_frame_async(&mut reader).await {
            Ok(bytes) => bytes,
            Err(error) if error == "connection closed" => return Ok(()),
            Err(error) => return Err(error),
        };
        let (header, payload) = VarHeader::take_from_slice(&bytes)
            .ok_or_else(|| "invalid recorder postcard-rpc header".to_string())?;
        if !matches!(header.seq_no, VarSeq::Seq4(_)) {
            return Err("recorder postcard-rpc requires Seq4".into());
        }
        let operation = operation_for_key(header.key)
            .ok_or_else(|| "unknown recorder postcard-rpc endpoint".to_string())?;
        let request: OpaqueRequest = decode_exact(payload)?;
        if request.0 == 0 {
            return Err("invalid recorder postcard-rpc deadline".into());
        }
        let dispatch_timeout = Duration::from_millis(u64::from(request.0)).min(CALL_TIMEOUT);
        let body: RecorderRequestBody = decode_exact(&request.1)?;
        if response_operation(&body) != operation {
            return Err("recorder postcard-rpc endpoint payload mismatch".into());
        }
        let request_seq = header.seq_no;
        let writer = Arc::clone(&writer);
        let permit = match slots.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                send_response(
                    &writer,
                    operation,
                    request_seq,
                    super::overloaded_response(operation),
                )
                .await?;
                continue;
            }
        };
        let call_recorder = recorder.clone();
        let call_authenticated_peer_id = authenticated_peer_id.clone();
        let call_peers = Arc::clone(&peers);
        calls.spawn(async move {
            let dispatched = tokio::task::spawn_blocking(move || {
                let _permit = permit;
                super::dispatch(
                    call_recorder,
                    body,
                    &call_authenticated_peer_id,
                    &call_peers,
                )
            });
            let response = match tokio::time::timeout(dispatch_timeout, dispatched).await {
                Ok(Ok(response)) => response,
                Ok(Err(error)) => super::error_response(operation, error.to_string()),
                Err(_) => super::error_response(operation, "recorder RPC deadline exceeded".into()),
            };
            send_response(&writer, operation, request_seq, response).await
        });
    }
}

async fn send_response<W>(
    writer: &Arc<tokio::sync::Mutex<W>>,
    operation: Operation,
    seq_no: VarSeq,
    body: RecorderResponseBody,
) -> Result<(), String>
where
    W: AsyncWrite + Unpin,
{
    let response = postcard::to_allocvec(&body).map_err(|error| error.to_string())?;
    let mut frame = VarHeader {
        key: VarKey::Key8(response_key(operation)),
        seq_no,
    }
    .write_to_vec();
    frame.extend_from_slice(&postcard::to_allocvec(&response).map_err(|error| error.to_string())?);
    let mut writer = writer.lock().await;
    tokio::time::timeout(CALL_TIMEOUT, write_raw_frame(&mut *writer, &frame))
        .await
        .map_err(|_| "recorder postcard-rpc response timed out".to_string())?
}

async fn write_raw_frame<W: AsyncWrite + Unpin>(
    writer: &mut W,
    frame: &[u8],
) -> Result<(), String> {
    let length = frame_length(frame)?;
    writer
        .write_all(&length)
        .await
        .map_err(|error| error.to_string())?;
    writer
        .write_all(frame)
        .await
        .map_err(|error| error.to_string())?;
    writer.flush().await.map_err(|error| error.to_string())
}

fn operation_for_key(key: VarKey) -> Option<Operation> {
    // The generated postcard-rpc dispatcher also installs its standard ICD
    // endpoints and awaits blocking handlers inline. This private protocol must
    // expose exactly these eight endpoints and keep reading while operations run,
    // so a small key dispatcher is the narrower, concurrently correct fit.
    [
        Operation::Identity,
        Operation::StoreCommand,
        Operation::FetchCommand,
        Operation::Record,
        Operation::InstallDecisionProof,
        Operation::InspectDecisionProof,
        Operation::InspectRecordSummary,
        Operation::ObserveReadFence,
    ]
    .into_iter()
    .find(|operation| key == VarKey::Key8(request_key(*operation)))
}

fn request_key(operation: Operation) -> postcard_rpc::Key {
    match operation {
        Operation::Identity => IdentityEndpoint::REQ_KEY,
        Operation::StoreCommand => StoreCommandEndpoint::REQ_KEY,
        Operation::FetchCommand => FetchCommandEndpoint::REQ_KEY,
        Operation::Record => RecordEndpoint::REQ_KEY,
        Operation::InstallDecisionProof => InstallDecisionProofEndpoint::REQ_KEY,
        Operation::InspectDecisionProof => InspectDecisionProofEndpoint::REQ_KEY,
        Operation::InspectRecordSummary => InspectRecordSummaryEndpoint::REQ_KEY,
        Operation::ObserveReadFence => ObserveReadFenceEndpoint::REQ_KEY,
    }
}

fn response_key(operation: Operation) -> postcard_rpc::Key {
    match operation {
        Operation::Identity => IdentityEndpoint::RESP_KEY,
        Operation::StoreCommand => StoreCommandEndpoint::RESP_KEY,
        Operation::FetchCommand => FetchCommandEndpoint::RESP_KEY,
        Operation::Record => RecordEndpoint::RESP_KEY,
        Operation::InstallDecisionProof => InstallDecisionProofEndpoint::RESP_KEY,
        Operation::InspectDecisionProof => InspectDecisionProofEndpoint::RESP_KEY,
        Operation::InspectRecordSummary => InspectRecordSummaryEndpoint::RESP_KEY,
        Operation::ObserveReadFence => ObserveReadFenceEndpoint::RESP_KEY,
    }
}

trait AsyncIo: AsyncRead + AsyncWrite {}
impl<T: AsyncRead + AsyncWrite + ?Sized> AsyncIo for T {}
type BoxedIo = Box<dyn AsyncIo + Send + Unpin>;

#[derive(Debug)]
struct WireFailure(String);

impl fmt::Display for WireFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for WireFailure {}

struct FrameTx {
    writer: tokio::io::WriteHalf<BoxedIo>,
}

impl WireTx for FrameTx {
    type Error = WireFailure;

    async fn send(&mut self, data: Vec<u8>) -> Result<(), Self::Error> {
        tokio::time::timeout(CALL_TIMEOUT, write_raw_frame(&mut self.writer, &data))
            .await
            .map_err(|_| WireFailure("recorder postcard-rpc frame send timed out".into()))?
            .map_err(WireFailure)
    }
}

struct FrameRx {
    reader: tokio::io::ReadHalf<BoxedIo>,
}

impl WireRx for FrameRx {
    type Error = WireFailure;

    async fn receive(&mut self) -> Result<Vec<u8>, Self::Error> {
        read_frame_async(&mut self.reader)
            .await
            .map_err(WireFailure)
    }
}

struct TokioSpawner;

impl WireSpawn for TokioSpawner {
    fn spawn(&mut self, future: impl Future<Output = ()> + Send + 'static) {
        tokio::spawn(future);
    }
}

#[derive(Clone)]
enum ClientTransport {
    Plain,
    Tls(RecorderPostcardRpcTlsClientConfig),
}

#[derive(Clone)]
struct ConnectionConfig {
    address: String,
    expected_recorder_id: String,
    local_node_id: String,
    peer_token: String,
    recovery_generation: u64,
    transport: ClientTransport,
}

struct BridgeRequest {
    body: RecorderRequestBody,
    operation: Operation,
    deadline: Instant,
    reply: mpsc::SyncSender<rhiza_quepaxa::Result<RecorderResponseBody>>,
}

struct CompletedCall {
    session_id: u64,
    result: rhiza_quepaxa::Result<RecorderResponseBody>,
    wire_failed: bool,
    reply: mpsc::SyncSender<rhiza_quepaxa::Result<RecorderResponseBody>>,
}

struct Lane {
    sender: tokio::sync::mpsc::Sender<BridgeRequest>,
}

pub struct TcpPostcardRpcRecorderClient {
    address: String,
    expected_recorder_id: String,
    local_node_id: String,
    recovery_generation: u64,
    call_timeout: Duration,
    transport_name: &'static str,
    consensus: Lane,
    control: Lane,
}

impl fmt::Debug for TcpPostcardRpcRecorderClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TcpPostcardRpcRecorderClient")
            .field("address", &self.address)
            .field("expected_recorder_id", &self.expected_recorder_id)
            .field("local_node_id", &self.local_node_id)
            .field("peer_token", &"[redacted]")
            .field("recovery_generation", &self.recovery_generation)
            .field("transport", &self.transport_name)
            .finish()
    }
}

impl TcpPostcardRpcRecorderClient {
    pub fn new(
        address: impl ToString,
        expected_recorder_id: impl Into<String>,
        local_node_id: impl Into<String>,
        peer_token: impl Into<String>,
        recovery_generation: u64,
    ) -> Result<Self, String> {
        Self::new_with_transport(
            address,
            expected_recorder_id,
            local_node_id,
            peer_token,
            recovery_generation,
            ClientTransport::Plain,
        )
    }

    pub fn new_tls(
        address: impl ToString,
        expected_recorder_id: impl Into<String>,
        local_node_id: impl Into<String>,
        peer_token: impl Into<String>,
        recovery_generation: u64,
        tls: RecorderPostcardRpcTlsClientConfig,
    ) -> Result<Self, String> {
        Self::new_with_transport(
            address,
            expected_recorder_id,
            local_node_id,
            peer_token,
            recovery_generation,
            ClientTransport::Tls(tls),
        )
    }

    fn new_with_transport(
        address: impl ToString,
        expected_recorder_id: impl Into<String>,
        local_node_id: impl Into<String>,
        peer_token: impl Into<String>,
        recovery_generation: u64,
        transport: ClientTransport,
    ) -> Result<Self, String> {
        Self::new_with_transport_and_timeout(
            address,
            expected_recorder_id,
            local_node_id,
            peer_token,
            recovery_generation,
            transport,
            CALL_TIMEOUT,
        )
    }

    fn new_with_transport_and_timeout(
        address: impl ToString,
        expected_recorder_id: impl Into<String>,
        local_node_id: impl Into<String>,
        peer_token: impl Into<String>,
        recovery_generation: u64,
        transport: ClientTransport,
        call_timeout: Duration,
    ) -> Result<Self, String> {
        let address = address.to_string();
        validate_recorder_tcp_endpoint(&address)?;
        let expected_recorder_id = expected_recorder_id.into();
        let local_node_id = local_node_id.into();
        let peer_token = peer_token.into();
        if expected_recorder_id.trim().is_empty()
            || local_node_id.trim().is_empty()
            || peer_token.trim().is_empty()
            || recovery_generation == 0
        {
            return Err("invalid recorder postcard-rpc client identity".into());
        }
        let config = ConnectionConfig {
            address: address.clone(),
            expected_recorder_id: expected_recorder_id.clone(),
            local_node_id: local_node_id.clone(),
            peer_token,
            recovery_generation,
            transport: transport.clone(),
        };
        let consensus = spawn_lane(config.clone(), "consensus")?;
        let control = spawn_lane(config, "control")?;
        Ok(Self {
            address,
            expected_recorder_id,
            local_node_id,
            recovery_generation,
            call_timeout,
            transport_name: match transport {
                ClientTransport::Plain => "plain",
                ClientTransport::Tls(_) => "tls",
            },
            consensus,
            control,
        })
    }

    fn exchange(
        &self,
        body: RecorderRequestBody,
        consensus: bool,
    ) -> rhiza_quepaxa::Result<RecorderResponseBody> {
        self.exchange_with_timeout(body, consensus, self.call_timeout)
    }

    fn exchange_with_timeout(
        &self,
        body: RecorderRequestBody,
        consensus: bool,
        timeout: Duration,
    ) -> rhiza_quepaxa::Result<RecorderResponseBody> {
        let deadline = Instant::now() + timeout.min(self.call_timeout);
        let operation = response_operation(&body);
        let (reply, receive) = mpsc::sync_channel(1);
        let lane = if consensus {
            &self.consensus
        } else {
            &self.control
        };
        lane.sender
            .try_send(BridgeRequest {
                body,
                operation,
                deadline,
                reply,
            })
            .map_err(|error| match error {
                tokio::sync::mpsc::error::TrySendError::Full(_) => {
                    Error::Io("recorder postcard-rpc bridge overloaded".into())
                }
                tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                    Error::Io("recorder postcard-rpc worker closed".into())
                }
            })?;
        let remaining = deadline.saturating_duration_since(Instant::now());
        receive
            .recv_timeout(remaining)
            .map_err(|error| match error {
                mpsc::RecvTimeoutError::Timeout => {
                    Error::Io("recorder postcard-rpc deadline exceeded".into())
                }
                mpsc::RecvTimeoutError::Disconnected => {
                    Error::Io("recorder postcard-rpc worker closed".into())
                }
            })?
    }
}

fn spawn_lane(config: ConnectionConfig, name: &str) -> Result<Lane, String> {
    let (sender, receiver) = tokio::sync::mpsc::channel(BRIDGE_DEPTH);
    let (ready_tx, ready_rx) = mpsc::sync_channel(1);
    thread::Builder::new()
        .name(format!("rhiza-recorder-prpc-{name}"))
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|error| error.to_string());
            match runtime {
                Ok(runtime) => {
                    let _ = ready_tx.send(Ok(()));
                    runtime.block_on(run_lane(config, receiver));
                }
                Err(error) => {
                    let _ = ready_tx.send(Err(error));
                }
            }
        })
        .map_err(|error| format!("cannot start recorder postcard-rpc worker: {error}"))?;
    ready_rx
        .recv()
        .map_err(|_| "recorder postcard-rpc worker failed to start".to_string())??;
    Ok(Lane { sender })
}

async fn run_lane(
    config: ConnectionConfig,
    mut receiver: tokio::sync::mpsc::Receiver<BridgeRequest>,
) {
    let mut session: Option<(u64, HostClient<WireError>)> = None;
    let mut next_session_id = 1_u64;
    let mut calls = tokio::task::JoinSet::new();
    loop {
        if calls.len() >= LANE_IN_FLIGHT {
            if let Some(completed) = calls.join_next().await {
                finish_call(completed, &mut session);
            }
            continue;
        }
        tokio::select! {
            completed = calls.join_next(), if !calls.is_empty() => {
                if let Some(completed) = completed {
                    finish_call(completed, &mut session);
                }
            }
            request = receiver.recv() => {
                let Some(request) = request else { break };
                if request.deadline <= Instant::now() {
                    let _ = request.reply.send(Err(Error::Io(
                        "recorder postcard-rpc deadline exceeded".into(),
                    )));
                    continue;
                }
                if session.as_ref().is_some_and(|(_, client)| client.is_closed()) {
                    session = None;
                }
                if session.is_none() {
                    match connect_session(&config, request.deadline).await {
                        Ok(connected) => {
                            session = Some((next_session_id, connected));
                            next_session_id = next_session_id.wrapping_add(1);
                        }
                        Err(error) => {
                            let _ = request.reply.send(Err(Error::Io(error)));
                            continue;
                        }
                    }
                }
                let (session_id, client) = session.as_ref().expect("session established");
                calls.spawn(run_call(*session_id, client.clone(), request));
            }
        }
    }
    if let Some((_, session)) = session {
        session.close();
    }
    calls.abort_all();
}

fn finish_call(
    completed: Result<CompletedCall, tokio::task::JoinError>,
    session: &mut Option<(u64, HostClient<WireError>)>,
) {
    match completed {
        Ok(completed) => {
            if completed.wire_failed
                && session
                    .as_ref()
                    .is_some_and(|(session_id, _)| *session_id == completed.session_id)
            {
                if let Some((_, client)) = session.take() {
                    client.close();
                }
            }
            let _ = completed.reply.send(completed.result);
        }
        Err(_) => {
            if let Some((_, client)) = session.take() {
                client.close();
            }
        }
    }
}

async fn run_call(
    session_id: u64,
    client: HostClient<WireError>,
    request: BridgeRequest,
) -> CompletedCall {
    let payload = match postcard::to_allocvec(&request.body) {
        Ok(payload) => payload,
        Err(error) => {
            return CompletedCall {
                session_id,
                result: Err(Error::Decode(error.to_string())),
                wire_failed: false,
                reply: request.reply,
            };
        }
    };
    if request.deadline <= Instant::now() {
        return CompletedCall {
            session_id,
            result: Err(Error::Io("recorder postcard-rpc deadline exceeded".into())),
            wire_failed: false,
            reply: request.reply,
        };
    }
    let remaining = request.deadline.saturating_duration_since(Instant::now());
    let opaque = (
        u32::try_from(remaining.as_millis())
            .unwrap_or(u32::MAX)
            .max(1),
        payload,
    );
    let future = send_endpoint(&client, request.operation, &opaque);
    let result = tokio::time::timeout_at(request.deadline.into(), future).await;
    let (result, wire_failed) = match result {
        Err(_) => (
            Err(Error::Io("recorder postcard-rpc deadline exceeded".into())),
            true,
        ),
        Ok(Err(HostErr::Postcard(error))) => (Err(Error::Decode(error.to_string())), false),
        Ok(Err(error)) => (Err(Error::Io(error.to_string())), true),
        Ok(Ok(response)) => match decode_exact::<RecorderResponseBody>(&response) {
            Ok(body) if response_matches(request.operation, &body) => (Ok(body), false),
            Ok(_) => (
                Err(Error::Decode(
                    "recorder postcard-rpc response operation mismatch".into(),
                )),
                true,
            ),
            Err(error) => (Err(Error::Decode(error)), true),
        },
    };
    CompletedCall {
        session_id,
        result,
        wire_failed,
        reply: request.reply,
    }
}

async fn send_endpoint(
    client: &HostClient<WireError>,
    operation: Operation,
    request: &OpaqueRequest,
) -> Result<OpaqueResponse, HostErr<WireError>> {
    // postcard-rpc 0.12.1 typed send_resp always originates Seq4, regardless of
    // HostClientConfig::seq_kind. The server intentionally accepts only Seq4.
    match operation {
        Operation::Identity => client.send_resp::<IdentityEndpoint>(request).await,
        Operation::StoreCommand => client.send_resp::<StoreCommandEndpoint>(request).await,
        Operation::FetchCommand => client.send_resp::<FetchCommandEndpoint>(request).await,
        Operation::Record => client.send_resp::<RecordEndpoint>(request).await,
        Operation::InstallDecisionProof => {
            client
                .send_resp::<InstallDecisionProofEndpoint>(request)
                .await
        }
        Operation::InspectDecisionProof => {
            client
                .send_resp::<InspectDecisionProofEndpoint>(request)
                .await
        }
        Operation::InspectRecordSummary => {
            client
                .send_resp::<InspectRecordSummaryEndpoint>(request)
                .await
        }
        Operation::ObserveReadFence => client.send_resp::<ObserveReadFenceEndpoint>(request).await,
    }
}

async fn connect_session(
    config: &ConnectionConfig,
    deadline: Instant,
) -> Result<HostClient<WireError>, String> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err("recorder postcard-rpc connect deadline exceeded".into());
    }
    let addresses = tokio::time::timeout(
        remaining.min(CONNECT_TIMEOUT),
        tokio::net::lookup_host(&config.address),
    )
    .await
    .map_err(|_| "recorder postcard-rpc address resolution timed out".to_string())?
    .map_err(|error| format!("cannot resolve recorder TCP address: {error}"))?
    .collect::<Vec<_>>();
    if addresses.is_empty() {
        return Err("recorder TCP address resolved to no endpoints".into());
    }
    let mut last_error = None;
    let mut socket = None;
    for address in addresses {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(
            remaining.min(CONNECT_TIMEOUT),
            tokio::net::TcpStream::connect(address),
        )
        .await
        {
            Ok(Ok(connected)) => {
                socket = Some(connected);
                break;
            }
            Ok(Err(error)) => last_error = Some(error.to_string()),
            Err(_) => last_error = Some("deadline exceeded".into()),
        }
    }
    let socket = socket.ok_or_else(|| {
        format!(
            "recorder TCP connect failed: {}",
            last_error.unwrap_or_else(|| "deadline exceeded".into())
        )
    })?;
    socket
        .set_nodelay(true)
        .map_err(|error| format!("cannot set recorder TCP_NODELAY: {error}"))?;
    let mut stream: BoxedIo = match &config.transport {
        ClientTransport::Plain => Box::new(socket),
        ClientTransport::Tls(tls) => {
            let connector = tokio_rustls::TlsConnector::from(Arc::clone(&tls.inner));
            let remaining = deadline.saturating_duration_since(Instant::now());
            let stream = tokio::time::timeout(
                remaining,
                connector.connect(tls.server_name.clone(), socket),
            )
            .await
            .map_err(|_| "recorder postcard-rpc TLS handshake timed out".to_string())?
            .map_err(|_| "recorder postcard-rpc TLS handshake failed".to_string())?;
            if stream.get_ref().1.alpn_protocol() != Some(POSTCARD_RPC_TLS_ALPN) {
                return Err("recorder postcard-rpc TLS ALPN negotiation failed".into());
            }
            Box::new(stream)
        }
    };
    let remaining = deadline.saturating_duration_since(Instant::now());
    tokio::time::timeout(
        remaining,
        super::write_value_async(
            &mut stream,
            &Hello {
                version: POSTCARD_RPC_WIRE_VERSION,
                node_id: config.local_node_id.clone(),
                recovery_generation: config.recovery_generation,
                token: config.peer_token.clone(),
            },
        ),
    )
    .await
    .map_err(|_| "recorder postcard-rpc HELLO timed out".to_string())??;
    let remaining = deadline.saturating_duration_since(Instant::now());
    let reply = tokio::time::timeout(remaining, read_frame_async(&mut stream))
        .await
        .map_err(|_| "recorder postcard-rpc HELLO timed out".to_string())??;
    let reply: HelloReply = decode_exact(&reply)?;
    match reply {
        HelloReply::Accepted {
            version,
            recorder_id,
        } if version == POSTCARD_RPC_WIRE_VERSION && recorder_id == config.expected_recorder_id => {
        }
        HelloReply::Accepted { .. } => return Err("recorder postcard-rpc identity mismatch".into()),
        HelloReply::Rejected => return Err("recorder postcard-rpc HELLO rejected".into()),
    }
    let (reader, writer) = tokio::io::split(stream);
    Ok(HostClient::new_with_wire(
        FrameTx { writer },
        FrameRx { reader },
        TokioSpawner,
        VarSeqKind::Seq4,
        ERROR_PATH,
        LANE_IN_FLIGHT,
    ))
}

impl RecorderRpc for TcpPostcardRpcRecorderClient {
    fn recorder_id(&self) -> rhiza_quepaxa::Result<String> {
        match self.exchange(RecorderRequestBody::Identity, false)? {
            RecorderResponseBody::Identity(result) => result.into_result(),
            _ => Err(Error::Decode("recorder response operation mismatch".into())),
        }
    }

    fn store_command_for(
        &self,
        cluster_id: String,
        epoch: u64,
        config_id: u64,
        config_digest: LogHash,
        command_hash: LogHash,
        command: StoredCommand,
    ) -> rhiza_quepaxa::Result<()> {
        match self.exchange(
            RecorderRequestBody::StoreCommand {
                cluster_id,
                epoch,
                config_id,
                config_digest,
                command_hash,
                command,
            },
            true,
        )? {
            RecorderResponseBody::StoreCommand(result) => result.into_result(),
            _ => Err(Error::Decode("recorder response operation mismatch".into())),
        }
    }

    fn fetch_command_for(
        &self,
        cluster_id: String,
        epoch: u64,
        config_id: u64,
        config_digest: LogHash,
        command_hash: LogHash,
    ) -> rhiza_quepaxa::Result<Option<StoredCommand>> {
        match self.exchange(
            RecorderRequestBody::FetchCommand {
                cluster_id,
                epoch,
                config_id,
                config_digest,
                command_hash,
            },
            false,
        )? {
            RecorderResponseBody::FetchCommand(result) => result.into_result(),
            _ => Err(Error::Decode("recorder response operation mismatch".into())),
        }
    }

    fn record(&self, request: RecordRequest) -> rhiza_quepaxa::Result<RecordSummary> {
        let response = self
            .exchange_with_timeout(
                RecorderRequestBody::Record(request),
                true,
                QUORUM_RECORD_REQUEST_TIMEOUT,
            )
            .map_err(map_quorum_record_transport_error)?;
        match response {
            RecorderResponseBody::Record(result) => result.into_result(),
            _ => Err(Error::Decode("recorder response operation mismatch".into())),
        }
        .map_err(map_quorum_record_transport_error)
    }

    fn install_decision_proof(
        &self,
        proof: DecisionProof,
        membership: &Membership,
    ) -> rhiza_quepaxa::Result<()> {
        match self.exchange(
            RecorderRequestBody::InstallDecisionProof {
                proof,
                members: membership.members().to_vec(),
            },
            true,
        )? {
            RecorderResponseBody::InstallDecisionProof(result) => result.into_result(),
            _ => Err(Error::Decode("recorder response operation mismatch".into())),
        }
    }

    fn inspect_decision_proof(&self, slot: u64) -> rhiza_quepaxa::Result<Option<DecisionProof>> {
        match self.exchange(RecorderRequestBody::InspectDecisionProof { slot }, false)? {
            RecorderResponseBody::InspectDecisionProof(result) => result.into_result(),
            _ => Err(Error::Decode("recorder response operation mismatch".into())),
        }
    }

    fn inspect_record_summary(&self, slot: u64) -> rhiza_quepaxa::Result<Option<RecordSummary>> {
        match self.exchange(RecorderRequestBody::InspectRecordSummary { slot }, false)? {
            RecorderResponseBody::InspectRecordSummary(result) => result.into_result(),
            _ => Err(Error::Decode("recorder response operation mismatch".into())),
        }
    }

    fn supports_context_read_fence(&self) -> bool {
        true
    }

    fn observe_read_fence(
        &self,
        request: ReadFenceRequest,
    ) -> rhiza_quepaxa::Result<ReadFenceObservation> {
        match self.exchange_with_timeout(
            RecorderRequestBody::ObserveReadFence(request),
            false,
            READ_FENCE_REQUEST_TIMEOUT,
        )? {
            RecorderResponseBody::ObserveReadFence(result) => result.into_result(),
            _ => Err(Error::Decode("recorder response operation mismatch".into())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::RpcResult;
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Condvar, Mutex,
    };

    #[derive(Clone)]
    struct SlowFirstInspection;

    impl RecorderRpc for SlowFirstInspection {
        fn recorder_id(&self) -> rhiza_quepaxa::Result<String> {
            Ok("node-1".into())
        }

        fn inspect_record_summary(
            &self,
            slot: u64,
        ) -> rhiza_quepaxa::Result<Option<RecordSummary>> {
            if slot == 1 {
                thread::sleep(Duration::from_millis(250));
            }
            Ok(None)
        }
    }

    #[derive(Clone)]
    struct BlockingMutation {
        started: mpsc::Sender<()>,
        release: Arc<(Mutex<bool>, Condvar)>,
        completed: Arc<AtomicUsize>,
    }

    impl RecorderRpc for BlockingMutation {
        fn recorder_id(&self) -> rhiza_quepaxa::Result<String> {
            Ok("node-1".into())
        }

        fn store_command_for(
            &self,
            _cluster_id: String,
            _epoch: u64,
            _config_id: u64,
            _config_digest: LogHash,
            _command_hash: LogHash,
            _command: StoredCommand,
        ) -> rhiza_quepaxa::Result<()> {
            self.started.send(()).unwrap();
            let (released, ready) = &*self.release;
            let mut released = released.lock().unwrap();
            while !*released {
                released = ready.wait(released).unwrap();
            }
            self.completed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[derive(Clone)]
    struct IdentityRecorder;

    impl RecorderRpc for IdentityRecorder {
        fn recorder_id(&self) -> rhiza_quepaxa::Result<String> {
            Ok("node-1".into())
        }

        fn inspect_record_summary(
            &self,
            _slot: u64,
        ) -> rhiza_quepaxa::Result<Option<RecordSummary>> {
            Ok(None)
        }
    }

    #[derive(Clone)]
    struct BlockingInspections {
        started: mpsc::Sender<u64>,
        release: Arc<(Mutex<bool>, Condvar)>,
        seen: Arc<Mutex<Vec<u64>>>,
        mutations: Arc<AtomicUsize>,
    }

    impl RecorderRpc for BlockingInspections {
        fn recorder_id(&self) -> rhiza_quepaxa::Result<String> {
            Ok("node-1".into())
        }

        fn inspect_record_summary(
            &self,
            slot: u64,
        ) -> rhiza_quepaxa::Result<Option<RecordSummary>> {
            self.seen.lock().unwrap().push(slot);
            if slot <= u64::try_from(LANE_IN_FLIGHT).unwrap_or(u64::MAX) {
                self.started.send(slot).unwrap();
                let (released, ready) = &*self.release;
                let mut released = released.lock().unwrap();
                while !*released {
                    released = ready.wait(released).unwrap();
                }
            }
            Ok(None)
        }

        fn store_command_for(
            &self,
            _cluster_id: String,
            _epoch: u64,
            _config_id: u64,
            _config_digest: LogHash,
            _command_hash: LogHash,
            _command: StoredCommand,
        ) -> rhiza_quepaxa::Result<()> {
            self.mutations.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn peers() -> Vec<PeerConfig> {
        (1..=3)
            .map(|index| {
                PeerConfig::new(
                    format!("node-{index}"),
                    format!("http://node-{index}:8081"),
                    format!("peer-token-{index}"),
                )
                .unwrap()
            })
            .collect()
    }

    #[test]
    fn endpoint_keys_are_unique_and_version_fenced() {
        assert_eq!(POSTCARD_RPC_WIRE_VERSION, 4);
        assert_eq!(POSTCARD_RPC_TLS_ALPN, b"rhiza-recorder-prpc/3");
        let keys = [
            IdentityEndpoint::REQ_KEY,
            StoreCommandEndpoint::REQ_KEY,
            FetchCommandEndpoint::REQ_KEY,
            RecordEndpoint::REQ_KEY,
            InstallDecisionProofEndpoint::REQ_KEY,
            InspectDecisionProofEndpoint::REQ_KEY,
            InspectRecordSummaryEndpoint::REQ_KEY,
            ObserveReadFenceEndpoint::REQ_KEY,
        ];
        for (index, key) in keys.iter().enumerate() {
            assert!(!keys[..index].contains(key));
        }
        assert_ne!(POSTCARD_RPC_WIRE_VERSION, WIRE_VERSION);
        assert_ne!(POSTCARD_RPC_TLS_ALPN, super::super::RECORDER_TLS_ALPN);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn late_response_after_timeout_is_dropped_and_next_call_recovers() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(serve_recorder_postcard_rpc(
            listener,
            SlowFirstInspection,
            peers(),
            7,
            std::future::pending(),
        ));
        let client = Arc::new(
            TcpPostcardRpcRecorderClient::new_with_transport_and_timeout(
                address,
                "node-1",
                "node-2",
                "peer-token-2",
                7,
                ClientTransport::Plain,
                Duration::from_millis(100),
            )
            .unwrap(),
        );
        let timed_out = Arc::clone(&client);
        assert!(
            tokio::task::spawn_blocking(move || timed_out.inspect_record_summary(1))
                .await
                .unwrap()
                .is_err()
        );
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(
            tokio::task::spawn_blocking(move || client.inspect_record_summary(2))
                .await
                .unwrap()
                .unwrap()
                .is_none()
        );
        server.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn postcard_rpc_read_fence_uses_the_short_control_deadline() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let blackhole = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            std::future::pending::<()>().await;
        });
        let client = TcpPostcardRpcRecorderClient::new_with_transport_and_timeout(
            address,
            "node-1",
            "node-2",
            "peer-token-2",
            7,
            ClientTransport::Plain,
            Duration::from_secs(5),
        )
        .unwrap();

        let started = Instant::now();
        let result = tokio::task::spawn_blocking(move || {
            client.observe_read_fence(ReadFenceRequest {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: LogHash::ZERO,
                slot: 1,
            })
        })
        .await
        .unwrap();

        assert!(result.is_err());
        assert!(started.elapsed() < Duration::from_millis(1_500));
        blackhole.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn postcard_rpc_record_transport_failure_releases_the_quorum_attempt_promptly() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let blackhole = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            std::future::pending::<()>().await;
        });
        let client = TcpPostcardRpcRecorderClient::new_with_transport_and_timeout(
            address,
            "node-1",
            "node-2",
            "peer-token-2",
            7,
            ClientTransport::Plain,
            Duration::from_secs(5),
        )
        .unwrap();

        let started = Instant::now();
        let result = tokio::task::spawn_blocking(move || {
            client.record(RecordRequest {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: LogHash::ZERO,
                slot: 1,
                step: 1,
                proposal: rhiza_quepaxa::Proposal::nil(),
                command: None,
            })
        })
        .await
        .unwrap();

        assert!(matches!(result, Err(Error::ProposeFailed)));
        assert!(started.elapsed() < Duration::from_millis(1_500));
        blackhole.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn server_deadline_returns_while_admitted_mutation_finishes_and_shutdown_drains_it() {
        let (started_tx, started_rx) = mpsc::channel();
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let completed = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(serve_recorder_postcard_rpc(
            listener,
            BlockingMutation {
                started: started_tx,
                release: Arc::clone(&release),
                completed: Arc::clone(&completed),
            },
            peers(),
            7,
            async move {
                let _ = shutdown_rx.await;
            },
        ));
        let config = ConnectionConfig {
            address: address.to_string(),
            expected_recorder_id: "node-1".into(),
            local_node_id: "node-2".into(),
            peer_token: "peer-token-2".into(),
            recovery_generation: 7,
            transport: ClientTransport::Plain,
        };
        let client = connect_session(&config, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        let membership = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
        let command = StoredCommand::new(rhiza_core::EntryType::Command, b"slow".to_vec());
        let request = RecorderRequestBody::StoreCommand {
            cluster_id: "rhiza:sql:cluster-a".into(),
            epoch: 1,
            config_id: 1,
            config_digest: membership.digest(),
            command_hash: command.hash(),
            command,
        };
        let opaque = (50, postcard::to_allocvec(&request).unwrap());
        let response = tokio::time::timeout(
            Duration::from_millis(300),
            send_endpoint(&client, Operation::StoreCommand, &opaque),
        )
        .await;
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        shutdown_tx.send(()).unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!server.is_finished());
        let (released, ready) = &*release;
        *released.lock().unwrap() = true;
        ready.notify_all();
        server.await.unwrap().unwrap();
        assert_eq!(completed.load(Ordering::SeqCst), 1);
        client.close();
        let response = response
            .expect("server must answer the advertised deadline")
            .unwrap();
        assert!(matches!(
            decode_exact::<RecorderResponseBody>(&response).unwrap(),
            RecorderResponseBody::StoreCommand(RpcResult::Error(message))
                if message.contains("deadline")
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn permanent_blackhole_is_closed_and_next_call_reconnects() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (closed_tx, closed_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            let hello: Hello = decode_exact(&read_frame_async(&mut first).await.unwrap()).unwrap();
            assert_eq!(hello.version, POSTCARD_RPC_WIRE_VERSION);
            super::super::write_value_async(
                &mut first,
                &HelloReply::Accepted {
                    version: POSTCARD_RPC_WIRE_VERSION,
                    recorder_id: "node-1".into(),
                },
            )
            .await
            .unwrap();
            read_frame_async(&mut first).await.unwrap();
            assert_eq!(
                read_frame_async(&mut first).await.unwrap_err(),
                "connection closed"
            );
            let _ = closed_tx.send(());
            let (second, _) = listener.accept().await.unwrap();
            serve_postcard_rpc_connection(
                second,
                IdentityRecorder,
                peers().into(),
                7,
                Arc::new(tokio::sync::Semaphore::new(DEFAULT_PEER_CONCURRENCY)),
            )
            .await
        });
        let client = Arc::new(
            TcpPostcardRpcRecorderClient::new_with_transport_and_timeout(
                address,
                "node-1",
                "node-2",
                "peer-token-2",
                7,
                ClientTransport::Plain,
                Duration::from_millis(250),
            )
            .unwrap(),
        );
        let blackholed = Arc::clone(&client);
        assert!(
            tokio::task::spawn_blocking(move || blackholed.recorder_id())
                .await
                .unwrap()
                .is_err()
        );
        tokio::time::timeout(Duration::from_secs(1), closed_rx)
            .await
            .expect("timed-out session socket must close")
            .unwrap();
        assert_eq!(
            tokio::task::spawn_blocking(move || client.recorder_id())
                .await
                .unwrap()
                .unwrap(),
            "node-1"
        );
        server.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn bridge_accepts_128_queued_calls_then_promptly_overloads() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
        let blackhole = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            let _ = accepted_tx.send(());
            std::future::pending::<()>().await;
        });
        let client = Arc::new(
            TcpPostcardRpcRecorderClient::new_with_transport_and_timeout(
                address,
                "node-1",
                "node-2",
                "peer-token-2",
                7,
                ClientTransport::Plain,
                Duration::from_millis(500),
            )
            .unwrap(),
        );
        let connecting = Arc::clone(&client);
        let first = tokio::task::spawn_blocking(move || connecting.recorder_id());
        tokio::time::timeout(Duration::from_secs(1), accepted_rx)
            .await
            .unwrap()
            .unwrap();

        let deadline = Instant::now() + Duration::from_millis(500);
        let mut receivers = Vec::new();
        for slot in 0..128 {
            let (reply, receive) = mpsc::sync_channel(1);
            assert!(client
                .control
                .sender
                .try_send(BridgeRequest {
                    body: RecorderRequestBody::InspectRecordSummary { slot },
                    operation: Operation::InspectRecordSummary,
                    deadline,
                    reply,
                })
                .is_ok());
            receivers.push(receive);
        }
        let (reply, _receive) = mpsc::sync_channel(1);
        let started = Instant::now();
        assert!(matches!(
            client.control.sender.try_send(BridgeRequest {
                body: RecorderRequestBody::InspectRecordSummary { slot: 129 },
                operation: Operation::InspectRecordSummary,
                deadline,
                reply,
            }),
            Err(tokio::sync::mpsc::error::TrySendError::Full(_))
        ));
        assert!(started.elapsed() < Duration::from_millis(50));

        blackhole.abort();
        drop(receivers);
        let _ = first.await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn server_framing_survives_response_completion_during_next_read() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            serve_postcard_rpc_connection(
                stream,
                IdentityRecorder,
                peers().into(),
                7,
                Arc::new(tokio::sync::Semaphore::new(DEFAULT_PEER_CONCURRENCY)),
            )
            .await
        });
        let client = Arc::new(
            TcpPostcardRpcRecorderClient::new(address, "node-1", "node-2", "peer-token-2", 7)
                .unwrap(),
        );
        let start = Arc::new(std::sync::Barrier::new(5));
        let calls = (0..4)
            .map(|worker| {
                let client = Arc::clone(&client);
                let start = Arc::clone(&start);
                thread::spawn(move || {
                    start.wait();
                    (worker..10_000)
                        .step_by(4)
                        .find_map(|slot| client.inspect_record_summary(slot).err())
                })
            })
            .collect::<Vec<_>>();
        start.wait();
        let errors = calls
            .into_iter()
            .filter_map(|call| call.join().unwrap())
            .collect::<Vec<_>>();
        drop(client);
        let server_result = tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .unwrap()
            .unwrap();

        assert!(
            errors.is_empty() && server_result.is_ok(),
            "client errors: {errors:?}; server result: {server_result:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn queued_mutation_expiring_before_send_never_reaches_recorder() {
        let (started_tx, started_rx) = mpsc::channel();
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let mutations = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(serve_recorder_postcard_rpc(
            listener,
            BlockingInspections {
                started: started_tx,
                release: Arc::clone(&release),
                seen: Arc::clone(&seen),
                mutations: Arc::clone(&mutations),
            },
            peers(),
            7,
            std::future::pending(),
        ));
        let client = Arc::new(
            TcpPostcardRpcRecorderClient::new_with_transport_and_timeout(
                address,
                "node-1",
                "node-2",
                "peer-token-2",
                7,
                ClientTransport::Plain,
                Duration::from_secs(1),
            )
            .unwrap(),
        );
        let blockers = (1..=LANE_IN_FLIGHT)
            .map(|slot| {
                let client = Arc::clone(&client);
                tokio::task::spawn_blocking(move || {
                    client.inspect_record_summary(u64::try_from(slot).unwrap())
                })
            })
            .collect::<Vec<_>>();
        for _ in 0..LANE_IN_FLIGHT {
            started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        }

        let (reply, receive) = mpsc::sync_channel(1);
        let command = StoredCommand::new(rhiza_core::EntryType::Command, b"queued".to_vec());
        client
            .control
            .sender
            .try_send(BridgeRequest {
                body: RecorderRequestBody::StoreCommand {
                    cluster_id: "rhiza:sql:cluster-a".into(),
                    epoch: 1,
                    config_id: 1,
                    config_digest: LogHash::ZERO,
                    command_hash: command.hash(),
                    command,
                },
                operation: Operation::StoreCommand,
                deadline: Instant::now() + Duration::from_millis(50),
                reply,
            })
            .unwrap_or_else(|_| panic!("short-lived request should enter the bounded queue"));
        assert!(matches!(
            receive.recv_timeout(Duration::from_millis(75)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        drop(receive);
        let (released, ready) = &*release;
        *released.lock().unwrap() = true;
        ready.notify_all();
        for blocker in blockers {
            assert!(blocker.await.unwrap().is_ok());
        }
        assert_eq!(client.recorder_id().unwrap(), "node-1");
        assert_eq!(mutations.load(Ordering::SeqCst), 0);
        let mut seen = seen.lock().unwrap().clone();
        seen.sort_unstable();
        assert_eq!(
            seen,
            (1..=u64::try_from(LANE_IN_FLIGHT).unwrap()).collect::<Vec<_>>()
        );
        server.abort();
    }
}
