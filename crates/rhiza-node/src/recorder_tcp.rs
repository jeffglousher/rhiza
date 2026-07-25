use std::{
    fmt,
    future::Future,
    io::{Read, Write},
    net::{SocketAddr, TcpStream, ToSocketAddrs},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Condvar, Mutex,
    },
    time::{Duration, Instant},
};

use rhiza_core::{LogHash, StoredCommand};
use rhiza_quepaxa::{
    DecisionProof, Error, Membership, ReadFenceObservation, ReadFenceRequest, RecordRequest,
    RecordSummary, RecorderRpc, RejectReason,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_rustls::TlsAcceptor;

use crate::{
    authenticated_proposer_admitted, map_quorum_record_transport_error,
    peer_credentials_authenticated, valid_recorder_command, valid_recorder_record, PeerConfig,
    DEFAULT_PEER_CONCURRENCY, MAX_HTTP_BODY_BYTES, QUORUM_RECORD_REQUEST_TIMEOUT,
    READ_FENCE_REQUEST_TIMEOUT,
};

const WIRE_VERSION: u16 = 3;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const CALL_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECTIONS_PER_LANE: usize = 2;
const MAX_SERVER_CONNECTIONS: usize = DEFAULT_PEER_CONCURRENCY * 4;
const RECORDER_TLS_ALPN: &[u8] = b"rhiza-recorder/3";

#[cfg(feature = "recorder-postcard-rpc")]
mod postcard_rpc;
#[cfg(feature = "recorder-postcard-rpc")]
pub use postcard_rpc::{
    serve_recorder_postcard_rpc, serve_recorder_postcard_rpc_tls,
    RecorderPostcardRpcTlsClientConfig, RecorderPostcardRpcTlsServerConfig,
    TcpPostcardRpcRecorderClient,
};

#[derive(Clone)]
pub struct RecorderTlsServerConfig {
    inner: Arc<rustls::ServerConfig>,
}

impl fmt::Debug for RecorderTlsServerConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecorderTlsServerConfig")
            .finish_non_exhaustive()
    }
}

impl RecorderTlsServerConfig {
    pub fn from_pem(certificate_chain_pem: &[u8], private_key_pem: &[u8]) -> Result<Self, String> {
        let certificates = rustls_pemfile::certs(&mut std::io::Cursor::new(certificate_chain_pem))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| "invalid recorder TLS certificate PEM".to_string())?;
        if certificates.is_empty() {
            return Err("recorder TLS certificate chain is empty".into());
        }
        let mut key_reader = std::io::Cursor::new(private_key_pem);
        let private_key = rustls_pemfile::private_key(&mut key_reader)
            .map_err(|_| "invalid recorder TLS private key PEM".to_string())?
            .ok_or_else(|| "recorder TLS private key is missing".to_string())?;
        if rustls_pemfile::private_key(&mut key_reader)
            .map_err(|_| "invalid recorder TLS private key PEM".to_string())?
            .is_some()
        {
            return Err("recorder TLS private key PEM contains multiple keys".into());
        }
        let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|_| "recorder TLS crypto provider does not support TLS 1.3".to_string())?
        .with_no_client_auth()
        .with_single_cert(certificates, private_key)
        .map_err(|_| {
            "recorder TLS certificate and private key are invalid or mismatched".to_string()
        })?;
        config.alpn_protocols = vec![RECORDER_TLS_ALPN.to_vec()];
        config.max_early_data_size = 0;
        Ok(Self {
            inner: Arc::new(config),
        })
    }
}

#[derive(Clone)]
pub struct RecorderTlsClientConfig {
    inner: Arc<rustls::ClientConfig>,
    server_name: rustls::pki_types::ServerName<'static>,
}

impl fmt::Debug for RecorderTlsClientConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RecorderTlsClientConfig")
            .field("server_name", &self.server_name)
            .finish_non_exhaustive()
    }
}

impl RecorderTlsClientConfig {
    pub fn from_ca_pem(ca_bundle_pem: &[u8], server_name: &str) -> Result<Self, String> {
        let certificates = rustls_pemfile::certs(&mut std::io::Cursor::new(ca_bundle_pem))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| "invalid recorder TLS CA bundle PEM".to_string())?;
        if certificates.is_empty() {
            return Err("recorder TLS CA bundle is empty".into());
        }
        let mut roots = rustls::RootCertStore::empty();
        for certificate in certificates {
            roots.add(certificate).map_err(|_| {
                "recorder TLS CA bundle contains an invalid certificate".to_string()
            })?;
        }
        let server_name = rustls::pki_types::ServerName::try_from(server_name.to_owned())
            .map_err(|_| "invalid recorder TLS server name".to_string())?;
        let mut config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|_| "recorder TLS crypto provider does not support TLS 1.3".to_string())?
        .with_root_certificates(roots)
        .with_no_client_auth();
        config.alpn_protocols = vec![RECORDER_TLS_ALPN.to_vec()];
        config.enable_early_data = false;
        Ok(Self {
            inner: Arc::new(config),
            server_name,
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct Hello {
    version: u16,
    node_id: String,
    recovery_generation: u64,
    token: String,
}

#[derive(Debug, Deserialize, Serialize)]
enum HelloReply {
    Accepted { version: u16, recorder_id: String },
    Rejected,
}

#[derive(Debug, Deserialize, Serialize)]
struct RequestFrame {
    version: u16,
    request_id: u64,
    remaining_deadline_ms: u32,
    body: RecorderRequestBody,
}

#[derive(Debug, Deserialize, Serialize)]
enum RecorderRequestBody {
    Identity,
    StoreCommand {
        cluster_id: String,
        epoch: u64,
        config_id: u64,
        config_digest: LogHash,
        command_hash: LogHash,
        command: StoredCommand,
    },
    FetchCommand {
        cluster_id: String,
        epoch: u64,
        config_id: u64,
        config_digest: LogHash,
        command_hash: LogHash,
    },
    Record(RecordRequest),
    InstallDecisionProof {
        proof: DecisionProof,
        members: Vec<String>,
    },
    InspectDecisionProof {
        slot: u64,
    },
    InspectRecordSummary {
        slot: u64,
    },
    ObserveReadFence(ReadFenceRequest),
}

#[derive(Debug, Deserialize, Serialize)]
struct ResponseFrame {
    version: u16,
    request_id: u64,
    body: RecorderResponseBody,
}

#[derive(Debug, Deserialize, Serialize)]
enum RecorderResponseBody {
    Identity(RpcResult<String>),
    StoreCommand(RpcResult<()>),
    FetchCommand(RpcResult<Option<StoredCommand>>),
    Record(RpcResult<RecordSummary>),
    InstallDecisionProof(RpcResult<()>),
    InspectDecisionProof(RpcResult<Option<DecisionProof>>),
    InspectRecordSummary(RpcResult<Option<RecordSummary>>),
    ObserveReadFence(RpcResult<ReadFenceObservation>),
}

#[derive(Debug, Deserialize, Serialize)]
enum RpcResult<T> {
    Ok(T),
    Rejected(RejectReason),
    Error(String),
    Overloaded,
}

impl<T> RpcResult<T> {
    fn from_result(result: rhiza_quepaxa::Result<T>) -> Self {
        match result {
            Ok(value) => Self::Ok(value),
            Err(Error::Rejected(reason)) => Self::Rejected(reason),
            Err(error) => Self::Error(error.to_string()),
        }
    }

    fn into_result(self) -> rhiza_quepaxa::Result<T> {
        match self {
            Self::Ok(value) => Ok(value),
            Self::Rejected(reason) => Err(Error::Rejected(reason)),
            Self::Error(message) => Err(Error::Io(message)),
            Self::Overloaded => Err(Error::Io("recorder RPC overloaded".into())),
        }
    }
}

pub async fn serve_recorder_tcp<R, F>(
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
    serve_recorder_tcp_inner(
        listener,
        recorder,
        peers,
        recovery_generation,
        None,
        shutdown,
    )
    .await
}

pub async fn serve_recorder_tcp_tls<R, F>(
    listener: tokio::net::TcpListener,
    recorder: R,
    peers: Vec<PeerConfig>,
    recovery_generation: u64,
    tls: RecorderTlsServerConfig,
    shutdown: F,
) -> Result<(), String>
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
    F: Future<Output = ()> + Send,
{
    serve_recorder_tcp_inner(
        listener,
        recorder,
        peers,
        recovery_generation,
        Some(tls.inner),
        shutdown,
    )
    .await
}

async fn serve_recorder_tcp_inner<R, F>(
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
    let reported_connection_error = Arc::new(AtomicBool::new(false));
    let mut tasks = tokio::task::JoinSet::new();
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            () = &mut shutdown => break,
            Some(_) = tasks.join_next(), if !tasks.is_empty() => {}
            accepted = listener.accept() => {
                let (stream, _) = accepted.map_err(|error| format!("recorder TCP accept failed: {error}"))?;
                let Ok(connection) = connections.clone().try_acquire_owned() else {
                    continue;
                };
                let _ = stream.set_nodelay(true);
                let recorder = recorder.clone();
                let peers = peers.clone();
                let slots = slots.clone();
                let tls = tls.clone();
                let reported_connection_error = Arc::clone(&reported_connection_error);
                tasks.spawn(async move {
                    let _connection = connection;
                    let result = if let Some(config) = tls {
                        let acceptor = TlsAcceptor::from(config);
                        match tokio::time::timeout(CONNECT_TIMEOUT, acceptor.accept(stream)).await {
                            Ok(Ok(tls_stream)) => {
                                if tls_stream.get_ref().1.alpn_protocol() != Some(RECORDER_TLS_ALPN) {
                                    Err("recorder TLS ALPN negotiation failed".to_string())
                                } else {
                                    serve_connection(tls_stream, recorder, peers, recovery_generation, slots).await
                                }
                            }
                            Ok(Err(_)) => Err("recorder TLS handshake failed".to_string()),
                            Err(_) => Err("recorder TLS handshake timed out".to_string()),
                        }
                    } else {
                        serve_connection(stream, recorder, peers, recovery_generation, slots).await
                    };
                    if let Err(error) = result {
                        if error != "connection closed"
                            && !reported_connection_error.swap(true, Ordering::Relaxed)
                        {
                            eprintln!("recorder TCP connection rejected: {error}");
                        }
                    }
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

async fn serve_connection<R, S>(
    mut stream: S,
    recorder: R,
    peers: Arc<[PeerConfig]>,
    recovery_generation: u64,
    slots: Arc<tokio::sync::Semaphore>,
) -> Result<(), String>
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let hello_bytes = tokio::time::timeout(CALL_TIMEOUT, read_frame_async(&mut stream))
        .await
        .map_err(|_| "recorder HELLO timed out".to_string())??;
    let hello: Hello = decode_exact(&hello_bytes)?;
    if !hello_authenticated(&hello, &peers, recovery_generation) {
        let _ = write_value_async_with_timeout(
            &mut stream,
            &HelloReply::Rejected,
            "recorder HELLO rejection",
        )
        .await;
        return Err("recorder HELLO rejected".into());
    }
    let identity_recorder = recorder.clone();
    let recorder_id = tokio::task::spawn_blocking(move || identity_recorder.recorder_id())
        .await
        .map_err(|error| format!("recorder identity task failed: {error}"))?
        .map_err(|error| error.to_string())?;
    write_value_async_with_timeout(
        &mut stream,
        &HelloReply::Accepted {
            version: WIRE_VERSION,
            recorder_id,
        },
        "recorder HELLO response",
    )
    .await?;

    loop {
        let request = match read_frame_async(&mut stream).await {
            Ok(bytes) => decode_exact::<RequestFrame>(&bytes)?,
            Err(error) if error == "connection closed" => return Ok(()),
            Err(error) => return Err(error),
        };
        if request.version != WIRE_VERSION || request.remaining_deadline_ms == 0 {
            return Err("invalid recorder request envelope".into());
        }
        let request_id = request.request_id;
        let operation = response_operation(&request.body);
        let dispatch_deadline = Instant::now()
            + Duration::from_millis(u64::from(request.remaining_deadline_ms)).min(CALL_TIMEOUT);
        let permit = match slots.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                write_value_async_with_timeout(
                    &mut stream,
                    &ResponseFrame {
                        version: WIRE_VERSION,
                        request_id,
                        body: overloaded_response(operation),
                    },
                    "recorder overload response",
                )
                .await?;
                continue;
            }
        };
        let body = dispatch_with_deadline(
            recorder.clone(),
            request.body,
            operation,
            permit,
            dispatch_deadline,
            hello.node_id.clone(),
            Arc::clone(&peers),
        )
        .await;
        write_value_async_with_timeout(
            &mut stream,
            &ResponseFrame {
                version: WIRE_VERSION,
                request_id,
                body,
            },
            "recorder response",
        )
        .await?;
    }
}

async fn dispatch_with_deadline<R>(
    recorder: R,
    body: RecorderRequestBody,
    operation: Operation,
    permit: tokio::sync::OwnedSemaphorePermit,
    deadline: Instant,
    authenticated_peer_id: String,
    peers: Arc<[PeerConfig]>,
) -> RecorderResponseBody
where
    R: RecorderRpc + Send + Sync + 'static,
{
    if deadline <= Instant::now() {
        return error_response(operation, "recorder RPC deadline exceeded".into());
    }
    let dispatched = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        dispatch(recorder, body, &authenticated_peer_id, &peers)
    });
    match tokio::time::timeout_at(deadline.into(), dispatched).await {
        Ok(Ok(response)) => response,
        Ok(Err(error)) => error_response(operation, error.to_string()),
        Err(_) => error_response(operation, "recorder RPC deadline exceeded".into()),
    }
}

fn hello_authenticated(hello: &Hello, peers: &[PeerConfig], recovery_generation: u64) -> bool {
    hello.version == WIRE_VERSION
        && hello.recovery_generation == recovery_generation
        && peer_credentials_authenticated(&hello.node_id, &hello.token, peers)
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Operation {
    Identity,
    StoreCommand,
    FetchCommand,
    Record,
    InstallDecisionProof,
    InspectDecisionProof,
    InspectRecordSummary,
    ObserveReadFence,
}

fn response_operation(request: &RecorderRequestBody) -> Operation {
    match request {
        RecorderRequestBody::Identity => Operation::Identity,
        RecorderRequestBody::StoreCommand { .. } => Operation::StoreCommand,
        RecorderRequestBody::FetchCommand { .. } => Operation::FetchCommand,
        RecorderRequestBody::Record(_) => Operation::Record,
        RecorderRequestBody::InstallDecisionProof { .. } => Operation::InstallDecisionProof,
        RecorderRequestBody::InspectDecisionProof { .. } => Operation::InspectDecisionProof,
        RecorderRequestBody::InspectRecordSummary { .. } => Operation::InspectRecordSummary,
        RecorderRequestBody::ObserveReadFence(_) => Operation::ObserveReadFence,
    }
}

fn dispatch<R: RecorderRpc>(
    recorder: R,
    request: RecorderRequestBody,
    authenticated_peer_id: &str,
    peers: &[PeerConfig],
) -> RecorderResponseBody {
    match request {
        RecorderRequestBody::Identity => {
            RecorderResponseBody::Identity(RpcResult::from_result(recorder.recorder_id()))
        }
        RecorderRequestBody::StoreCommand {
            cluster_id,
            epoch,
            config_id,
            config_digest,
            command_hash,
            command,
        } => {
            let result = if !valid_recorder_command(&command) {
                Err(Error::Rejected(RejectReason::InvalidRequest))
            } else {
                recorder.store_command_for(
                    cluster_id,
                    epoch,
                    config_id,
                    config_digest,
                    command_hash,
                    command,
                )
            };
            RecorderResponseBody::StoreCommand(RpcResult::from_result(result))
        }
        RecorderRequestBody::FetchCommand {
            cluster_id,
            epoch,
            config_id,
            config_digest,
            command_hash,
        } => RecorderResponseBody::FetchCommand(RpcResult::from_result(
            recorder.fetch_command_for(cluster_id, epoch, config_id, config_digest, command_hash),
        )),
        RecorderRequestBody::Record(request) => {
            let result = if !valid_recorder_record(&request)
                || !authenticated_proposer_admitted(
                    authenticated_peer_id,
                    &request.proposal.proposer_id,
                    peers,
                ) {
                Err(Error::Rejected(RejectReason::InvalidRequest))
            } else {
                recorder.record(request)
            };
            RecorderResponseBody::Record(RpcResult::from_result(result))
        }
        RecorderRequestBody::InstallDecisionProof { proof, members } => {
            let result = if !authenticated_proposer_admitted(
                authenticated_peer_id,
                &proof.proposal().proposer_id,
                peers,
            ) {
                Err(Error::Rejected(RejectReason::InvalidRequest))
            } else {
                Membership::from_voters(members)
                    .and_then(|membership| recorder.install_decision_proof(proof, &membership))
            };
            RecorderResponseBody::InstallDecisionProof(RpcResult::from_result(result))
        }
        RecorderRequestBody::InspectDecisionProof { slot } => {
            RecorderResponseBody::InspectDecisionProof(RpcResult::from_result(
                recorder.inspect_decision_proof(slot),
            ))
        }
        RecorderRequestBody::InspectRecordSummary { slot } => {
            RecorderResponseBody::InspectRecordSummary(RpcResult::from_result(
                recorder.inspect_record_summary(slot),
            ))
        }
        RecorderRequestBody::ObserveReadFence(request) => RecorderResponseBody::ObserveReadFence(
            RpcResult::from_result(recorder.observe_read_fence(request)),
        ),
    }
}

fn overloaded_response(operation: Operation) -> RecorderResponseBody {
    match operation {
        Operation::Identity => RecorderResponseBody::Identity(RpcResult::Overloaded),
        Operation::StoreCommand => RecorderResponseBody::StoreCommand(RpcResult::Overloaded),
        Operation::FetchCommand => RecorderResponseBody::FetchCommand(RpcResult::Overloaded),
        Operation::Record => RecorderResponseBody::Record(RpcResult::Overloaded),
        Operation::InstallDecisionProof => {
            RecorderResponseBody::InstallDecisionProof(RpcResult::Overloaded)
        }
        Operation::InspectDecisionProof => {
            RecorderResponseBody::InspectDecisionProof(RpcResult::Overloaded)
        }
        Operation::InspectRecordSummary => {
            RecorderResponseBody::InspectRecordSummary(RpcResult::Overloaded)
        }
        Operation::ObserveReadFence => {
            RecorderResponseBody::ObserveReadFence(RpcResult::Overloaded)
        }
    }
}

fn error_response(operation: Operation, message: String) -> RecorderResponseBody {
    match operation {
        Operation::Identity => RecorderResponseBody::Identity(RpcResult::Error(message)),
        Operation::StoreCommand => RecorderResponseBody::StoreCommand(RpcResult::Error(message)),
        Operation::FetchCommand => RecorderResponseBody::FetchCommand(RpcResult::Error(message)),
        Operation::Record => RecorderResponseBody::Record(RpcResult::Error(message)),
        Operation::InstallDecisionProof => {
            RecorderResponseBody::InstallDecisionProof(RpcResult::Error(message))
        }
        Operation::InspectDecisionProof => {
            RecorderResponseBody::InspectDecisionProof(RpcResult::Error(message))
        }
        Operation::InspectRecordSummary => {
            RecorderResponseBody::InspectRecordSummary(RpcResult::Error(message))
        }
        Operation::ObserveReadFence => {
            RecorderResponseBody::ObserveReadFence(RpcResult::Error(message))
        }
    }
}

async fn read_frame_async<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<Vec<u8>, String> {
    let mut length = [0_u8; 4];
    match reader.read_exact(&mut length).await {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
            return Err("connection closed".into())
        }
        Err(error) => return Err(error.to_string()),
    }
    let length = usize::try_from(u32::from_be_bytes(length)).unwrap_or(usize::MAX);
    if length == 0 || length > MAX_HTTP_BODY_BYTES {
        return Err("invalid recorder frame length".into());
    }
    let mut frame = vec![0; length];
    reader
        .read_exact(&mut frame)
        .await
        .map_err(|error| error.to_string())?;
    Ok(frame)
}

async fn write_value_async<W: tokio::io::AsyncWrite + Unpin, T: Serialize>(
    writer: &mut W,
    value: &T,
) -> Result<(), String> {
    let encoded = postcard::to_allocvec(value).map_err(|error| error.to_string())?;
    write_frame_async(writer, &encoded).await
}

async fn write_value_async_with_timeout<W: tokio::io::AsyncWrite + Unpin, T: Serialize>(
    writer: &mut W,
    value: &T,
    operation: &str,
) -> Result<(), String> {
    tokio::time::timeout(CALL_TIMEOUT, write_value_async(writer, value))
        .await
        .map_err(|_| format!("{operation} timed out"))?
}

async fn write_frame_async<W: tokio::io::AsyncWrite + Unpin>(
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
        .map_err(|error| error.to_string())
}

fn decode_exact<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    let (value, trailing) = postcard::take_from_bytes(bytes).map_err(|error| error.to_string())?;
    if !trailing.is_empty() {
        return Err("trailing recorder frame bytes".into());
    }
    Ok(value)
}

fn frame_length(frame: &[u8]) -> Result<[u8; 4], String> {
    if frame.is_empty() || frame.len() > MAX_HTTP_BODY_BYTES {
        return Err("invalid recorder frame length".into());
    }
    let length = u32::try_from(frame.len()).map_err(|_| "recorder frame is too large")?;
    Ok(length.to_be_bytes())
}

struct ConnectionPool {
    state: Mutex<PoolState>,
    available: Condvar,
}

#[derive(Default)]
struct PoolState {
    idle: Vec<RecorderClientStream>,
    open: usize,
}

trait DeadlineClock {
    fn now(&self) -> Instant;
}

#[derive(Clone, Copy)]
struct SystemClock;

impl DeadlineClock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

trait SocketTimeouts {
    fn set_read_timeout(&self, timeout: Option<Duration>) -> std::io::Result<()>;
    fn set_write_timeout(&self, timeout: Option<Duration>) -> std::io::Result<()>;
}

impl SocketTimeouts for TcpStream {
    fn set_read_timeout(&self, timeout: Option<Duration>) -> std::io::Result<()> {
        TcpStream::set_read_timeout(self, timeout)
    }

    fn set_write_timeout(&self, timeout: Option<Duration>) -> std::io::Result<()> {
        TcpStream::set_write_timeout(self, timeout)
    }
}

struct DeadlineStream<S, C = SystemClock> {
    inner: S,
    deadline: Instant,
    clock: C,
}

impl<S> DeadlineStream<S> {
    fn new(inner: S, deadline: Instant) -> Self {
        Self::new_with_clock(inner, deadline, SystemClock)
    }
}

impl<S, C> DeadlineStream<S, C> {
    fn new_with_clock(inner: S, deadline: Instant, clock: C) -> Self {
        Self {
            inner,
            deadline,
            clock,
        }
    }

    fn set_deadline(&mut self, deadline: Instant) {
        self.deadline = deadline;
    }
}

impl<S, C: DeadlineClock> DeadlineStream<S, C> {
    fn remaining(&self) -> std::io::Result<Duration> {
        let remaining = self.deadline.saturating_duration_since(self.clock.now());
        if remaining.is_zero() {
            Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "recorder RPC deadline exceeded",
            ))
        } else {
            Ok(remaining)
        }
    }
}

impl<S: Read + SocketTimeouts, C: DeadlineClock> Read for DeadlineStream<S, C> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.inner.set_read_timeout(Some(self.remaining()?))?;
        self.inner.read(buffer)
    }
}

impl<S: Write + SocketTimeouts, C: DeadlineClock> Write for DeadlineStream<S, C> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.inner.set_write_timeout(Some(self.remaining()?))?;
        self.inner.write(buffer)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.set_write_timeout(Some(self.remaining()?))?;
        self.inner.flush()
    }
}

enum RecorderClientStream {
    Plain(DeadlineStream<TcpStream>),
    Tls(Box<rustls::StreamOwned<rustls::ClientConnection, DeadlineStream<TcpStream>>>),
}

impl RecorderClientStream {
    fn set_deadline(&mut self, deadline: Instant) {
        match self {
            Self::Plain(stream) => stream.set_deadline(deadline),
            Self::Tls(stream) => stream.sock.set_deadline(deadline),
        }
    }

    fn ensure_deadline(&self) -> std::io::Result<()> {
        match self {
            Self::Plain(stream) => stream.remaining().map(|_| ()),
            Self::Tls(stream) => stream.sock.remaining().map(|_| ()),
        }
    }
}

impl Read for RecorderClientStream {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        self.ensure_deadline()?;
        match self {
            Self::Plain(stream) => stream.read(buffer),
            Self::Tls(stream) => stream.read(buffer),
        }
    }
}

impl Write for RecorderClientStream {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.ensure_deadline()?;
        match self {
            Self::Plain(stream) => stream.write(buffer),
            Self::Tls(stream) => stream.write(buffer),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.ensure_deadline()?;
        match self {
            Self::Plain(stream) => stream.flush(),
            Self::Tls(stream) => stream.flush(),
        }
    }
}

#[derive(Clone)]
enum ClientTransport {
    Plain,
    Tls(RecorderTlsClientConfig),
}

impl ConnectionPool {
    fn new() -> Self {
        Self {
            state: Mutex::new(PoolState::default()),
            available: Condvar::new(),
        }
    }
}

pub struct TcpPostcardRecorderClient {
    address: String,
    expected_recorder_id: String,
    local_node_id: String,
    peer_token: String,
    recovery_generation: u64,
    transport: ClientTransport,
    call_timeout: Duration,
    consensus: ConnectionPool,
    control: ConnectionPool,
    next_request_id: AtomicU64,
}

impl fmt::Debug for TcpPostcardRecorderClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TcpPostcardRecorderClient")
            .field("address", &self.address)
            .field("expected_recorder_id", &self.expected_recorder_id)
            .field("local_node_id", &self.local_node_id)
            .field("peer_token", &"[redacted]")
            .field("recovery_generation", &self.recovery_generation)
            .field("call_timeout", &self.call_timeout)
            .field(
                "transport",
                &match self.transport {
                    ClientTransport::Plain => "plain",
                    ClientTransport::Tls(_) => "tls",
                },
            )
            .finish()
    }
}

impl TcpPostcardRecorderClient {
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
        tls: RecorderTlsClientConfig,
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
            || call_timeout.is_zero()
        {
            return Err("invalid recorder TCP client identity".into());
        }
        Ok(Self {
            address,
            expected_recorder_id,
            local_node_id,
            peer_token,
            recovery_generation,
            transport,
            call_timeout,
            consensus: ConnectionPool::new(),
            control: ConnectionPool::new(),
            next_request_id: AtomicU64::new(1),
        })
    }

    fn exchange(
        &self,
        request: RecorderRequestBody,
        consensus: bool,
    ) -> rhiza_quepaxa::Result<RecorderResponseBody> {
        self.exchange_with_timeout(request, consensus, self.call_timeout)
    }

    fn exchange_with_timeout(
        &self,
        request: RecorderRequestBody,
        consensus: bool,
        timeout: Duration,
    ) -> rhiza_quepaxa::Result<RecorderResponseBody> {
        let deadline = Instant::now() + timeout.min(self.call_timeout);
        let pool = if consensus {
            &self.consensus
        } else {
            &self.control
        };
        let mut stream = self.checkout(pool, deadline)?;
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let operation = response_operation(&request);
        stream.set_deadline(deadline);
        let remaining_deadline_ms = match advertised_remaining_deadline_ms(deadline) {
            Ok(remaining) => remaining,
            Err(error) => {
                self.discard(pool);
                return Err(error);
            }
        };
        let frame = RequestFrame {
            version: WIRE_VERSION,
            request_id,
            remaining_deadline_ms,
            body: request,
        };
        let result = write_value_sync(&mut stream, &frame)
            .and_then(|()| read_frame_sync(&mut stream))
            .and_then(|bytes| decode_exact::<ResponseFrame>(&bytes));
        match result {
            Ok(response)
                if response.version == WIRE_VERSION
                    && response.request_id == request_id
                    && response_matches(operation, &response.body) =>
            {
                self.checkin(pool, stream);
                Ok(response.body)
            }
            Ok(_) => {
                self.discard(pool);
                Err(Error::Decode("recorder response envelope mismatch".into()))
            }
            Err(error) => {
                self.discard(pool);
                Err(Error::Io(error))
            }
        }
    }

    fn checkout(
        &self,
        pool: &ConnectionPool,
        deadline: Instant,
    ) -> rhiza_quepaxa::Result<RecorderClientStream> {
        loop {
            let mut state = pool
                .state
                .lock()
                .map_err(|_| Error::Io("recorder connection pool lock poisoned".into()))?;
            if let Some(stream) = state.idle.pop() {
                return Ok(stream);
            }
            if state.open < CONNECTIONS_PER_LANE {
                state.open += 1;
                drop(state);
                return match self.connect(deadline) {
                    Ok(stream) => Ok(stream),
                    Err(error) => {
                        self.discard(pool);
                        Err(Error::Io(error))
                    }
                };
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Error::Io("recorder connection checkout timed out".into()));
            }
            let (next, wait) = pool
                .available
                .wait_timeout(state, remaining)
                .map_err(|_| Error::Io("recorder connection pool lock poisoned".into()))?;
            drop(next);
            if wait.timed_out() {
                return Err(Error::Io("recorder connection checkout timed out".into()));
            }
        }
    }

    fn connect(&self, deadline: Instant) -> Result<RecorderClientStream, String> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let connect_timeout = CONNECT_TIMEOUT.min(remaining);
        if connect_timeout.is_zero() {
            return Err("recorder connect deadline exceeded".into());
        }
        let mut last_error = None;
        let mut socket = None;
        let resolved_addresses = self
            .address
            .to_socket_addrs()
            .map_err(|error| format!("cannot resolve recorder TCP address: {error}"))?
            .collect::<Vec<SocketAddr>>();
        if resolved_addresses.is_empty() {
            return Err("recorder TCP address resolved to no endpoints".into());
        }
        for address in &resolved_addresses {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            match TcpStream::connect_timeout(address, connect_timeout.min(remaining)) {
                Ok(connected) => {
                    socket = Some(connected);
                    break;
                }
                Err(error) => last_error = Some(error),
            }
        }
        let socket = socket.ok_or_else(|| {
            format!(
                "recorder TCP connect failed: {}",
                last_error
                    .map(|error| error.to_string())
                    .unwrap_or_else(|| "deadline exceeded".into())
            )
        })?;
        socket
            .set_nodelay(true)
            .map_err(|error| format!("cannot set recorder TCP_NODELAY: {error}"))?;
        let socket = DeadlineStream::new(socket, deadline);
        let mut stream = match &self.transport {
            ClientTransport::Plain => RecorderClientStream::Plain(socket),
            ClientTransport::Tls(tls) => {
                let connection =
                    rustls::ClientConnection::new(Arc::clone(&tls.inner), tls.server_name.clone())
                        .map_err(|_| "cannot initialize recorder TLS connection".to_string())?;
                let mut stream = rustls::StreamOwned::new(connection, socket);
                while stream.conn.is_handshaking() {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return Err("recorder TLS handshake timed out".into());
                    }
                    stream
                        .conn
                        .complete_io(&mut stream.sock)
                        .map_err(|_| "recorder TLS handshake failed".to_string())?;
                }
                if stream.conn.alpn_protocol() != Some(RECORDER_TLS_ALPN) {
                    return Err("recorder TLS ALPN negotiation failed".into());
                }
                RecorderClientStream::Tls(Box::new(stream))
            }
        };
        write_value_sync(
            &mut stream,
            &Hello {
                version: WIRE_VERSION,
                node_id: self.local_node_id.clone(),
                recovery_generation: self.recovery_generation,
                token: self.peer_token.clone(),
            },
        )?;
        let reply: HelloReply = decode_exact(&read_frame_sync(&mut stream)?)?;
        match reply {
            HelloReply::Accepted {
                version,
                recorder_id,
            } if version == WIRE_VERSION && recorder_id == self.expected_recorder_id => Ok(stream),
            HelloReply::Accepted { .. } => Err("recorder identity mismatch".into()),
            HelloReply::Rejected => Err("recorder HELLO rejected".into()),
        }
    }

    fn checkin(&self, pool: &ConnectionPool, stream: RecorderClientStream) {
        if let Ok(mut state) = pool.state.lock() {
            state.idle.push(stream);
            pool.available.notify_one();
        }
    }

    fn discard(&self, pool: &ConnectionPool) {
        if let Ok(mut state) = pool.state.lock() {
            state.open = state.open.saturating_sub(1);
            pool.available.notify_one();
        }
    }
}

pub fn validate_recorder_tcp_endpoint(address: &str) -> Result<(), String> {
    let parsed = reqwest::Url::parse(&format!("tcp://{address}"))
        .map_err(|_| "invalid recorder TCP address".to_string())?;
    if parsed.host_str().is_none()
        || parsed.port().is_none()
        || !matches!(parsed.path(), "" | "/")
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err("invalid recorder TCP address".into());
    }
    Ok(())
}

fn response_matches(operation: Operation, response: &RecorderResponseBody) -> bool {
    matches!(
        (operation, response),
        (Operation::Identity, RecorderResponseBody::Identity(_))
            | (
                Operation::StoreCommand,
                RecorderResponseBody::StoreCommand(_)
            )
            | (
                Operation::FetchCommand,
                RecorderResponseBody::FetchCommand(_)
            )
            | (Operation::Record, RecorderResponseBody::Record(_))
            | (
                Operation::InstallDecisionProof,
                RecorderResponseBody::InstallDecisionProof(_)
            )
            | (
                Operation::InspectDecisionProof,
                RecorderResponseBody::InspectDecisionProof(_)
            )
            | (
                Operation::InspectRecordSummary,
                RecorderResponseBody::InspectRecordSummary(_)
            )
            | (
                Operation::ObserveReadFence,
                RecorderResponseBody::ObserveReadFence(_)
            )
    )
}

impl RecorderRpc for TcpPostcardRecorderClient {
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
        let request = RecorderRequestBody::StoreCommand {
            cluster_id,
            epoch,
            config_id,
            config_digest,
            command_hash,
            command,
        };
        match self.exchange(request, false)? {
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
        let request = RecorderRequestBody::FetchCommand {
            cluster_id,
            epoch,
            config_id,
            config_digest,
            command_hash,
        };
        match self.exchange(request, false)? {
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
        let request = RecorderRequestBody::InstallDecisionProof {
            proof,
            members: membership.members().to_vec(),
        };
        match self.exchange(request, true)? {
            RecorderResponseBody::InstallDecisionProof(result) => result.into_result(),
            _ => Err(Error::Decode("recorder response operation mismatch".into())),
        }
    }

    fn inspect_decision_proof(&self, slot: u64) -> rhiza_quepaxa::Result<Option<DecisionProof>> {
        let request = RecorderRequestBody::InspectDecisionProof { slot };
        match self.exchange(request, false)? {
            RecorderResponseBody::InspectDecisionProof(result) => result.into_result(),
            _ => Err(Error::Decode("recorder response operation mismatch".into())),
        }
    }

    fn inspect_record_summary(&self, slot: u64) -> rhiza_quepaxa::Result<Option<RecordSummary>> {
        let request = RecorderRequestBody::InspectRecordSummary { slot };
        match self.exchange(request, false)? {
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

fn advertised_remaining_deadline_ms(deadline: Instant) -> rhiza_quepaxa::Result<u32> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(Error::Io("recorder RPC deadline exceeded".into()));
    }
    Ok(u32::try_from(remaining.as_millis())
        .unwrap_or(u32::MAX)
        .max(1))
}

fn read_frame_sync(reader: &mut impl Read) -> Result<Vec<u8>, String> {
    let mut length = [0_u8; 4];
    reader
        .read_exact(&mut length)
        .map_err(|error| error.to_string())?;
    let length = usize::try_from(u32::from_be_bytes(length)).unwrap_or(usize::MAX);
    if length == 0 || length > MAX_HTTP_BODY_BYTES {
        return Err("invalid recorder frame length".into());
    }
    let mut frame = vec![0; length];
    reader
        .read_exact(&mut frame)
        .map_err(|error| error.to_string())?;
    Ok(frame)
}

fn write_value_sync(writer: &mut impl Write, value: &impl Serialize) -> Result<(), String> {
    let encoded = postcard::to_allocvec(value).map_err(|error| error.to_string())?;
    let length = frame_length(&encoded)?;
    writer
        .write_all(&length)
        .map_err(|error| error.to_string())?;
    writer
        .write_all(&encoded)
        .map_err(|error| error.to_string())?;
    writer.flush().map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        cell::{Cell, RefCell},
        collections::VecDeque,
        net::TcpListener,
        rc::Rc,
        sync::{
            atomic::{AtomicUsize, Ordering},
            mpsc,
        },
        thread,
    };

    #[derive(Clone)]
    struct FakeClock {
        origin: Instant,
        elapsed: Rc<Cell<Duration>>,
    }

    impl DeadlineClock for FakeClock {
        fn now(&self) -> Instant {
            self.origin + self.elapsed.get()
        }
    }

    struct SlowPartialIo {
        clock: FakeClock,
        step: Duration,
        input: VecDeque<u8>,
        read_timeout: Cell<Option<Duration>>,
        write_timeout: Cell<Option<Duration>>,
        read_timeouts: Rc<RefCell<Vec<Duration>>>,
        write_timeouts: Rc<RefCell<Vec<Duration>>>,
    }

    type SlowPartialFixture = (
        SlowPartialIo,
        FakeClock,
        Rc<RefCell<Vec<Duration>>>,
        Rc<RefCell<Vec<Duration>>>,
    );

    impl SlowPartialIo {
        fn spend(&self, timeout: Option<Duration>) -> std::io::Result<()> {
            let timeout = timeout.expect("deadline stream must configure a timeout");
            if self.step > timeout {
                self.clock.elapsed.set(self.clock.elapsed.get() + timeout);
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "scripted operation reached its timeout",
                ));
            }
            self.clock.elapsed.set(self.clock.elapsed.get() + self.step);
            Ok(())
        }
    }

    impl SocketTimeouts for SlowPartialIo {
        fn set_read_timeout(&self, timeout: Option<Duration>) -> std::io::Result<()> {
            self.read_timeout.set(timeout);
            self.read_timeouts
                .borrow_mut()
                .push(timeout.expect("read timeout must be bounded"));
            Ok(())
        }

        fn set_write_timeout(&self, timeout: Option<Duration>) -> std::io::Result<()> {
            self.write_timeout.set(timeout);
            self.write_timeouts
                .borrow_mut()
                .push(timeout.expect("write timeout must be bounded"));
            Ok(())
        }
    }

    impl Read for SlowPartialIo {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            self.spend(self.read_timeout.get())?;
            let Some(byte) = self.input.pop_front() else {
                return Ok(0);
            };
            buffer[0] = byte;
            Ok(1)
        }
    }

    impl Write for SlowPartialIo {
        fn write(&mut self, _buffer: &[u8]) -> std::io::Result<usize> {
            self.spend(self.write_timeout.get())?;
            Ok(1)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            self.spend(self.write_timeout.get())
        }
    }

    fn slow_partial_io(input: Vec<u8>) -> SlowPartialFixture {
        let clock = FakeClock {
            origin: Instant::now(),
            elapsed: Rc::new(Cell::new(Duration::ZERO)),
        };
        let read_timeouts = Rc::new(RefCell::new(Vec::new()));
        let write_timeouts = Rc::new(RefCell::new(Vec::new()));
        (
            SlowPartialIo {
                clock: clock.clone(),
                step: Duration::from_millis(30),
                input: input.into(),
                read_timeout: Cell::new(None),
                write_timeout: Cell::new(None),
                read_timeouts: Rc::clone(&read_timeouts),
                write_timeouts: Rc::clone(&write_timeouts),
            },
            clock,
            read_timeouts,
            write_timeouts,
        )
    }

    #[test]
    fn sync_frame_read_refreshes_timeout_against_one_absolute_deadline() {
        let mut input = 1_u32.to_be_bytes().to_vec();
        input.push(42);
        let (io, clock, read_timeouts, _) = slow_partial_io(input);
        let deadline = clock.now() + Duration::from_millis(100);
        let mut stream = DeadlineStream::new_with_clock(io, deadline, clock.clone());

        assert!(read_frame_sync(&mut stream).is_err());

        assert_eq!(clock.elapsed.get(), Duration::from_millis(100));
        assert_eq!(
            *read_timeouts.borrow(),
            [100, 70, 40, 10].map(Duration::from_millis)
        );
    }

    #[test]
    fn sync_frame_write_refreshes_timeout_against_one_absolute_deadline() {
        let (io, clock, _, write_timeouts) = slow_partial_io(Vec::new());
        let deadline = clock.now() + Duration::from_millis(100);
        let mut stream = DeadlineStream::new_with_clock(io, deadline, clock.clone());

        assert!(write_value_sync(&mut stream, &42_u64).is_err());

        assert_eq!(clock.elapsed.get(), Duration::from_millis(100));
        assert_eq!(
            *write_timeouts.borrow(),
            [100, 70, 40, 10].map(Duration::from_millis)
        );
    }

    #[test]
    fn framed_client_bounds_partial_response_drip_by_sender_deadline() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (advertised_tx, advertised_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let hello: Hello = decode_exact(&read_frame_sync(&mut stream).unwrap()).unwrap();
            assert_eq!(hello.version, WIRE_VERSION);
            thread::sleep(Duration::from_millis(80));
            write_value_sync(
                &mut stream,
                &HelloReply::Accepted {
                    version: WIRE_VERSION,
                    recorder_id: "node-1".into(),
                },
            )
            .unwrap();
            let request: RequestFrame =
                decode_exact(&read_frame_sync(&mut stream).unwrap()).unwrap();
            advertised_tx.send(request.remaining_deadline_ms).unwrap();
            for byte in [0_u8, 0, 0, 1, 0] {
                thread::sleep(Duration::from_millis(120));
                if stream.write_all(&[byte]).is_err() {
                    break;
                }
            }
        });
        let client = TcpPostcardRecorderClient::new_with_transport_and_timeout(
            address,
            "node-1",
            "node-2",
            "peer-token-2",
            7,
            ClientTransport::Plain,
            Duration::from_millis(400),
        )
        .unwrap();

        let started = Instant::now();
        assert!(client.recorder_id().is_err());
        let elapsed = started.elapsed();

        let advertised = advertised_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(
            advertised > 0 && advertised <= 350,
            "advertised {advertised}ms"
        );
        assert!(
            elapsed < Duration::from_millis(550),
            "partial response exceeded the sender-owned deadline: {elapsed:?}"
        );
        server.join().unwrap();
    }

    #[test]
    fn framed_read_fence_uses_the_short_control_deadline() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (_stream, _) = listener.accept().unwrap();
            thread::sleep(Duration::from_secs(2));
        });
        let client = TcpPostcardRecorderClient::new_with_transport_and_timeout(
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
        assert!(client
            .observe_read_fence(ReadFenceRequest {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: LogHash::ZERO,
                slot: 1,
            })
            .is_err());
        assert!(started.elapsed() < Duration::from_millis(1_500));
        server.join().unwrap();
    }

    #[test]
    fn framed_record_transport_failure_releases_the_quorum_attempt_promptly() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (_stream, _) = listener.accept().unwrap();
            thread::sleep(Duration::from_secs(2));
        });
        let client = TcpPostcardRecorderClient::new_with_transport_and_timeout(
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
        let result = client.record(RecordRequest {
            cluster_id: "cluster".into(),
            epoch: 1,
            config_id: 1,
            config_digest: LogHash::ZERO,
            slot: 1,
            step: 1,
            proposal: rhiza_quepaxa::Proposal::nil(),
            command: None,
        });

        assert!(matches!(result, Err(Error::ProposeFailed)));
        assert!(started.elapsed() < Duration::from_millis(1_500));
        server.join().unwrap();
    }

    #[derive(Clone)]
    struct BlockingMutation {
        started: mpsc::Sender<()>,
        release: Arc<(Mutex<bool>, Condvar)>,
        completed: Arc<AtomicUsize>,
    }

    #[derive(Clone)]
    struct CountingMutation {
        calls: Arc<AtomicUsize>,
    }

    impl RecorderRpc for CountingMutation {
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
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
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

    #[tokio::test]
    async fn request_expired_before_dispatch_never_reaches_recorder() {
        let calls = Arc::new(AtomicUsize::new(0));
        let command = StoredCommand::new(rhiza_core::EntryType::Command, b"expired".to_vec());
        let permit = Arc::new(tokio::sync::Semaphore::new(1))
            .acquire_owned()
            .await
            .unwrap();

        let response = dispatch_with_deadline(
            CountingMutation {
                calls: Arc::clone(&calls),
            },
            RecorderRequestBody::StoreCommand {
                cluster_id: "rhiza:sql:cluster-a".into(),
                epoch: 1,
                config_id: 1,
                config_digest: LogHash::ZERO,
                command_hash: command.hash(),
                command,
            },
            Operation::StoreCommand,
            permit,
            Instant::now() - Duration::from_millis(1),
            "node-1".into(),
            peers().into(),
        )
        .await;

        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(matches!(
            response,
            RecorderResponseBody::StoreCommand(RpcResult::Error(message))
                if message.contains("deadline")
        ));
    }

    #[tokio::test]
    async fn saturated_server_returns_overload_without_calling_recorder() {
        let calls = Arc::new(AtomicUsize::new(0));
        let slots = Arc::new(tokio::sync::Semaphore::new(1));
        let held = Arc::clone(&slots).acquire_owned().await.unwrap();
        let (mut client, server_stream) = tokio::io::duplex(4096);
        let server = tokio::spawn(serve_connection(
            server_stream,
            CountingMutation {
                calls: Arc::clone(&calls),
            },
            peers().into(),
            7,
            slots,
        ));
        write_value_async(
            &mut client,
            &Hello {
                version: WIRE_VERSION,
                node_id: "node-2".into(),
                recovery_generation: 7,
                token: "peer-token-2".into(),
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            decode_exact::<HelloReply>(&read_frame_async(&mut client).await.unwrap()).unwrap(),
            HelloReply::Accepted { .. }
        ));
        let command = StoredCommand::new(rhiza_core::EntryType::Command, b"overloaded".to_vec());
        write_value_async(
            &mut client,
            &RequestFrame {
                version: WIRE_VERSION,
                request_id: 1,
                remaining_deadline_ms: 1_000,
                body: RecorderRequestBody::StoreCommand {
                    cluster_id: "rhiza:sql:cluster-a".into(),
                    epoch: 1,
                    config_id: 1,
                    config_digest: LogHash::ZERO,
                    command_hash: command.hash(),
                    command,
                },
            },
        )
        .await
        .unwrap();

        let response: ResponseFrame =
            decode_exact(&read_frame_async(&mut client).await.unwrap()).unwrap();
        assert!(matches!(
            response.body,
            RecorderResponseBody::StoreCommand(RpcResult::Overloaded)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        drop(client);
        drop(held);
        server.await.unwrap().unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn server_deadline_returns_while_admitted_mutation_finishes_and_shutdown_drains_it() {
        let (started_tx, started_rx) = mpsc::channel();
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let completed = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(serve_recorder_tcp(
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
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        write_value_async(
            &mut stream,
            &Hello {
                version: WIRE_VERSION,
                node_id: "node-2".into(),
                recovery_generation: 7,
                token: "peer-token-2".into(),
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            decode_exact::<HelloReply>(&read_frame_async(&mut stream).await.unwrap()).unwrap(),
            HelloReply::Accepted { .. }
        ));
        let membership = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
        let command = StoredCommand::new(rhiza_core::EntryType::Command, b"slow".to_vec());
        write_value_async(
            &mut stream,
            &RequestFrame {
                version: WIRE_VERSION,
                request_id: 1,
                remaining_deadline_ms: 50,
                body: RecorderRequestBody::StoreCommand {
                    cluster_id: "rhiza:sql:cluster-a".into(),
                    epoch: 1,
                    config_id: 1,
                    config_digest: membership.digest(),
                    command_hash: command.hash(),
                    command,
                },
            },
        )
        .await
        .unwrap();
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let response =
            tokio::time::timeout(Duration::from_millis(300), read_frame_async(&mut stream)).await;
        shutdown_tx.send(()).unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!server.is_finished());
        let (released, ready) = &*release;
        *released.lock().unwrap() = true;
        ready.notify_all();
        server.await.unwrap().unwrap();
        assert_eq!(completed.load(Ordering::SeqCst), 1);
        let response = response
            .expect("server must answer the advertised deadline")
            .unwrap();
        assert!(matches!(
            decode_exact::<ResponseFrame>(&response).unwrap().body,
            RecorderResponseBody::StoreCommand(RpcResult::Error(message))
                if message.contains("deadline")
        ));
    }

    #[test]
    fn postcard_decoder_rejects_trailing_bytes_and_wrong_hello_version() {
        assert_eq!(WIRE_VERSION, 3);
        assert_eq!(RECORDER_TLS_ALPN, b"rhiza-recorder/3");
        let hello = Hello {
            version: WIRE_VERSION,
            node_id: "node-1".into(),
            recovery_generation: 7,
            token: "peer-token-1".into(),
        };
        let mut encoded = postcard::to_allocvec(&hello).unwrap();
        encoded.push(0);
        assert!(decode_exact::<Hello>(&encoded).is_err());

        let wrong_version = Hello {
            version: WIRE_VERSION + 1,
            ..hello
        };
        assert!(!hello_authenticated(&wrong_version, &[], 7));
    }

    #[test]
    fn recorder_tcp_endpoint_accepts_socket_and_dns_addresses_without_paths() {
        assert!(validate_recorder_tcp_endpoint("127.0.0.1:8082").is_ok());
        assert!(validate_recorder_tcp_endpoint("node-1.internal:8082").is_ok());
        assert!(validate_recorder_tcp_endpoint("[::1]:8082").is_ok());
        assert!(validate_recorder_tcp_endpoint("127.0.0.1").is_err());
        assert!(validate_recorder_tcp_endpoint("127.0.0.1:8082/path").is_err());
    }

    #[tokio::test]
    async fn frame_reader_rejects_zero_oversize_and_truncated_frames() {
        for length in [0_u32, u32::try_from(MAX_HTTP_BODY_BYTES + 1).unwrap()] {
            let (mut writer, mut reader) = tokio::io::duplex(16);
            writer.write_all(&length.to_be_bytes()).await.unwrap();
            assert!(read_frame_async(&mut reader).await.is_err());
        }

        let (mut writer, mut reader) = tokio::io::duplex(16);
        writer.write_all(&4_u32.to_be_bytes()).await.unwrap();
        writer.write_all(&[1, 2]).await.unwrap();
        drop(writer);
        assert!(read_frame_async(&mut reader).await.is_err());
    }
}
