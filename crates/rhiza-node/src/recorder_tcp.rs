use std::{
    fmt,
    io::{self, Read, Write},
    net::{SocketAddr, TcpStream, ToSocketAddrs},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Condvar, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

use rhiza_core::{LogHash, StoredCommand};
use rhiza_quepaxa::{
    DecisionProof, EffectBundleBinding, Error, Membership, ReadFenceObservation, ReadFenceRequest,
    RecordRequest, RecordSummary, RecorderRpc, RejectReason,
};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_rustls::TlsAcceptor;

use crate::{
    authenticated_proposer_admitted, peer_credentials_authenticated, preserve_mutation_outcome,
    recorder_decode::{decode_postcard_exact_bounded, RecorderDecodeLimits, RecorderWireRoot},
    recorder_error_from_wire, recorder_wire_error, valid_recorder_command, valid_recorder_record,
    PeerConfig, RecorderWireError, DEFAULT_PEER_CONCURRENCY, MAX_HTTP_BODY_BYTES,
    QUORUM_RECORD_REQUEST_TIMEOUT, READ_FENCE_REQUEST_TIMEOUT,
};

const WIRE_VERSION: u16 = 5;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const CALL_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECTIONS_PER_LANE: usize = 2;
const MAX_SERVER_CONNECTIONS: usize = DEFAULT_PEER_CONCURRENCY * 4;
const MAX_SERVER_DECODE_CONCURRENCY: usize = 32;
const RECORDER_TLS_ALPN: &[u8] = b"rhiza-recorder/5";

/// Lifecycle signals and ownership receipts for one recorder listener.
///
/// `started` is acknowledged only after the server future owns the listener
/// through its RAII owner. `listener_dropped` is sent only after that owner
/// has dropped the actual listener FD. Cooperative shutdown stops admission
/// and drains protocol work; force asks every connection scope to abort its
/// remaining asynchronous work and reap it.
pub struct RecorderIngressLifecycle {
    shutdown: tokio::sync::watch::Receiver<bool>,
    force: tokio::sync::watch::Receiver<bool>,
    started: Option<tokio::sync::oneshot::Sender<()>>,
    listener_dropped: Option<tokio::sync::oneshot::Sender<()>>,
}

impl RecorderIngressLifecycle {
    pub fn new(
        shutdown: tokio::sync::watch::Receiver<bool>,
        force: tokio::sync::watch::Receiver<bool>,
        started: tokio::sync::oneshot::Sender<()>,
        listener_dropped: tokio::sync::oneshot::Sender<()>,
    ) -> Self {
        Self {
            shutdown,
            force,
            started: Some(started),
            listener_dropped: Some(listener_dropped),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecorderTaskDisposition {
    Quiesced,
    Uncertain,
}

#[derive(Debug)]
pub struct RecorderIngressExit {
    pub result: Result<(), String>,
    pub tasks: RecorderTaskDisposition,
}

struct RecorderListenerOwner {
    listener: Option<tokio::net::TcpListener>,
    listener_dropped: Option<tokio::sync::oneshot::Sender<()>>,
}

impl RecorderListenerOwner {
    fn new(
        listener: tokio::net::TcpListener,
        listener_dropped: tokio::sync::oneshot::Sender<()>,
    ) -> Self {
        Self {
            listener: Some(listener),
            listener_dropped: Some(listener_dropped),
        }
    }

    fn listener(&self) -> &tokio::net::TcpListener {
        self.listener
            .as_ref()
            .expect("recorder listener is accepted only while open")
    }

    fn close(&mut self) {
        drop(self.listener.take());
        if let Some(listener_dropped) = self.listener_dropped.take() {
            let _ = listener_dropped.send(());
        }
    }
}

impl Drop for RecorderListenerOwner {
    fn drop(&mut self) {
        self.close();
    }
}

async fn wait_for_ingress_signal(signal: &mut tokio::sync::watch::Receiver<bool>) {
    loop {
        if *signal.borrow() {
            return;
        }
        if signal.changed().await.is_err() {
            return;
        }
    }
}

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
pub(crate) struct Hello {
    version: u16,
    node_id: String,
    recovery_generation: u64,
    token: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) enum HelloReply {
    Accepted { version: u16, recorder_id: String },
    Rejected,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct RequestFrame {
    version: u16,
    request_id: u64,
    remaining_deadline_ms: u32,
    body: RecorderRequestBody,
}

// Postcard encodes structs positionally. This borrowed form is deliberately
// wire-identical to `RequestFrame`, allowing allocation-free preflight before
// socket checkout while the owned body remains available for the final encode.
#[derive(Serialize)]
struct RequestFrameRef<'a> {
    version: u16,
    request_id: u64,
    remaining_deadline_ms: u32,
    body: &'a RecorderRequestBody,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) enum RecorderRequestBody {
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
    StageEffectBundleChunk {
        binding: EffectBundleBinding,
        manifest_command: StoredCommand,
        ordinal: u16,
        chunk: Vec<u8>,
    },
    FinalizeStagedEffectBundle {
        binding: EffectBundleBinding,
        manifest_command: StoredCommand,
    },
    FetchEffectBundleManifest {
        binding: EffectBundleBinding,
    },
    FetchEffectBundleChunk {
        binding: EffectBundleBinding,
        ordinal: u16,
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
pub(crate) struct ResponseFrame {
    version: u16,
    request_id: u64,
    body: RecorderResponseBody,
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) enum RecorderResponseBody {
    Identity(RpcResult<String>),
    StoreCommand(RpcResult<()>),
    FetchCommand(RpcResult<Option<StoredCommand>>),
    StageEffectBundleChunk(RpcResult<()>),
    FinalizeStagedEffectBundle(RpcResult<()>),
    FetchEffectBundleManifest(RpcResult<Option<StoredCommand>>),
    FetchEffectBundleChunk(RpcResult<Option<Vec<u8>>>),
    Record(RpcResult<RecordSummary>),
    InstallDecisionProof(RpcResult<()>),
    InspectDecisionProof(RpcResult<Option<DecisionProof>>),
    InspectRecordSummary(RpcResult<Option<RecordSummary>>),
    ObserveReadFence(RpcResult<ReadFenceObservation>),
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) enum RpcResult<T> {
    Ok(T),
    Rejected(RejectReason),
    Error(RecorderWireError),
    Overloaded,
}

impl<T> RpcResult<T> {
    fn from_result(result: rhiza_quepaxa::Result<T>) -> Self {
        match result {
            Ok(value) => Self::Ok(value),
            Err(Error::Rejected(reason)) => Self::Rejected(reason),
            Err(error) => Self::Error(recorder_wire_error(error)),
        }
    }

    fn into_result(self) -> rhiza_quepaxa::Result<T> {
        match self {
            Self::Ok(value) => Ok(value),
            Self::Rejected(reason) => Err(Error::Rejected(reason)),
            Self::Error(error) => Err(recorder_error_from_wire(error)),
            Self::Overloaded => Err(Error::Io("recorder RPC overloaded".into())),
        }
    }
}

pub async fn serve_recorder_tcp<R>(
    listener: tokio::net::TcpListener,
    recorder: R,
    peers: Vec<PeerConfig>,
    recovery_generation: u64,
    lifecycle: RecorderIngressLifecycle,
) -> RecorderIngressExit
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    serve_recorder_tcp_inner(
        listener,
        recorder,
        peers,
        recovery_generation,
        None,
        lifecycle,
    )
    .await
}

pub async fn serve_recorder_tcp_tls<R>(
    listener: tokio::net::TcpListener,
    recorder: R,
    peers: Vec<PeerConfig>,
    recovery_generation: u64,
    tls: RecorderTlsServerConfig,
    lifecycle: RecorderIngressLifecycle,
) -> RecorderIngressExit
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    serve_recorder_tcp_inner(
        listener,
        recorder,
        peers,
        recovery_generation,
        Some(tls.inner),
        lifecycle,
    )
    .await
}

async fn serve_recorder_tcp_inner<R>(
    listener: tokio::net::TcpListener,
    recorder: R,
    peers: Vec<PeerConfig>,
    recovery_generation: u64,
    tls: Option<Arc<rustls::ServerConfig>>,
    lifecycle: RecorderIngressLifecycle,
) -> RecorderIngressExit
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    #[cfg(test)]
    let response_write_test_server = listener.local_addr().ok();
    let peers: Arc<[PeerConfig]> = peers.into();
    let slots = Arc::new(tokio::sync::Semaphore::new(DEFAULT_PEER_CONCURRENCY));
    let decode_slots = Arc::new(tokio::sync::Semaphore::new(MAX_SERVER_DECODE_CONCURRENCY));
    let reported_connection_error = Arc::new(AtomicBool::new(false));
    run_recorder_ingress(
        listener,
        lifecycle,
        Arc::clone(&slots),
        DEFAULT_PEER_CONCURRENCY,
        MAX_SERVER_CONNECTIONS,
        "recorder TCP accept failed",
        move |stream, _peer_address, shutdown, force, connection| {
            let recorder = recorder.clone();
            let peers = Arc::clone(&peers);
            let slots = Arc::clone(&slots);
            let decode_slots = Arc::clone(&decode_slots);
            let tls = tls.clone();
            let reported_connection_error = Arc::clone(&reported_connection_error);
            async move {
                let _connection = connection;
                let signals = RecorderConnectionSignals { shutdown, force };
                let context = RecorderConnectionContext {
                    peers,
                    recovery_generation,
                    slots,
                    decode_slots,
                    signals: Some(signals),
                    #[cfg(test)]
                    response_write_test_server,
                };
                let result = if let Some(config) = tls {
                    serve_tls_connection(stream, config, recorder, context).await
                } else {
                    serve_connection_with_decode_slots(stream, recorder, context).await
                };
                if let Err(error) = &result {
                    if error != "connection closed"
                        && !reported_connection_error.swap(true, Ordering::Relaxed)
                    {
                        eprintln!("recorder TCP connection rejected: {error}");
                    }
                }
                result
            }
        },
    )
    .await
}

async fn run_recorder_ingress<H, Fut>(
    listener: tokio::net::TcpListener,
    mut lifecycle: RecorderIngressLifecycle,
    work_slots: Arc<tokio::sync::Semaphore>,
    work_slot_count: usize,
    connection_limit: usize,
    accept_error_prefix: &'static str,
    mut serve_connection: H,
) -> RecorderIngressExit
where
    H: FnMut(
        tokio::net::TcpStream,
        SocketAddr,
        tokio::sync::watch::Receiver<bool>,
        tokio::sync::watch::Receiver<bool>,
        tokio::sync::OwnedSemaphorePermit,
    ) -> Fut,
    Fut: std::future::Future<Output = Result<(), String>> + Send + 'static,
{
    let listener_dropped = lifecycle
        .listener_dropped
        .take()
        .expect("recorder lifecycle owns one listener-drop receipt");
    let mut listener = RecorderListenerOwner::new(listener, listener_dropped);
    let connection_slots = Arc::new(tokio::sync::Semaphore::new(connection_limit));
    let (connection_shutdown, _) = tokio::sync::watch::channel(false);
    let (connection_force, _) = tokio::sync::watch::channel(false);
    let mut connections = tokio::task::JoinSet::new();
    let mut result = Ok(());
    let mut forced = false;
    // A connection join error means its asynchronous scope was cancelled or
    // panicked outside the protocol's normal error path.  The listener can
    // still close cleanly, but that is not evidence that all admitted work
    // reached a known terminal state.
    let mut connection_tasks_uncertain = false;
    if let Some(started) = lifecycle.started.take() {
        let _ = started.send(());
    }

    loop {
        tokio::select! {
            biased;
            () = wait_for_ingress_signal(&mut lifecycle.force) => {
                forced = true;
                break;
            }
            () = wait_for_ingress_signal(&mut lifecycle.shutdown) => break,
            Some(completed) = connections.join_next(), if !connections.is_empty() => {
                if let Err(error) = completed {
                    eprintln!("recorder connection task failed: {error}");
                    connection_tasks_uncertain = true;
                }
            }
            accepted = listener.listener().accept() => {
                let (stream, peer_address) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        result = Err(format!("{accept_error_prefix}: {error}"));
                        break;
                    }
                };
                let Ok(connection) = connection_slots.clone().try_acquire_owned() else {
                    continue;
                };
                let _ = stream.set_nodelay(true);
                connections.spawn(serve_connection(
                    stream,
                    peer_address,
                    connection_shutdown.subscribe(),
                    connection_force.subscribe(),
                    connection,
                ));
            }
        }
    }

    // Dropping the actual listener is the only source of the out-of-band
    // Closed receipt. Existing connections remain owned by this scope.
    listener.close();
    if forced {
        connection_force.send_replace(true);
    } else {
        connection_shutdown.send_replace(true);
    }
    while !connections.is_empty() {
        tokio::select! {
            biased;
            () = wait_for_ingress_signal(&mut lifecycle.force), if !forced => {
                forced = true;
                connection_force.send_replace(true);
            }
            Some(completed) = connections.join_next() => {
                if let Err(error) = completed {
                    eprintln!("recorder connection task failed while draining: {error}");
                    connection_tasks_uncertain = true;
                }
            }
        }
    }

    if forced || connection_tasks_uncertain {
        return RecorderIngressExit {
            result,
            tasks: RecorderTaskDisposition::Uncertain,
        };
    }

    let work_slot_count = u32::try_from(work_slot_count).unwrap_or(u32::MAX);
    let work_drained = work_slots.acquire_many_owned(work_slot_count);
    tokio::pin!(work_drained);
    tokio::select! {
        biased;
        () = wait_for_ingress_signal(&mut lifecycle.force) => RecorderIngressExit {
            result,
            tasks: RecorderTaskDisposition::Uncertain,
        },
        drained = &mut work_drained => match drained {
            Ok(_permit) => RecorderIngressExit {
                result,
                tasks: RecorderTaskDisposition::Quiesced,
            },
            Err(_) => RecorderIngressExit {
                result: Err("recorder operation semaphore closed during shutdown".to_string()),
                tasks: RecorderTaskDisposition::Uncertain,
            },
        },
    }
}

struct RecorderConnectionSignals {
    shutdown: tokio::sync::watch::Receiver<bool>,
    force: tokio::sync::watch::Receiver<bool>,
}

struct RecorderConnectionContext {
    peers: Arc<[PeerConfig]>,
    recovery_generation: u64,
    slots: Arc<tokio::sync::Semaphore>,
    decode_slots: Arc<tokio::sync::Semaphore>,
    signals: Option<RecorderConnectionSignals>,
    #[cfg(test)]
    response_write_test_server: Option<SocketAddr>,
}

async fn serve_tls_connection<R>(
    stream: tokio::net::TcpStream,
    config: Arc<rustls::ServerConfig>,
    recorder: R,
    mut context: RecorderConnectionContext,
) -> Result<(), String>
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    let acceptor = TlsAcceptor::from(config);
    let handshake = tokio::time::timeout(CONNECT_TIMEOUT, acceptor.accept(stream));
    tokio::pin!(handshake);
    let mut signals = context.signals.take().expect("TLS connection has signals");
    let tls_stream = tokio::select! {
        biased;
        () = wait_for_ingress_signal(&mut signals.force) => return Ok(()),
        () = wait_for_ingress_signal(&mut signals.shutdown) => return Ok(()),
        handshake = &mut handshake => match handshake {
            Ok(Ok(tls_stream)) => tls_stream,
            Ok(Err(_)) => return Err("recorder TLS handshake failed".to_string()),
            Err(_) => return Err("recorder TLS handshake timed out".to_string()),
        },
    };
    if tls_stream.get_ref().1.alpn_protocol() != Some(RECORDER_TLS_ALPN) {
        return Err("recorder TLS ALPN negotiation failed".to_string());
    }
    context.signals = Some(signals);
    serve_connection_with_decode_slots(tls_stream, recorder, context).await
}

#[cfg(test)]
async fn serve_connection<R, S>(
    stream: S,
    recorder: R,
    peers: Arc<[PeerConfig]>,
    recovery_generation: u64,
    slots: Arc<tokio::sync::Semaphore>,
) -> Result<(), String>
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    serve_connection_with_decode_slots(
        stream,
        recorder,
        RecorderConnectionContext {
            peers,
            recovery_generation,
            slots,
            decode_slots: Arc::new(tokio::sync::Semaphore::new(MAX_SERVER_DECODE_CONCURRENCY)),
            signals: None,
            response_write_test_server: None,
        },
    )
    .await
}

async fn serve_connection_with_decode_slots<R, S>(
    mut stream: S,
    recorder: R,
    mut context: RecorderConnectionContext,
) -> Result<(), String>
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let hello_bytes = tokio::time::timeout(
        CALL_TIMEOUT,
        read_frame_with_signals(&mut stream, &mut context.signals),
    )
    .await
    .map_err(|_| "recorder HELLO timed out".to_string())??;
    let Some(hello_bytes) = hello_bytes else {
        return Ok(());
    };
    let hello: Hello = decode_framed_with_gate(&hello_bytes, &context.decode_slots).await?;
    if !hello_authenticated(&hello, &context.peers, context.recovery_generation) {
        let rejection = write_value_async_with_timeout(
            &mut stream,
            &HelloReply::Rejected,
            "recorder HELLO rejection",
        );
        let _ = await_unless_forced(&mut context.signals, rejection).await;
        return Err("recorder HELLO rejected".into());
    }
    let permit = match context.slots.clone().try_acquire_owned() {
        Ok(permit) => Arc::new(permit),
        Err(_) => {
            let rejection = write_value_async_with_timeout(
                &mut stream,
                &HelloReply::Rejected,
                "recorder HELLO overload rejection",
            );
            let _ = await_unless_forced(&mut context.signals, rejection).await;
            return Err("recorder HELLO overloaded".into());
        }
    };
    let identity_recorder = recorder.clone();
    // Keep one permit reference in the connection scope through the full
    // HELLO response write. The blocking task owns the other reference, so a
    // force/deadline that detaches the task cannot make its work invisible to
    // ingress quiescence.
    let backend_permit = Arc::clone(&permit);
    let identity = tokio::task::spawn_blocking(move || {
        let _permit = backend_permit;
        identity_recorder.recorder_id(&rhiza_quepaxa::RecorderRpcContext::with_timeout(
            CALL_TIMEOUT,
        ))
    });
    let Some(recorder_id) = await_unless_forced(&mut context.signals, identity).await else {
        return Ok(());
    };
    let recorder_id = recorder_id
        .map_err(|error| format!("recorder identity task failed: {error}"))?
        .map_err(|error| error.to_string())?;
    let hello_reply = HelloReply::Accepted {
        version: WIRE_VERSION,
        recorder_id,
    };
    let hello_response =
        write_value_async_with_timeout(&mut stream, &hello_reply, "recorder HELLO response");
    let Some(hello_response) = await_unless_forced(&mut context.signals, hello_response).await
    else {
        return Ok(());
    };
    hello_response?;
    drop(permit);

    loop {
        let request = match read_frame_with_signals(&mut stream, &mut context.signals).await {
            Ok(Some(bytes)) => {
                decode_request_framed_with_gate(&bytes, &context.decode_slots).await?
            }
            Ok(None) => return Ok(()),
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
        let permit = match context.slots.clone().try_acquire_owned() {
            Ok(permit) => Arc::new(permit),
            Err(_) => {
                let overload = write_response_async_with_timeout(
                    &mut stream,
                    request_id,
                    operation,
                    overloaded_response(operation),
                    "recorder overload response",
                );
                let Some(overload) = await_unless_forced(&mut context.signals, overload).await
                else {
                    return Ok(());
                };
                overload?;
                continue;
            }
        };
        let dispatch = dispatch_with_deadline(
            recorder.clone(),
            request.body,
            operation,
            Arc::clone(&permit),
            dispatch_deadline,
            hello.node_id.clone(),
            Arc::clone(&context.peers),
        );
        let Some(body) = await_unless_forced(&mut context.signals, dispatch).await else {
            return Ok(());
        };
        #[cfg(test)]
        response_write_test_gate_before_write(context.response_write_test_server, request_id).await;
        let response = write_response_async_with_timeout(
            &mut stream,
            request_id,
            operation,
            body,
            "recorder response",
        );
        let Some(response) = await_unless_forced(&mut context.signals, response).await else {
            return Ok(());
        };
        response?;
        // A response-side reference makes the admission slot cover protocol
        // work through the completed socket write. A timed-out blocking
        // dispatch retains its clone until it really returns.
        drop(permit);
    }
}

async fn read_frame_with_signals<R>(
    reader: &mut R,
    signals: &mut Option<RecorderConnectionSignals>,
) -> Result<Option<Vec<u8>>, String>
where
    R: tokio::io::AsyncRead + Unpin,
{
    match signals {
        Some(signals) => tokio::select! {
            biased;
            () = wait_for_ingress_signal(&mut signals.force) => Ok(None),
            () = wait_for_ingress_signal(&mut signals.shutdown) => Ok(None),
            frame = read_frame_async(reader) => frame.map(Some),
        },
        None => read_frame_async(reader).await.map(Some),
    }
}

async fn await_unless_forced<T, F>(
    signals: &mut Option<RecorderConnectionSignals>,
    work: F,
) -> Option<T>
where
    F: std::future::Future<Output = T>,
{
    match signals {
        Some(signals) => tokio::select! {
            biased;
            () = wait_for_ingress_signal(&mut signals.force) => None,
            result = work => Some(result),
        },
        None => Some(work.await),
    }
}

async fn decode_framed_with_gate<T>(
    bytes: &[u8],
    decode_slots: &Arc<tokio::sync::Semaphore>,
) -> Result<T, String>
where
    T: RecorderWireRoot,
{
    let _permit = decode_slots
        .acquire()
        .await
        .map_err(|_| "recorder decode semaphore closed".to_string())?;
    decode_postcard_exact_bounded(
        bytes,
        RecorderDecodeLimits::for_wire_bytes(MAX_HTTP_BODY_BYTES),
    )
    .map_err(|error| error.to_string())
}

async fn decode_request_framed_with_gate(
    bytes: &[u8],
    decode_slots: &Arc<tokio::sync::Semaphore>,
) -> Result<RequestFrame, String> {
    let _permit = decode_slots
        .acquire()
        .await
        .map_err(|_| "recorder decode semaphore closed".to_string())?;
    #[cfg(test)]
    let test_hook = request_decode_test_hook_after_permit(bytes).await;
    let decoded = decode_postcard_exact_bounded(
        bytes,
        RecorderDecodeLimits::for_wire_bytes(MAX_HTTP_BODY_BYTES),
    );
    #[cfg(test)]
    if let (Some(hook), Err(error)) = (&test_hook, &decoded) {
        let _ = hook
            .events
            .send(RequestDecodeTestEvent::Failed(error.to_string()));
    }
    decoded.map_err(|error| error.to_string())
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
enum RequestDecodeTestEvent {
    Entered,
    Failed(String),
}

#[cfg(test)]
#[derive(Clone)]
struct RequestDecodeTestHook {
    request_ids: Arc<std::collections::BTreeSet<u64>>,
    events: tokio::sync::mpsc::UnboundedSender<RequestDecodeTestEvent>,
    release: Option<Arc<tokio::sync::Notify>>,
}

#[cfg(test)]
static REQUEST_DECODE_TEST_HOOK: std::sync::OnceLock<
    std::sync::Mutex<Option<Arc<RequestDecodeTestHook>>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
static REQUEST_DECODE_TEST_LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> =
    std::sync::OnceLock::new();

#[cfg(test)]
fn request_decode_test_hook_slot() -> &'static std::sync::Mutex<Option<Arc<RequestDecodeTestHook>>>
{
    REQUEST_DECODE_TEST_HOOK.get_or_init(|| std::sync::Mutex::new(None))
}

#[cfg(test)]
fn request_decode_test_lock() -> &'static tokio::sync::Mutex<()> {
    REQUEST_DECODE_TEST_LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

#[cfg(test)]
struct RequestDecodeTestHookGuard;

#[cfg(test)]
impl Drop for RequestDecodeTestHookGuard {
    fn drop(&mut self) {
        *request_decode_test_hook_slot().lock().unwrap() = None;
    }
}

#[cfg(test)]
fn install_request_decode_test_hook(hook: RequestDecodeTestHook) -> RequestDecodeTestHookGuard {
    let mut slot = request_decode_test_hook_slot().lock().unwrap();
    assert!(
        slot.is_none(),
        "request decode test hook is already installed"
    );
    *slot = Some(Arc::new(hook));
    RequestDecodeTestHookGuard
}

#[cfg(test)]
async fn request_decode_test_hook_after_permit(bytes: &[u8]) -> Option<Arc<RequestDecodeTestHook>> {
    // This is intentionally a prefix-only test discriminator: production
    // still performs exactly one owned RequestFrame decode after the permit.
    let request_id = {
        let mut deserializer = postcard::Deserializer::from_bytes(bytes);
        let _version = u16::deserialize(&mut deserializer).ok()?;
        u64::deserialize(&mut deserializer).ok()?
    };
    let hook = request_decode_test_hook_slot().lock().unwrap().clone()?;
    if !hook.request_ids.contains(&request_id) {
        return None;
    }
    let _ = hook.events.send(RequestDecodeTestEvent::Entered);
    if let Some(release) = &hook.release {
        release.notified().await;
    }
    Some(hook)
}

#[cfg(test)]
struct ResponseWriteTestGate {
    server: SocketAddr,
    request_ids: std::collections::BTreeSet<u64>,
    entered: tokio::sync::mpsc::UnboundedSender<u64>,
    released: AtomicBool,
    release_notification: tokio::sync::Notify,
}

#[cfg(test)]
impl ResponseWriteTestGate {
    fn release(&self) {
        if !self.released.swap(true, Ordering::AcqRel) {
            self.release_notification.notify_waiters();
        }
    }

    async fn wait(&self) {
        loop {
            if self.released.load(Ordering::Acquire) {
                return;
            }
            let notified = self.release_notification.notified();
            if self.released.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
}

#[cfg(test)]
type ResponseWriteTestGates = Vec<(u64, Arc<ResponseWriteTestGate>)>;

#[cfg(test)]
static RESPONSE_WRITE_TEST_GATES: std::sync::OnceLock<std::sync::Mutex<ResponseWriteTestGates>> =
    std::sync::OnceLock::new();

#[cfg(test)]
static NEXT_RESPONSE_WRITE_TEST_GATE: AtomicU64 = AtomicU64::new(1);

#[cfg(test)]
fn response_write_test_gates() -> &'static std::sync::Mutex<ResponseWriteTestGates> {
    RESPONSE_WRITE_TEST_GATES.get_or_init(|| std::sync::Mutex::new(Vec::new()))
}

#[cfg(test)]
struct InstalledResponseWriteTestGate {
    id: u64,
    gate: Arc<ResponseWriteTestGate>,
}

#[cfg(test)]
impl InstalledResponseWriteTestGate {
    fn release(&self) {
        self.gate.release();
    }
}

#[cfg(test)]
impl Drop for InstalledResponseWriteTestGate {
    fn drop(&mut self) {
        self.gate.release();
        response_write_test_gates()
            .lock()
            .unwrap()
            .retain(|(id, _)| *id != self.id);
    }
}

#[cfg(test)]
fn install_response_write_test_gate(
    server: SocketAddr,
    request_ids: std::collections::BTreeSet<u64>,
    entered: tokio::sync::mpsc::UnboundedSender<u64>,
) -> InstalledResponseWriteTestGate {
    assert!(
        !request_ids.is_empty(),
        "response write gate must be scoped"
    );
    let id = NEXT_RESPONSE_WRITE_TEST_GATE.fetch_add(1, Ordering::Relaxed);
    assert_ne!(id, 0, "response write test gate identity exhausted");
    let gate = Arc::new(ResponseWriteTestGate {
        server,
        request_ids,
        entered,
        released: AtomicBool::new(false),
        release_notification: tokio::sync::Notify::new(),
    });
    let mut gates = response_write_test_gates().lock().unwrap();
    assert!(
        !gates.iter().any(|(_, existing)| existing.server == server),
        "response write test gate already installed for server {server}"
    );
    gates.push((id, Arc::clone(&gate)));
    InstalledResponseWriteTestGate { id, gate }
}

#[cfg(test)]
async fn response_write_test_gate_before_write(server: Option<SocketAddr>, request_id: u64) {
    let Some(server) = server else {
        return;
    };
    let gate = response_write_test_gates()
        .lock()
        .unwrap()
        .iter()
        .find(|(_, gate)| gate.server == server && gate.request_ids.contains(&request_id))
        .map(|(_, gate)| Arc::clone(gate));
    if let Some(gate) = gate {
        let _ = gate.entered.send(request_id);
        gate.wait().await;
    }
}

async fn dispatch_with_deadline<R>(
    recorder: R,
    body: RecorderRequestBody,
    operation: Operation,
    permit: Arc<tokio::sync::OwnedSemaphorePermit>,
    deadline: Instant,
    authenticated_peer_id: String,
    peers: Arc<[PeerConfig]>,
) -> RecorderResponseBody
where
    R: RecorderRpc + Send + Sync + 'static,
{
    if deadline <= Instant::now() {
        return error_response(operation, Error::RpcDeadlineExceeded);
    }
    let dispatched = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        dispatch(
            recorder,
            body,
            &rhiza_quepaxa::RecorderRpcContext::with_deadline(deadline),
            &authenticated_peer_id,
            &peers,
        )
    });
    match tokio::time::timeout_at(deadline.into(), dispatched).await {
        Ok(Ok(response)) => response,
        Ok(Err(_)) => error_response(operation, operation.panic_error()),
        Err(_) => error_response(operation, Error::RpcDeadlineExceeded),
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
    StageEffectBundleChunk,
    FinalizeStagedEffectBundle,
    FetchEffectBundleManifest,
    FetchEffectBundleChunk,
    Record,
    InstallDecisionProof,
    InspectDecisionProof,
    InspectRecordSummary,
    ObserveReadFence,
}

impl Operation {
    const fn is_mutating(self) -> bool {
        matches!(
            self,
            Self::StoreCommand
                | Self::StageEffectBundleChunk
                | Self::FinalizeStagedEffectBundle
                | Self::Record
                | Self::InstallDecisionProof
        )
    }

    const fn panic_error(self) -> Error {
        if self.is_mutating() {
            Error::UnknownOutcome
        } else {
            Error::ProposeFailed
        }
    }
}

fn response_operation(request: &RecorderRequestBody) -> Operation {
    match request {
        RecorderRequestBody::Identity => Operation::Identity,
        RecorderRequestBody::StoreCommand { .. } => Operation::StoreCommand,
        RecorderRequestBody::FetchCommand { .. } => Operation::FetchCommand,
        RecorderRequestBody::StageEffectBundleChunk { .. } => Operation::StageEffectBundleChunk,
        RecorderRequestBody::FinalizeStagedEffectBundle { .. } => {
            Operation::FinalizeStagedEffectBundle
        }
        RecorderRequestBody::FetchEffectBundleManifest { .. } => {
            Operation::FetchEffectBundleManifest
        }
        RecorderRequestBody::FetchEffectBundleChunk { .. } => Operation::FetchEffectBundleChunk,
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
    context: &rhiza_quepaxa::RecorderRpcContext,
    authenticated_peer_id: &str,
    peers: &[PeerConfig],
) -> RecorderResponseBody {
    match request {
        RecorderRequestBody::Identity => {
            RecorderResponseBody::Identity(RpcResult::from_result(recorder.recorder_id(context)))
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
                    context,
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
        } => {
            RecorderResponseBody::FetchCommand(RpcResult::from_result(recorder.fetch_command_for(
                context,
                cluster_id,
                epoch,
                config_id,
                config_digest,
                command_hash,
            )))
        }
        RecorderRequestBody::StageEffectBundleChunk {
            binding,
            manifest_command,
            ordinal,
            chunk,
        } => RecorderResponseBody::StageEffectBundleChunk(RpcResult::from_result(
            recorder.stage_effect_bundle_chunk(context, binding, manifest_command, ordinal, chunk),
        )),
        RecorderRequestBody::FinalizeStagedEffectBundle {
            binding,
            manifest_command,
        } => RecorderResponseBody::FinalizeStagedEffectBundle(RpcResult::from_result(
            recorder.finalize_staged_effect_bundle(context, binding, manifest_command),
        )),
        RecorderRequestBody::FetchEffectBundleManifest { binding } => {
            RecorderResponseBody::FetchEffectBundleManifest(RpcResult::from_result(
                recorder.fetch_effect_bundle_manifest(context, binding),
            ))
        }
        RecorderRequestBody::FetchEffectBundleChunk { binding, ordinal } => {
            RecorderResponseBody::FetchEffectBundleChunk(RpcResult::from_result(
                recorder.fetch_effect_bundle_chunk(context, binding, ordinal),
            ))
        }
        RecorderRequestBody::Record(request) => {
            let result = if !valid_recorder_record(&request)
                || !authenticated_proposer_admitted(
                    authenticated_peer_id,
                    &request.proposal.proposer_id,
                    peers,
                ) {
                Err(Error::Rejected(RejectReason::InvalidRequest))
            } else {
                recorder.record(context, request)
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
                Membership::from_voters(members).and_then(|membership| {
                    recorder.install_decision_proof(context, proof, &membership)
                })
            };
            RecorderResponseBody::InstallDecisionProof(RpcResult::from_result(result))
        }
        RecorderRequestBody::InspectDecisionProof { slot } => {
            RecorderResponseBody::InspectDecisionProof(RpcResult::from_result(
                recorder.inspect_decision_proof(context, slot),
            ))
        }
        RecorderRequestBody::InspectRecordSummary { slot } => {
            RecorderResponseBody::InspectRecordSummary(RpcResult::from_result(
                recorder.inspect_record_summary(context, slot),
            ))
        }
        RecorderRequestBody::ObserveReadFence(request) => RecorderResponseBody::ObserveReadFence(
            RpcResult::from_result(recorder.observe_read_fence(context, request)),
        ),
    }
}

fn overloaded_response(operation: Operation) -> RecorderResponseBody {
    match operation {
        Operation::Identity => RecorderResponseBody::Identity(RpcResult::Overloaded),
        Operation::StoreCommand => RecorderResponseBody::StoreCommand(RpcResult::Overloaded),
        Operation::FetchCommand => RecorderResponseBody::FetchCommand(RpcResult::Overloaded),
        Operation::StageEffectBundleChunk => {
            RecorderResponseBody::StageEffectBundleChunk(RpcResult::Overloaded)
        }
        Operation::FinalizeStagedEffectBundle => {
            RecorderResponseBody::FinalizeStagedEffectBundle(RpcResult::Overloaded)
        }
        Operation::FetchEffectBundleManifest => {
            RecorderResponseBody::FetchEffectBundleManifest(RpcResult::Overloaded)
        }
        Operation::FetchEffectBundleChunk => {
            RecorderResponseBody::FetchEffectBundleChunk(RpcResult::Overloaded)
        }
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

fn error_response(operation: Operation, error: Error) -> RecorderResponseBody {
    let error = recorder_wire_error(error);
    match operation {
        Operation::Identity => RecorderResponseBody::Identity(RpcResult::Error(error)),
        Operation::StoreCommand => RecorderResponseBody::StoreCommand(RpcResult::Error(error)),
        Operation::FetchCommand => RecorderResponseBody::FetchCommand(RpcResult::Error(error)),
        Operation::StageEffectBundleChunk => {
            RecorderResponseBody::StageEffectBundleChunk(RpcResult::Error(error))
        }
        Operation::FinalizeStagedEffectBundle => {
            RecorderResponseBody::FinalizeStagedEffectBundle(RpcResult::Error(error))
        }
        Operation::FetchEffectBundleManifest => {
            RecorderResponseBody::FetchEffectBundleManifest(RpcResult::Error(error))
        }
        Operation::FetchEffectBundleChunk => {
            RecorderResponseBody::FetchEffectBundleChunk(RpcResult::Error(error))
        }
        Operation::Record => RecorderResponseBody::Record(RpcResult::Error(error)),
        Operation::InstallDecisionProof => {
            RecorderResponseBody::InstallDecisionProof(RpcResult::Error(error))
        }
        Operation::InspectDecisionProof => {
            RecorderResponseBody::InspectDecisionProof(RpcResult::Error(error))
        }
        Operation::InspectRecordSummary => {
            RecorderResponseBody::InspectRecordSummary(RpcResult::Error(error))
        }
        Operation::ObserveReadFence => {
            RecorderResponseBody::ObserveReadFence(RpcResult::Error(error))
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
            return Err("connection closed".into());
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

/// Postcard's allocating helper grows a `Vec` before a caller can compare the
/// result with the wire cap. Count first with a hard ceiling, then allocate
/// exactly the accepted size for the real serialization pass.
struct BoundedPostcardSize {
    size: usize,
    limit: usize,
}

impl postcard::ser_flavors::Flavor for BoundedPostcardSize {
    type Output = usize;

    fn try_push(&mut self, _byte: u8) -> postcard::Result<()> {
        self.try_extend(&[0])
    }

    fn try_extend(&mut self, bytes: &[u8]) -> postcard::Result<()> {
        let Some(size) = self.size.checked_add(bytes.len()) else {
            return Err(postcard::Error::SerializeBufferFull);
        };
        if size > self.limit {
            return Err(postcard::Error::SerializeBufferFull);
        }
        self.size = size;
        Ok(())
    }

    fn finalize(self) -> postcard::Result<Self::Output> {
        Ok(self.size)
    }
}

pub(super) fn bounded_postcard_size<T: Serialize + ?Sized>(
    value: &T,
    limit: usize,
) -> Result<usize, String> {
    postcard::serialize_with_flavor(value, BoundedPostcardSize { size: 0, limit }).map_err(
        |error| {
            if matches!(error, postcard::Error::SerializeBufferFull) {
                "recorder frame exceeds limit".to_string()
            } else {
                error.to_string()
            }
        },
    )
}

pub(super) fn bounded_postcard_encode<T: Serialize + ?Sized>(
    value: &T,
    limit: usize,
) -> Result<Vec<u8>, String> {
    let size = bounded_postcard_size(value, limit)?;
    let mut encoded = vec![0_u8; size];
    let encoded_len = postcard::to_slice(value, &mut encoded)
        .map_err(|error| error.to_string())?
        .len();
    if encoded_len != size {
        return Err("recorder frame serialization size changed".into());
    }
    Ok(encoded)
}

async fn write_value_async<W: tokio::io::AsyncWrite + Unpin, T: Serialize>(
    writer: &mut W,
    value: &T,
) -> Result<(), String> {
    let encoded = bounded_postcard_encode(value, MAX_HTTP_BODY_BYTES)?;
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

async fn write_response_async<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    request_id: u64,
    operation: Operation,
    body: RecorderResponseBody,
) -> Result<(), String> {
    let response = ResponseFrame {
        version: WIRE_VERSION,
        request_id,
        body,
    };
    let encoded = match bounded_postcard_encode(&response, MAX_HTTP_BODY_BYTES) {
        Ok(encoded) => encoded,
        Err(_) => {
            let fallback = ResponseFrame {
                version: WIRE_VERSION,
                request_id,
                body: error_response(
                    operation,
                    Error::Decode("recorder response exceeds frame limit".into()),
                ),
            };
            bounded_postcard_encode(&fallback, MAX_HTTP_BODY_BYTES)
                .map_err(|_| "recorder response fallback exceeds frame limit".to_string())?
        }
    };
    write_frame_async(writer, &encoded).await
}

async fn write_response_async_with_timeout<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    request_id: u64,
    operation: Operation,
    body: RecorderResponseBody,
    description: &str,
) -> Result<(), String> {
    tokio::time::timeout(
        CALL_TIMEOUT,
        write_response_async(writer, request_id, operation, body),
    )
    .await
    .map_err(|_| format!("{description} timed out"))?
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

fn decode_framed<T>(bytes: &[u8]) -> Result<T, String>
where
    T: RecorderWireRoot,
{
    decode_postcard_exact_bounded(
        bytes,
        RecorderDecodeLimits::for_wire_bytes(MAX_HTTP_BODY_BYTES),
    )
    .map_err(|error| error.to_string())
}

#[cfg(test)]
fn decode_exact<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    let (value, trailing) = postcard::take_from_bytes(bytes).map_err(|error| error.to_string())?;
    if !trailing.is_empty() {
        return Err("trailing recorder frame bytes".into());
    }
    Ok(value)
}

fn frame_length(frame: &[u8]) -> Result<[u8; 4], String> {
    frame_length_from_size(frame.len())
}

fn frame_length_from_size(size: usize) -> Result<[u8; 4], String> {
    if size == 0 || size > MAX_HTTP_BODY_BYTES {
        return Err("invalid recorder frame length".into());
    }
    let length = u32::try_from(size).map_err(|_| "recorder frame is too large")?;
    Ok(length.to_be_bytes())
}

struct ConnectionPool {
    state: Mutex<PoolState>,
    available: Condvar,
}

/// Owns one `PoolState::open` reservation while a connector result is in
/// flight.  A successful receiver explicitly disarms it because the checked
/// out stream then represents that open slot; every other path (failed send,
/// queued completion dropped after cancellation, or failed connection) drops
/// the reservation and returns the slot exactly once.
struct ConnectionReservation {
    pool: Arc<ConnectionPool>,
    active: bool,
}

impl ConnectionReservation {
    fn new(pool: Arc<ConnectionPool>) -> Self {
        Self { pool, active: true }
    }

    fn into_checked_out(mut self) {
        self.active = false;
    }
}

impl Drop for ConnectionReservation {
    fn drop(&mut self) {
        if self.active {
            TcpPostcardRecorderClient::discard_pool(&self.pool);
        }
    }
}

struct ConnectorCompletion {
    result: Result<RecorderClientStream, String>,
    reservation: ConnectionReservation,
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

    fn set_nonblocking(&self, nonblocking: bool) -> std::io::Result<()> {
        match self {
            Self::Plain(stream) => stream.inner.set_nonblocking(nonblocking),
            Self::Tls(stream) => stream.sock.inner.set_nonblocking(nonblocking),
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

/// Bounds each blocking socket operation to a short slice of the caller's
/// absolute deadline.  Between slices we observe the shared cancellation
/// token; this lets shutdown return without waiting for a long read/write
/// timeout while retaining the original deadline as the hard cap.
struct ContextualStream<'a> {
    inner: &'a mut RecorderClientStream,
    context: &'a rhiza_quepaxa::RecorderRpcContext,
    deadline: Instant,
}

impl ContextualStream<'_> {
    fn check(&mut self) -> io::Result<()> {
        self.context.check().map_err(|error| {
            io::Error::new(
                match error {
                    Error::RpcDeadlineExceeded => io::ErrorKind::TimedOut,
                    _ => io::ErrorKind::Interrupted,
                },
                error.to_string(),
            )
        })?;
        let slice = Instant::now()
            .checked_add(Duration::from_millis(10))
            .unwrap_or(self.deadline)
            .min(self.deadline);
        self.inner.set_deadline(slice);
        Ok(())
    }

    fn retryable_timeout(&self, error: &io::Error) -> bool {
        matches!(
            error.kind(),
            io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
        ) && Instant::now() < self.deadline
    }
}

impl Read for ContextualStream<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        loop {
            self.check()?;
            match self.inner.read(buffer) {
                Err(error) if self.retryable_timeout(&error) => {
                    thread::sleep(Duration::from_millis(5));
                    continue;
                }
                result => return result,
            }
        }
    }
}

impl Write for ContextualStream<'_> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        loop {
            self.check()?;
            match self.inner.write(buffer) {
                Err(error) if self.retryable_timeout(&error) => {
                    thread::sleep(Duration::from_millis(5));
                    continue;
                }
                result => return result,
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        loop {
            self.check()?;
            match self.inner.flush() {
                Err(error) if self.retryable_timeout(&error) => {
                    thread::sleep(Duration::from_millis(5));
                    continue;
                }
                result => return result,
            }
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
    consensus: Arc<ConnectionPool>,
    control: Arc<ConnectionPool>,
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
            consensus: Arc::new(ConnectionPool::new()),
            control: Arc::new(ConnectionPool::new()),
            next_request_id: AtomicU64::new(1),
        })
    }

    fn exchange(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
        request: RecorderRequestBody,
        consensus: bool,
        mutating: bool,
    ) -> rhiza_quepaxa::Result<RecorderResponseBody> {
        self.exchange_with_timeout(context, request, consensus, mutating, self.call_timeout)
    }

    fn exchange_with_timeout(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
        request: RecorderRequestBody,
        consensus: bool,
        mutating: bool,
        transport_cap: Duration,
    ) -> rhiza_quepaxa::Result<RecorderResponseBody> {
        context.check()?;
        let timeout = context
            .remaining()
            .ok_or(Error::RpcDeadlineExceeded)?
            .min(transport_cap)
            .min(self.call_timeout);
        if timeout.is_zero() {
            return Err(Error::RpcDeadlineExceeded);
        }
        let deadline = Instant::now() + timeout;
        let operation = response_operation(&request);
        // Count a maximum-size envelope before a socket is checked out. This
        // proves a caller request fits without allocating it or advertising a
        // deadline that goes stale while connection setup runs.
        match preflight_request_frame_size(context, &request) {
            Ok(()) => {}
            Err(FramePreflightError::Context(error)) => return Err(error),
            Err(FramePreflightError::Decode(error)) => return Err(Error::Decode(error)),
        }
        let pool = if consensus {
            &self.consensus
        } else {
            &self.control
        };
        let mut stream = self.checkout(pool, deadline, context)?;
        stream.set_deadline(deadline);
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let frame = match prepare_request_frame(context, deadline, request_id, request) {
            Ok(frame) => frame,
            Err(FramePreflightError::Context(error)) => {
                self.discard(pool);
                return Err(error);
            }
            Err(FramePreflightError::Decode(error)) => {
                self.discard(pool);
                return Err(Error::Decode(error));
            }
        };
        // The write helper returns the exact number of bytes accepted by the
        // underlying stream. A mutation becomes ambiguous only after at least
        // one request byte left this process; pre-write cancellation, expiry,
        // and transport failures remain definite local outcomes.
        let result = {
            let mut contextual = ContextualStream {
                inner: &mut stream,
                context,
                deadline,
            };
            let request_bytes = match write_prepared_frame(&mut contextual, context, &frame) {
                Ok(bytes) => bytes,
                Err(error) => return self.finish_frame_write_error(pool, mutating, error),
            };
            read_frame_with_context(&mut contextual, context)
                .map_err(|error| (request_bytes, error))
                .and_then(|bytes| {
                    decode_framed::<ResponseFrame>(&bytes)
                        .map_err(FrameIoError::Decode)
                        .map_err(|error| (request_bytes, error))
                })
        };
        match result {
            Ok(response)
                if response.version == WIRE_VERSION
                    && response.request_id == request_id
                    && response_matches(operation, &response.body) =>
            {
                self.checkin(pool, stream);
                match context.check() {
                    Ok(()) => Ok(response.body),
                    Err(_) if mutating => Err(Error::UnknownOutcome),
                    Err(error) => Err(error),
                }
            }
            Ok(_) => {
                self.discard(pool);
                Err(if mutating {
                    Error::UnknownOutcome
                } else {
                    Error::Decode("recorder response envelope mismatch".into())
                })
            }
            Err((request_bytes, error)) => {
                self.discard(pool);
                Err(frame_error_outcome(error, request_bytes, mutating))
            }
        }
    }

    fn finish_frame_write_error(
        &self,
        pool: &ConnectionPool,
        mutating: bool,
        error: FrameWriteError,
    ) -> rhiza_quepaxa::Result<RecorderResponseBody> {
        self.discard(pool);
        Err(frame_error_outcome(
            error.error,
            error.bytes_written,
            mutating,
        ))
    }

    fn checkout(
        &self,
        pool: &Arc<ConnectionPool>,
        deadline: Instant,
        context: &rhiza_quepaxa::RecorderRpcContext,
    ) -> rhiza_quepaxa::Result<RecorderClientStream> {
        loop {
            context.check()?;
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
                // DNS, TCP connect, TLS, and HELLO are all potentially
                // blocking std APIs.  Bound them to the existing lane open
                // count, then let the caller poll its own cancellation
                // context.  If the caller leaves first, the worker drops any
                // completed socket and releases its reservation; it can never
                // add that socket to the pool.
                let (reply, receive) = std::sync::mpsc::sync_channel(1);
                let reservation = ConnectionReservation::new(Arc::clone(pool));
                let address = self.address.clone();
                let expected_recorder_id = self.expected_recorder_id.clone();
                let local_node_id = self.local_node_id.clone();
                let peer_token = self.peer_token.clone();
                let recovery_generation = self.recovery_generation;
                let transport = self.transport.clone();
                if let Err(error) = thread::Builder::new()
                    .name("rhiza-recorder-connect".into())
                    .spawn(move || {
                        let remaining = deadline.saturating_duration_since(Instant::now());
                        let worker_context =
                            rhiza_quepaxa::RecorderRpcContext::with_timeout(remaining);
                        let result = Self::connect_with_config(
                            address,
                            expected_recorder_id,
                            local_node_id,
                            peer_token,
                            recovery_generation,
                            transport,
                            deadline,
                            &worker_context,
                        );
                        // `ConnectorCompletion` owns the reservation until a
                        // receiver has accepted a stream.  In particular,
                        // dropping a queued completion after cancellation
                        // releases it without relying on the caller.
                        let _ = reply.send(ConnectorCompletion {
                            result,
                            reservation,
                        });
                    })
                {
                    return Err(Error::Io(format!(
                        "cannot start recorder connector: {error}"
                    )));
                }
                loop {
                    context.check()?;
                    let Some(remaining) = context.remaining() else {
                        return Err(Error::RpcDeadlineExceeded);
                    };
                    match receive.recv_timeout(remaining.min(Duration::from_millis(10))) {
                        Ok(ConnectorCompletion {
                            result: Ok(stream),
                            reservation,
                        }) => match context.check() {
                            Ok(()) => {
                                reservation.into_checked_out();
                                return Ok(stream);
                            }
                            Err(error) => return Err(error),
                        },
                        Ok(ConnectorCompletion {
                            result: Err(error),
                            reservation,
                        }) => {
                            drop(reservation);
                            return match context.check() {
                                Err(error) => Err(error),
                                Ok(()) => Err(Error::Io(error)),
                            };
                        }
                        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => continue,
                        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                            return Err(Error::Io(
                                "recorder connector stopped unexpectedly".into(),
                            ));
                        }
                    }
                }
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Error::Io("recorder connection checkout timed out".into()));
            }
            let (next, wait) = pool
                .available
                .wait_timeout(state, remaining.min(Duration::from_millis(10)))
                .map_err(|_| Error::Io("recorder connection pool lock poisoned".into()))?;
            drop(next);
            if wait.timed_out() && deadline <= Instant::now() {
                return Err(Error::RpcDeadlineExceeded);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn connect_with_config(
        address: String,
        expected_recorder_id: String,
        local_node_id: String,
        peer_token: String,
        recovery_generation: u64,
        transport: ClientTransport,
        deadline: Instant,
        context: &rhiza_quepaxa::RecorderRpcContext,
    ) -> Result<RecorderClientStream, String> {
        context.check().map_err(|error| error.to_string())?;
        let remaining = deadline.saturating_duration_since(Instant::now());
        let connect_timeout = CONNECT_TIMEOUT.min(remaining);
        if connect_timeout.is_zero() {
            return Err("recorder connect deadline exceeded".into());
        }
        let mut last_error = None;
        let mut socket = None;
        let resolved_addresses = address
            .as_str()
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
        let mut stream = match &transport {
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
        stream
            .set_nonblocking(true)
            .map_err(|error| format!("cannot make recorder HELLO socket nonblocking: {error}"))?;
        let reply: HelloReply = {
            let mut contextual = ContextualStream {
                inner: &mut stream,
                context,
                deadline,
            };
            write_value_sync(
                &mut contextual,
                &Hello {
                    version: WIRE_VERSION,
                    node_id: local_node_id,
                    recovery_generation,
                    token: peer_token,
                },
            )?;
            decode_framed(&read_frame_sync(&mut contextual)?)?
        };
        stream
            .set_nonblocking(false)
            .map_err(|error| format!("cannot restore recorder socket blocking mode: {error}"))?;
        match reply {
            HelloReply::Accepted {
                version,
                recorder_id,
            } if version == WIRE_VERSION && recorder_id == expected_recorder_id => Ok(stream),
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
        Self::discard_pool(pool);
    }

    fn discard_pool(pool: &ConnectionPool) {
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
            | (
                Operation::StageEffectBundleChunk,
                RecorderResponseBody::StageEffectBundleChunk(_)
            )
            | (
                Operation::FinalizeStagedEffectBundle,
                RecorderResponseBody::FinalizeStagedEffectBundle(_)
            )
            | (
                Operation::FetchEffectBundleManifest,
                RecorderResponseBody::FetchEffectBundleManifest(_)
            )
            | (
                Operation::FetchEffectBundleChunk,
                RecorderResponseBody::FetchEffectBundleChunk(_)
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
    fn recorder_id(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
    ) -> rhiza_quepaxa::Result<String> {
        match self.exchange(context, RecorderRequestBody::Identity, false, false)? {
            RecorderResponseBody::Identity(result) => result.into_result(),
            _ => Err(Error::Decode("recorder response operation mismatch".into())),
        }
    }

    fn store_command_for(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
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
        match self.exchange(context, request, false, true)? {
            RecorderResponseBody::StoreCommand(result) => {
                result.into_result().map_err(preserve_mutation_outcome)
            }
            _ => Err(Error::Decode("recorder response operation mismatch".into())),
        }
    }

    fn fetch_command_for(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
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
        match self.exchange(context, request, false, false)? {
            RecorderResponseBody::FetchCommand(result) => result.into_result(),
            _ => Err(Error::Decode("recorder response operation mismatch".into())),
        }
    }

    fn stage_effect_bundle_chunk(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
        binding: EffectBundleBinding,
        manifest_command: StoredCommand,
        ordinal: u16,
        chunk: Vec<u8>,
    ) -> rhiza_quepaxa::Result<()> {
        match self.exchange(
            context,
            RecorderRequestBody::StageEffectBundleChunk {
                binding,
                manifest_command,
                ordinal,
                chunk,
            },
            false,
            true,
        )? {
            RecorderResponseBody::StageEffectBundleChunk(result) => {
                result.into_result().map_err(preserve_mutation_outcome)
            }
            _ => Err(Error::Decode("recorder response operation mismatch".into())),
        }
    }

    fn finalize_staged_effect_bundle(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
        binding: EffectBundleBinding,
        manifest_command: StoredCommand,
    ) -> rhiza_quepaxa::Result<()> {
        match self.exchange(
            context,
            RecorderRequestBody::FinalizeStagedEffectBundle {
                binding,
                manifest_command,
            },
            false,
            true,
        )? {
            RecorderResponseBody::FinalizeStagedEffectBundle(result) => {
                result.into_result().map_err(preserve_mutation_outcome)
            }
            _ => Err(Error::Decode("recorder response operation mismatch".into())),
        }
    }

    fn fetch_effect_bundle_manifest(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
        binding: EffectBundleBinding,
    ) -> rhiza_quepaxa::Result<Option<StoredCommand>> {
        match self.exchange(
            context,
            RecorderRequestBody::FetchEffectBundleManifest { binding },
            false,
            false,
        )? {
            RecorderResponseBody::FetchEffectBundleManifest(result) => result.into_result(),
            _ => Err(Error::Decode("recorder response operation mismatch".into())),
        }
    }

    fn fetch_effect_bundle_chunk(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
        binding: EffectBundleBinding,
        ordinal: u16,
    ) -> rhiza_quepaxa::Result<Option<Vec<u8>>> {
        match self.exchange(
            context,
            RecorderRequestBody::FetchEffectBundleChunk { binding, ordinal },
            false,
            false,
        )? {
            RecorderResponseBody::FetchEffectBundleChunk(result) => result.into_result(),
            _ => Err(Error::Decode("recorder response operation mismatch".into())),
        }
    }

    fn record(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
        request: RecordRequest,
    ) -> rhiza_quepaxa::Result<RecordSummary> {
        let response = self.exchange_with_timeout(
            context,
            RecorderRequestBody::Record(request),
            true,
            true,
            QUORUM_RECORD_REQUEST_TIMEOUT,
        )?;
        match response {
            RecorderResponseBody::Record(result) => {
                result.into_result().map_err(preserve_mutation_outcome)
            }
            _ => Err(Error::Decode("recorder response operation mismatch".into())),
        }
    }

    fn install_decision_proof(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
        proof: DecisionProof,
        membership: &Membership,
    ) -> rhiza_quepaxa::Result<()> {
        let request = RecorderRequestBody::InstallDecisionProof {
            proof,
            members: membership.members().to_vec(),
        };
        match self.exchange(context, request, true, true)? {
            RecorderResponseBody::InstallDecisionProof(result) => {
                result.into_result().map_err(preserve_mutation_outcome)
            }
            _ => Err(Error::Decode("recorder response operation mismatch".into())),
        }
    }

    fn inspect_decision_proof(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
        slot: u64,
    ) -> rhiza_quepaxa::Result<Option<DecisionProof>> {
        let request = RecorderRequestBody::InspectDecisionProof { slot };
        match self.exchange(context, request, false, false)? {
            RecorderResponseBody::InspectDecisionProof(result) => result.into_result(),
            _ => Err(Error::Decode("recorder response operation mismatch".into())),
        }
    }

    fn inspect_record_summary(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
        slot: u64,
    ) -> rhiza_quepaxa::Result<Option<RecordSummary>> {
        let request = RecorderRequestBody::InspectRecordSummary { slot };
        match self.exchange(context, request, false, false)? {
            RecorderResponseBody::InspectRecordSummary(result) => result.into_result(),
            _ => Err(Error::Decode("recorder response operation mismatch".into())),
        }
    }

    fn supports_context_read_fence(&self) -> bool {
        true
    }

    fn observe_read_fence(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
        request: ReadFenceRequest,
    ) -> rhiza_quepaxa::Result<ReadFenceObservation> {
        match self.exchange_with_timeout(
            context,
            RecorderRequestBody::ObserveReadFence(request),
            false,
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
        return Err(Error::RpcDeadlineExceeded);
    }
    Ok(u32::try_from(remaining.as_millis())
        .unwrap_or(u32::MAX)
        .max(1))
}

#[derive(Debug)]
enum FramePreflightError {
    Context(Error),
    Decode(String),
}

#[derive(Debug)]
enum FrameIoError {
    Context(Error),
    Transport(String),
    Decode(String),
}

#[derive(Debug)]
struct FrameWriteError {
    error: FrameIoError,
    bytes_written: usize,
}

fn preflight_request_frame_size(
    context: &rhiza_quepaxa::RecorderRpcContext,
    body: &RecorderRequestBody,
) -> Result<(), FramePreflightError> {
    context.check().map_err(FramePreflightError::Context)?;
    // Use largest varints for the two envelope counters. If this maximum form
    // fits, the final frame built after connection setup also fits; unlike the
    // final frame this pass allocates nothing and cannot advertise a stale
    // deadline to the peer.
    bounded_postcard_size(
        &RequestFrameRef {
            version: WIRE_VERSION,
            request_id: u64::MAX,
            remaining_deadline_ms: u32::MAX,
            body,
        },
        MAX_HTTP_BODY_BYTES,
    )
    .map_err(FramePreflightError::Decode)?;
    context.check().map_err(FramePreflightError::Context)
}

fn prepare_request_frame(
    context: &rhiza_quepaxa::RecorderRpcContext,
    deadline: Instant,
    request_id: u64,
    body: RecorderRequestBody,
) -> Result<Vec<u8>, FramePreflightError> {
    context.check().map_err(FramePreflightError::Context)?;
    let remaining_deadline_ms =
        advertised_remaining_deadline_ms(deadline).map_err(FramePreflightError::Context)?;
    let request = RequestFrame {
        version: WIRE_VERSION,
        request_id,
        remaining_deadline_ms,
        body,
    };
    let encoded_size = bounded_postcard_size(&request, MAX_HTTP_BODY_BYTES)
        .map_err(FramePreflightError::Decode)?;
    let length = frame_length_from_size(encoded_size).map_err(FramePreflightError::Decode)?;
    context.check().map_err(FramePreflightError::Context)?;
    // Prefix and payload share one exact allocation, so a large accepted
    // request never coexists with a second full-size prefix frame.
    let mut frame = vec![0_u8; length.len() + encoded_size];
    frame[..length.len()].copy_from_slice(&length);
    let actual_size = postcard::to_slice(&request, &mut frame[length.len()..])
        .map_err(|error| FramePreflightError::Decode(error.to_string()))?
        .len();
    if actual_size != encoded_size {
        return Err(FramePreflightError::Decode(
            "recorder frame serialization size changed".into(),
        ));
    }
    Ok(frame)
}

fn classify_frame_io_error(
    context: &rhiza_quepaxa::RecorderRpcContext,
    error: io::Error,
) -> FrameIoError {
    match context.check() {
        Err(error) => FrameIoError::Context(error),
        Ok(()) => FrameIoError::Transport(error.to_string()),
    }
}

fn write_prepared_frame<W: Write>(
    writer: &mut W,
    context: &rhiza_quepaxa::RecorderRpcContext,
    frame: &[u8],
) -> Result<usize, FrameWriteError> {
    let mut written = 0_usize;
    while written < frame.len() {
        if let Err(error) = context.check() {
            return Err(FrameWriteError {
                error: FrameIoError::Context(error),
                bytes_written: written,
            });
        }
        match writer.write(&frame[written..]) {
            Ok(0) => {
                return Err(FrameWriteError {
                    error: FrameIoError::Transport(
                        "recorder request write returned zero bytes".into(),
                    ),
                    bytes_written: written,
                });
            }
            Ok(count) => written += count,
            Err(error) => {
                return Err(FrameWriteError {
                    error: classify_frame_io_error(context, error),
                    bytes_written: written,
                });
            }
        }
    }
    if let Err(error) = writer.flush() {
        return Err(FrameWriteError {
            error: classify_frame_io_error(context, error),
            bytes_written: written,
        });
    }
    Ok(written)
}

fn read_frame_with_context(
    reader: &mut impl Read,
    context: &rhiza_quepaxa::RecorderRpcContext,
) -> Result<Vec<u8>, FrameIoError> {
    let mut length = [0_u8; 4];
    read_exact_with_context(reader, context, &mut length)?;
    let length = usize::try_from(u32::from_be_bytes(length)).unwrap_or(usize::MAX);
    if length == 0 || length > MAX_HTTP_BODY_BYTES {
        return Err(FrameIoError::Decode("invalid recorder frame length".into()));
    }
    let mut frame = vec![0; length];
    read_exact_with_context(reader, context, &mut frame)?;
    Ok(frame)
}

fn read_exact_with_context(
    reader: &mut impl Read,
    context: &rhiza_quepaxa::RecorderRpcContext,
    buffer: &mut [u8],
) -> Result<(), FrameIoError> {
    let mut read = 0;
    while read < buffer.len() {
        context.check().map_err(FrameIoError::Context)?;
        match reader.read(&mut buffer[read..]) {
            Ok(0) => {
                return Err(FrameIoError::Transport(
                    "recorder response closed before a complete frame".into(),
                ));
            }
            Ok(count) => read += count,
            Err(error) => return Err(classify_frame_io_error(context, error)),
        }
    }
    Ok(())
}

fn frame_error_outcome(error: FrameIoError, request_bytes_written: usize, mutating: bool) -> Error {
    if mutating && request_bytes_written > 0 {
        return Error::UnknownOutcome;
    }
    match error {
        FrameIoError::Context(error) => error,
        FrameIoError::Transport(error) => Error::Io(error),
        FrameIoError::Decode(error) => Error::Decode(error),
    }
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
    let encoded = bounded_postcard_encode(value, MAX_HTTP_BODY_BYTES)?;
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
            atomic::{AtomicBool, AtomicUsize, Ordering},
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

    struct ScriptedWriter {
        actions: VecDeque<Result<usize, io::ErrorKind>>,
        bytes_written: usize,
        write_calls: usize,
    }

    impl ScriptedWriter {
        fn new(actions: impl IntoIterator<Item = Result<usize, io::ErrorKind>>) -> Self {
            Self {
                actions: actions.into_iter().collect(),
                bytes_written: 0,
                write_calls: 0,
            }
        }
    }

    impl Write for ScriptedWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.write_calls += 1;
            match self.actions.pop_front().unwrap_or(Ok(bytes.len())) {
                Ok(count) => {
                    let count = count.min(bytes.len());
                    self.bytes_written += count;
                    Ok(count)
                }
                Err(kind) => Err(io::Error::from(kind)),
            }
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    struct CancelAfterFirstWrite {
        cancelled: Arc<AtomicBool>,
        bytes_written: usize,
    }

    impl Write for CancelAfterFirstWrite {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            let count = bytes.len().min(1);
            self.bytes_written += count;
            self.cancelled.store(true, Ordering::SeqCst);
            Ok(count)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    struct ExpireAfterFirstWrite {
        until: Instant,
        bytes_written: usize,
    }

    impl Write for ExpireAfterFirstWrite {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            let count = bytes.len().min(1);
            self.bytes_written += count;
            while Instant::now() < self.until {
                thread::yield_now();
            }
            Ok(count)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
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

    fn prepared_identity_frame(context: &rhiza_quepaxa::RecorderRpcContext) -> Vec<u8> {
        prepare_request_frame(
            context,
            Instant::now() + Duration::from_secs(1),
            1,
            RecorderRequestBody::Identity,
        )
        .unwrap()
    }

    struct CancelledRead {
        cancelled: Arc<AtomicBool>,
        calls: usize,
    }

    impl Read for CancelledRead {
        fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
            self.calls += 1;
            self.cancelled.store(true, Ordering::SeqCst);
            Err(io::Error::new(io::ErrorKind::Interrupted, "cancelled read"))
        }
    }

    #[test]
    fn framed_exact_read_does_not_retry_a_context_interruption() {
        let cancelled = Arc::new(AtomicBool::new(false));
        let context = rhiza_quepaxa::RecorderRpcContext::with_timeout_and_cancellation(
            Duration::from_secs(1),
            Arc::clone(&cancelled),
        );
        let mut reader = CancelledRead {
            cancelled,
            calls: 0,
        };
        let mut buffer = [0_u8; 1];
        assert!(matches!(
            read_exact_with_context(&mut reader, &context, &mut buffer),
            Err(FrameIoError::Context(Error::RpcCancelled))
        ));
        assert_eq!(reader.calls, 1);
    }

    #[test]
    fn framed_request_preflight_returns_typed_cancel_and_deadline_without_writing() {
        let cancelled = Arc::new(AtomicBool::new(true));
        let cancelled_context = rhiza_quepaxa::RecorderRpcContext::with_timeout_and_cancellation(
            Duration::from_secs(1),
            cancelled,
        );
        assert!(matches!(
            prepare_request_frame(
                &cancelled_context,
                Instant::now() + Duration::from_secs(1),
                1,
                RecorderRequestBody::Identity,
            ),
            Err(FramePreflightError::Context(Error::RpcCancelled))
        ));
        let expired_context = rhiza_quepaxa::RecorderRpcContext::with_timeout(Duration::ZERO);
        assert!(matches!(
            prepare_request_frame(
                &expired_context,
                Instant::now(),
                1,
                RecorderRequestBody::Identity,
            ),
            Err(FramePreflightError::Context(Error::RpcDeadlineExceeded))
        ));
        assert!(matches!(
            advertised_remaining_deadline_ms(Instant::now()),
            Err(Error::RpcDeadlineExceeded)
        ));
    }

    #[test]
    fn framed_request_write_counter_distinguishes_zero_prefix_and_body_failures() {
        let context = rhiza_quepaxa::RecorderRpcContext::with_timeout(Duration::from_secs(1));
        let frame = prepared_identity_frame(&context);

        let mut zero = ScriptedWriter::new([Err(io::ErrorKind::ConnectionReset)]);
        let error = write_prepared_frame(&mut zero, &context, &frame).unwrap_err();
        assert_eq!(error.bytes_written, 0);
        assert_eq!(zero.bytes_written, 0);
        assert_eq!(zero.write_calls, 1);
        assert!(matches!(
            frame_error_outcome(error.error, 0, true),
            Error::Io(_)
        ));
        assert!(matches!(
            frame_error_outcome(FrameIoError::Transport("zero".into()), 0, false),
            Error::Io(_)
        ));

        let mut prefix = ScriptedWriter::new([Ok(1), Err(io::ErrorKind::BrokenPipe)]);
        let error = write_prepared_frame(&mut prefix, &context, &frame).unwrap_err();
        assert_eq!(error.bytes_written, 1);
        assert_eq!(prefix.bytes_written, 1);
        assert!(matches!(
            frame_error_outcome(error.error, 1, true),
            Error::UnknownOutcome
        ));
        assert!(matches!(
            frame_error_outcome(FrameIoError::Transport("prefix".into()), 1, false),
            Error::Io(_)
        ));

        let mut body =
            ScriptedWriter::new([Ok(usize::from(4_u8)), Ok(1), Err(io::ErrorKind::BrokenPipe)]);
        let error = write_prepared_frame(&mut body, &context, &frame).unwrap_err();
        assert_eq!(error.bytes_written, 5);
        assert_eq!(body.bytes_written, 5);
        assert!(matches!(
            frame_error_outcome(error.error, 5, true),
            Error::UnknownOutcome
        ));
        assert!(matches!(
            frame_error_outcome(FrameIoError::Transport("body".into()), 5, false),
            Error::Io(_)
        ));
    }

    #[test]
    fn framed_partial_request_cancel_and_deadline_are_ambiguous_only_for_mutations() {
        let cancelled = Arc::new(AtomicBool::new(false));
        let context = rhiza_quepaxa::RecorderRpcContext::with_timeout_and_cancellation(
            Duration::from_secs(1),
            Arc::clone(&cancelled),
        );
        let frame = prepared_identity_frame(&context);
        let mut cancelling = CancelAfterFirstWrite {
            cancelled,
            bytes_written: 0,
        };
        let error = write_prepared_frame(&mut cancelling, &context, &frame).unwrap_err();
        assert_eq!(error.bytes_written, 1);
        assert_eq!(cancelling.bytes_written, 1);
        assert!(matches!(
            error.error,
            FrameIoError::Context(Error::RpcCancelled)
        ));
        assert!(matches!(
            frame_error_outcome(FrameIoError::Context(Error::RpcCancelled), 1, true),
            Error::UnknownOutcome
        ));
        assert!(matches!(
            frame_error_outcome(FrameIoError::Context(Error::RpcCancelled), 1, false),
            Error::RpcCancelled
        ));

        // Leave enough scheduling headroom for the first accepted byte, then
        // deterministically cross the context deadline before the next write.
        let context = rhiza_quepaxa::RecorderRpcContext::with_timeout(Duration::from_millis(50));
        let frame = prepared_identity_frame(&context);
        let mut expiring = ExpireAfterFirstWrite {
            until: Instant::now() + Duration::from_millis(75),
            bytes_written: 0,
        };
        let error = write_prepared_frame(&mut expiring, &context, &frame).unwrap_err();
        assert_eq!(error.bytes_written, 1);
        assert_eq!(expiring.bytes_written, 1);
        assert!(matches!(
            error.error,
            FrameIoError::Context(Error::RpcDeadlineExceeded)
        ));
        assert!(matches!(
            frame_error_outcome(FrameIoError::Context(Error::RpcDeadlineExceeded), 1, true),
            Error::UnknownOutcome
        ));
        assert!(matches!(
            frame_error_outcome(FrameIoError::Context(Error::RpcDeadlineExceeded), 1, false),
            Error::RpcDeadlineExceeded
        ));
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
        assert!(client
            .recorder_id(&rhiza_quepaxa::RecorderRpcContext::with_timeout(
                Duration::from_millis(400),
            ))
            .is_err());
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
    fn framed_response_envelope_mismatch_is_unknown_only_after_a_mutation() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().unwrap();
                let _: Hello = decode_exact(&read_frame_sync(&mut stream).unwrap()).unwrap();
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
                let body = match request.body {
                    RecorderRequestBody::Record(_) => {
                        RecorderResponseBody::Record(RpcResult::Overloaded)
                    }
                    RecorderRequestBody::Identity => {
                        RecorderResponseBody::Identity(RpcResult::Overloaded)
                    }
                    _ => panic!("test only sends record and identity"),
                };
                write_value_sync(
                    &mut stream,
                    &ResponseFrame {
                        version: WIRE_VERSION,
                        request_id: request.request_id.wrapping_add(1),
                        body,
                    },
                )
                .unwrap();
            }
        });
        let client = TcpPostcardRecorderClient::new_with_transport_and_timeout(
            address,
            "node-1",
            "node-2",
            "peer-token-2",
            7,
            ClientTransport::Plain,
            Duration::from_secs(2),
        )
        .unwrap();
        let context = rhiza_quepaxa::RecorderRpcContext::with_timeout(Duration::from_secs(2));
        let mutation = client.record(
            &context,
            RecordRequest {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: LogHash::ZERO,
                slot: 1,
                step: 1,
                proposal: rhiza_quepaxa::Proposal::nil(),
                command: None,
            },
        );
        assert!(matches!(mutation, Err(Error::UnknownOutcome)));
        assert!(matches!(
            client.recorder_id(&context),
            Err(Error::Decode(_))
        ));
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
            .observe_read_fence(
                &rhiza_quepaxa::RecorderRpcContext::with_timeout(Duration::from_secs(5),),
                ReadFenceRequest {
                    cluster_id: "cluster".into(),
                    epoch: 1,
                    config_id: 1,
                    config_digest: LogHash::ZERO,
                    slot: 1,
                }
            )
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
        let result = client.record(
            &rhiza_quepaxa::RecorderRpcContext::with_timeout(Duration::from_secs(5)),
            RecordRequest {
                cluster_id: "cluster".into(),
                epoch: 1,
                config_id: 1,
                config_digest: LogHash::ZERO,
                slot: 1,
                step: 1,
                proposal: rhiza_quepaxa::Proposal::nil(),
                command: None,
            },
        );

        // The peer never completes HELLO, so no recorder request was framed;
        // this is a definite pre-send transport failure, not an ambiguity.
        assert!(matches!(result, Err(Error::Io(_))));
        assert!(started.elapsed() < Duration::from_millis(1_500));
        server.join().unwrap();
    }

    #[test]
    fn framed_hello_observes_shared_cancellation_without_waiting_for_the_deadline() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (accepted_tx, accepted_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let server = thread::spawn(move || {
            let (_stream, _) = listener.accept().unwrap();
            accepted_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        });
        let client = Arc::new(
            TcpPostcardRecorderClient::new_with_transport_and_timeout(
                address,
                "node-1",
                "node-2",
                "peer-token-2",
                7,
                ClientTransport::Plain,
                Duration::from_secs(5),
            )
            .unwrap(),
        );
        let cancelled = Arc::new(AtomicBool::new(false));
        let context = rhiza_quepaxa::RecorderRpcContext::with_timeout_and_cancellation(
            Duration::from_secs(5),
            Arc::clone(&cancelled),
        );
        let calling = Arc::clone(&client);
        let caller = thread::spawn(move || calling.recorder_id(&context));
        accepted_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        let started = Instant::now();
        cancelled.store(true, Ordering::SeqCst);
        let result = caller.join().unwrap();

        assert!(matches!(result, Err(Error::RpcCancelled)));
        assert!(
            started.elapsed() < Duration::from_millis(250),
            "HELLO cancellation was not observed promptly: {:?}",
            started.elapsed()
        );
        release_tx.send(()).unwrap();
        server.join().unwrap();
    }

    #[test]
    fn cancelled_preframe_connectors_are_bounded_by_the_lane_capacity() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (accepted_tx, accepted_rx) = mpsc::sync_channel(CONNECTIONS_PER_LANE);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let server = thread::spawn(move || {
            let mut streams = Vec::with_capacity(CONNECTIONS_PER_LANE);
            for _ in 0..CONNECTIONS_PER_LANE {
                let (stream, _) = listener.accept().unwrap();
                accepted_tx.send(()).unwrap();
                streams.push(stream);
            }
            release_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            drop(streams);
        });
        let client = Arc::new(
            TcpPostcardRecorderClient::new_with_transport_and_timeout(
                address,
                "node-1",
                "node-2",
                "peer-token-2",
                7,
                ClientTransport::Plain,
                Duration::from_secs(5),
            )
            .unwrap(),
        );

        for _ in 0..CONNECTIONS_PER_LANE {
            let cancelled = Arc::new(AtomicBool::new(false));
            let context = rhiza_quepaxa::RecorderRpcContext::with_timeout_and_cancellation(
                Duration::from_secs(5),
                Arc::clone(&cancelled),
            );
            let calling = Arc::clone(&client);
            let caller = thread::spawn(move || calling.recorder_id(&context));
            accepted_rx.recv_timeout(Duration::from_secs(1)).unwrap();
            cancelled.store(true, Ordering::SeqCst);
            assert!(matches!(caller.join().unwrap(), Err(Error::RpcCancelled)));
        }

        for _ in 0..4 {
            let cancelled = Arc::new(AtomicBool::new(false));
            let context = rhiza_quepaxa::RecorderRpcContext::with_timeout_and_cancellation(
                Duration::from_secs(5),
                Arc::clone(&cancelled),
            );
            let calling = Arc::clone(&client);
            let caller = thread::spawn(move || calling.recorder_id(&context));
            thread::sleep(Duration::from_millis(20));
            cancelled.store(true, Ordering::SeqCst);
            assert!(matches!(caller.join().unwrap(), Err(Error::RpcCancelled)));
        }
        assert!(accepted_rx.recv_timeout(Duration::from_millis(50)).is_err());
        assert_eq!(
            client.control.state.lock().unwrap().open,
            CONNECTIONS_PER_LANE
        );

        release_tx.send(()).unwrap();
        server.join().unwrap();
        let cleanup_deadline = Instant::now() + Duration::from_secs(1);
        while client.control.state.lock().unwrap().open != 0 {
            assert!(
                Instant::now() < cleanup_deadline,
                "connector lease was not released"
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn dropped_queued_connector_completions_release_every_reservation() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let client = TcpPostcardRecorderClient::new_with_transport_and_timeout(
            address,
            "node-1",
            "node-2",
            "peer-token-2",
            7,
            ClientTransport::Plain,
            Duration::from_secs(2),
        )
        .unwrap();
        let pool = Arc::clone(&client.control);
        let (sender, receiver) = mpsc::sync_channel(CONNECTIONS_PER_LANE);
        {
            let mut state = pool.state.lock().unwrap();
            state.open = CONNECTIONS_PER_LANE;
        }
        for _ in 0..CONNECTIONS_PER_LANE {
            let stream = TcpStream::connect(address).unwrap();
            sender
                .send(ConnectorCompletion {
                    result: Ok(RecorderClientStream::Plain(DeadlineStream::new(
                        stream,
                        Instant::now() + Duration::from_secs(2),
                    ))),
                    reservation: ConnectionReservation::new(Arc::clone(&pool)),
                })
                .unwrap();
        }
        drop(sender);
        drop(receiver);

        assert_eq!(pool.state.lock().unwrap().open, 0);
        // A fresh public checkout is now admissible; it must not inherit
        // either cancelled caller's reservation.
        let server = thread::spawn(move || {
            for _ in 0..CONNECTIONS_PER_LANE {
                let (_cancelled_stream, _) = listener.accept().unwrap();
            }
            let (mut stream, _) = listener.accept().unwrap();
            let _: Hello = decode_exact(&read_frame_sync(&mut stream).unwrap()).unwrap();
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
            write_value_sync(
                &mut stream,
                &ResponseFrame {
                    version: WIRE_VERSION,
                    request_id: request.request_id,
                    body: RecorderResponseBody::Identity(RpcResult::Ok("node-1".into())),
                },
            )
            .unwrap();
        });
        assert_eq!(
            client
                .recorder_id(&rhiza_quepaxa::RecorderRpcContext::with_timeout(
                    Duration::from_secs(2),
                ))
                .unwrap(),
            "node-1"
        );
        server.join().unwrap();
    }

    #[derive(Clone)]
    struct BlockingMutation {
        started: mpsc::Sender<()>,
        release: Arc<(Mutex<bool>, Condvar)>,
        completed: Arc<AtomicUsize>,
    }

    #[derive(Clone)]
    struct BlockingIdentity {
        started: mpsc::Sender<()>,
        release: Arc<(Mutex<bool>, Condvar)>,
        completed: Arc<AtomicUsize>,
    }

    #[derive(Clone)]
    struct CountingMutation {
        calls: Arc<AtomicUsize>,
    }

    #[derive(Clone)]
    struct CountingIdentity {
        calls: Arc<AtomicUsize>,
    }

    #[derive(Clone)]
    struct OversizedFetch {
        command: Arc<StoredCommand>,
        calls: Arc<AtomicUsize>,
    }

    impl RecorderRpc for CountingMutation {
        fn recorder_id(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
        ) -> rhiza_quepaxa::Result<String> {
            Ok("node-1".into())
        }

        fn store_command_for(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
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

        fn install_decision_proof(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
            _proof: DecisionProof,
            _membership: &Membership,
        ) -> rhiza_quepaxa::Result<()> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    impl RecorderRpc for OversizedFetch {
        fn recorder_id(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
        ) -> rhiza_quepaxa::Result<String> {
            Ok("node-1".into())
        }

        fn fetch_command_for(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
            _cluster_id: String,
            _epoch: u64,
            _config_id: u64,
            _config_digest: LogHash,
            _command_hash: LogHash,
        ) -> rhiza_quepaxa::Result<Option<StoredCommand>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(Some((*self.command).clone()))
        }
    }

    impl RecorderRpc for CountingIdentity {
        fn recorder_id(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
        ) -> rhiza_quepaxa::Result<String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok("node-1".into())
        }
    }

    impl RecorderRpc for BlockingMutation {
        fn recorder_id(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
        ) -> rhiza_quepaxa::Result<String> {
            Ok("node-1".into())
        }

        fn store_command_for(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
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

    impl RecorderRpc for BlockingIdentity {
        fn recorder_id(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
        ) -> rhiza_quepaxa::Result<String> {
            self.started.send(()).unwrap();
            let (released, ready) = &*self.release;
            let mut released = released.lock().unwrap();
            while !*released {
                released = ready.wait(released).unwrap();
            }
            self.completed.fetch_add(1, Ordering::SeqCst);
            Ok("node-1".into())
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

    struct TestIngressControl {
        shutdown: tokio::sync::watch::Sender<bool>,
        force: tokio::sync::watch::Sender<bool>,
        started: tokio::sync::oneshot::Receiver<()>,
        listener_dropped: tokio::sync::oneshot::Receiver<()>,
    }

    struct TestRunningIngressControl {
        shutdown: tokio::sync::watch::Sender<bool>,
        _force: tokio::sync::watch::Sender<bool>,
        _listener_dropped: tokio::sync::oneshot::Receiver<()>,
    }

    fn test_ingress_lifecycle() -> (TestIngressControl, RecorderIngressLifecycle) {
        let (shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
        let (force, force_rx) = tokio::sync::watch::channel(false);
        let (started, started_rx) = tokio::sync::oneshot::channel();
        let (listener_dropped, listener_dropped_rx) = tokio::sync::oneshot::channel();
        (
            TestIngressControl {
                shutdown,
                force,
                started: started_rx,
                listener_dropped: listener_dropped_rx,
            },
            RecorderIngressLifecycle::new(shutdown_rx, force_rx, started, listener_dropped),
        )
    }

    async fn start_actual_recorder_server<R>(
        recorder: R,
    ) -> (
        SocketAddr,
        TestRunningIngressControl,
        tokio::task::JoinHandle<RecorderIngressExit>,
    )
    where
        R: RecorderRpc + Clone + Send + Sync + 'static,
    {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (control, lifecycle) = test_ingress_lifecycle();
        let server = tokio::spawn(serve_recorder_tcp(
            listener,
            recorder,
            peers(),
            7,
            lifecycle,
        ));
        let TestIngressControl {
            shutdown,
            force,
            started,
            listener_dropped,
        } = control;
        started.await.unwrap();
        (
            address,
            TestRunningIngressControl {
                shutdown,
                _force: force,
                _listener_dropped: listener_dropped,
            },
            server,
        )
    }

    #[tokio::test]
    async fn ingress_marks_connection_task_panic_as_uncertain() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (control, lifecycle) = test_ingress_lifecycle();
        let (entered, entered_rx) = tokio::sync::oneshot::channel();
        let mut entered = Some(entered);
        let server = tokio::spawn(run_recorder_ingress(
            listener,
            lifecycle,
            Arc::new(tokio::sync::Semaphore::new(1)),
            1,
            1,
            "test accept failed",
            move |_stream, _peer, _shutdown, _force, _connection| {
                let entered = entered.take().expect("test admits exactly one connection");
                async move {
                    let _ = entered.send(());
                    panic!("test connection task panic");
                    #[allow(unreachable_code)]
                    Ok::<(), String>(())
                }
            },
        ));
        control.started.await.unwrap();
        let client = tokio::net::TcpStream::connect(address).await.unwrap();
        entered_rx.await.unwrap();
        control.shutdown.send_replace(true);
        control.listener_dropped.await.unwrap();
        let exit = server.await.unwrap();
        assert!(exit.result.is_ok());
        assert_eq!(exit.tasks, RecorderTaskDisposition::Uncertain);
        drop(client);
        drop(control.force);
    }

    async fn authenticated_tcp_hello(address: SocketAddr) -> tokio::net::TcpStream {
        let mut stream = tokio::time::timeout(
            Duration::from_secs(1),
            tokio::net::TcpStream::connect(address),
        )
        .await
        .unwrap()
        .unwrap();
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
            decode_exact::<HelloReply>(
                &tokio::time::timeout(Duration::from_secs(1), read_frame_async(&mut stream))
                    .await
                    .unwrap()
                    .unwrap(),
            )
            .unwrap(),
            HelloReply::Accepted { .. }
        ));
        stream
    }

    async fn next_decode_event(
        events: &mut tokio::sync::mpsc::UnboundedReceiver<RequestDecodeTestEvent>,
    ) -> RequestDecodeTestEvent {
        tokio::time::timeout(Duration::from_secs(1), events.recv())
            .await
            .unwrap()
            .unwrap()
    }

    #[test]
    fn bounded_postcard_encode_rejects_before_allocating_an_oversized_output() {
        // A bytes Vec has a one-byte sequence length prefix at these sizes.
        // The exact boundary proves the second pass allocates only accepted
        // output; the following byte is rejected by the counting pass.
        assert_eq!(
            bounded_postcard_encode(&vec![0_u8; 31], 32).unwrap().len(),
            32
        );
        assert!(bounded_postcard_encode(&vec![0_u8; 32], 32).is_err());
    }

    #[tokio::test]
    async fn framed_decode_gate_blocks_until_a_permit_is_released() {
        // Keep the gate parameterized in the helper so this remains a
        // deterministic test of the same acquire/decode/release path used by
        // the production-wide 32-slot gate.
        let gate = Arc::new(tokio::sync::Semaphore::new(1));
        let held = Arc::clone(&gate).acquire_owned().await.unwrap();
        let bytes = bounded_postcard_encode(
            &Hello {
                version: WIRE_VERSION,
                node_id: "node-2".into(),
                recovery_generation: 7,
                token: "peer-token-2".into(),
            },
            MAX_HTTP_BODY_BYTES,
        )
        .unwrap();
        let mut decode = tokio::spawn({
            let gate = Arc::clone(&gate);
            async move { decode_framed_with_gate::<Hello>(&bytes, &gate).await }
        });

        assert!(tokio::time::timeout(Duration::from_millis(25), &mut decode)
            .await
            .is_err());
        drop(held);
        let hello = tokio::time::timeout(Duration::from_secs(1), decode)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(hello.node_id, "node-2");
        assert_eq!(gate.available_permits(), 1);
    }

    #[tokio::test]
    async fn framed_oversized_decision_proof_does_not_dispatch_and_releases_decode_slot() {
        let calls = Arc::new(AtomicUsize::new(0));
        let operation_slots = Arc::new(tokio::sync::Semaphore::new(1));
        let decode_slots = Arc::new(tokio::sync::Semaphore::new(1));
        let summary = rhiza_quepaxa::RecorderSummary {
            recorder_id: "r".into(),
            slot: 1,
            step: 1,
            first_current: None,
            aggregate_prior: None,
        };
        let proof = DecisionProof::FastPath {
            cluster_id: "rhiza:sql:cluster-a".into(),
            slot: 1,
            epoch: 1,
            config_id: 1,
            config_digest: LogHash::ZERO,
            proposal: rhiza_quepaxa::Proposal::new(
                rhiza_quepaxa::ProposalPriority::from_u64(1),
                "node-2",
                1,
                rhiza_quepaxa::AcceptedValue {
                    command_hash: LogHash::ZERO,
                    prev_hash: LogHash::ZERO,
                    entry_hash: LogHash::ZERO,
                },
            ),
            // The wire frame is comfortably below MAX_HTTP_BODY_BYTES, but
            // decoding its Vec exceeds the fixed 4x MAX_HTTP_BODY_BYTES heap
            // budget due to RecorderSummary's in-memory footprint.
            summaries: vec![summary; 16_384],
        };
        let malformed = RequestFrame {
            version: WIRE_VERSION,
            request_id: 1,
            remaining_deadline_ms: 1_000,
            body: RecorderRequestBody::InstallDecisionProof {
                proof,
                members: vec!["node-1".into(), "node-2".into()],
            },
        };
        let malformed_bytes = bounded_postcard_encode(&malformed, MAX_HTTP_BODY_BYTES).unwrap();
        assert!(malformed_bytes.len() < MAX_HTTP_BODY_BYTES);
        assert!(decode_framed::<RequestFrame>(&malformed_bytes).is_err());

        let (mut client, server_stream) = tokio::io::duplex(512 * 1024);
        let server = tokio::spawn(serve_connection_with_decode_slots(
            server_stream,
            CountingMutation {
                calls: Arc::clone(&calls),
            },
            RecorderConnectionContext {
                peers: peers().into(),
                recovery_generation: 7,
                slots: Arc::clone(&operation_slots),
                decode_slots: Arc::clone(&decode_slots),
                signals: None,
                response_write_test_server: None,
            },
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
        write_frame_async(&mut client, &malformed_bytes)
            .await
            .unwrap();
        assert!(server.await.unwrap().is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(decode_slots.available_permits(), 1);

        // A separate connection sharing the same decode gate must proceed:
        // malformed request decode never leaks a permit or consumes an
        // operation slot.
        let (mut fresh_client, fresh_server_stream) = tokio::io::duplex(16 * 1024);
        let fresh_server = tokio::spawn(serve_connection_with_decode_slots(
            fresh_server_stream,
            CountingMutation {
                calls: Arc::clone(&calls),
            },
            RecorderConnectionContext {
                peers: peers().into(),
                recovery_generation: 7,
                slots: operation_slots,
                decode_slots: Arc::clone(&decode_slots),
                signals: None,
                response_write_test_server: None,
            },
        ));
        write_value_async(
            &mut fresh_client,
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
            decode_exact::<HelloReply>(&read_frame_async(&mut fresh_client).await.unwrap())
                .unwrap(),
            HelloReply::Accepted { .. }
        ));
        write_value_async(
            &mut fresh_client,
            &RequestFrame {
                version: WIRE_VERSION,
                request_id: 2,
                remaining_deadline_ms: 1_000,
                body: RecorderRequestBody::Identity,
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            decode_exact::<ResponseFrame>(&read_frame_async(&mut fresh_client).await.unwrap())
                .unwrap()
                .body,
            RecorderResponseBody::Identity(RpcResult::Ok(recorder_id)) if recorder_id == "node-1"
        ));
        drop(fresh_client);
        assert!(fresh_server.await.unwrap().is_ok());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(decode_slots.available_permits(), 1);
    }

    #[tokio::test]
    async fn actual_listener_rejects_oversized_proof_before_backend_and_recovers_slot() {
        let _test_lock = request_decode_test_lock().lock().await;
        let calls = Arc::new(AtomicUsize::new(0));
        let summary = rhiza_quepaxa::RecorderSummary {
            recorder_id: "r".into(),
            slot: 1,
            step: 1,
            first_current: None,
            aggregate_prior: None,
        };
        let request = RequestFrame {
            version: WIRE_VERSION,
            request_id: 90_001,
            remaining_deadline_ms: 1_000,
            body: RecorderRequestBody::InstallDecisionProof {
                proof: DecisionProof::FastPath {
                    cluster_id: "rhiza:sql:cluster-a".into(),
                    slot: 1,
                    epoch: 1,
                    config_id: 1,
                    config_digest: LogHash::ZERO,
                    proposal: rhiza_quepaxa::Proposal::new(
                        rhiza_quepaxa::ProposalPriority::from_u64(1),
                        "node-2",
                        1,
                        rhiza_quepaxa::AcceptedValue {
                            command_hash: LogHash::ZERO,
                            prev_hash: LogHash::ZERO,
                            entry_hash: LogHash::ZERO,
                        },
                    ),
                    summaries: vec![summary; 16_384],
                },
                members: vec!["node-1".into(), "node-2".into()],
            },
        };
        let bytes = bounded_postcard_encode(&request, MAX_HTTP_BODY_BYTES).unwrap();
        assert!(bytes.len() < MAX_HTTP_BODY_BYTES);
        assert!(decode_framed::<RequestFrame>(&bytes).is_err());
        let (events_tx, mut events) = tokio::sync::mpsc::unbounded_channel();
        let _hook = install_request_decode_test_hook(RequestDecodeTestHook {
            request_ids: Arc::new([90_001].into_iter().collect()),
            events: events_tx,
            release: None,
        });
        let (address, shutdown, server) = start_actual_recorder_server(CountingMutation {
            calls: Arc::clone(&calls),
        })
        .await;
        let mut client = authenticated_tcp_hello(address).await;
        write_frame_async(&mut client, &bytes).await.unwrap();
        assert_eq!(
            next_decode_event(&mut events).await,
            RequestDecodeTestEvent::Entered
        );
        assert_eq!(
            next_decode_event(&mut events).await,
            RequestDecodeTestEvent::Failed("recorder decode heap budget exceeded".into())
        );
        assert!(
            tokio::time::timeout(Duration::from_secs(1), read_frame_async(&mut client))
                .await
                .unwrap()
                .is_err()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        let mut fresh = authenticated_tcp_hello(address).await;
        write_value_async(
            &mut fresh,
            &RequestFrame {
                version: WIRE_VERSION,
                request_id: 90_002,
                remaining_deadline_ms: 1_000,
                body: RecorderRequestBody::Identity,
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            decode_exact::<ResponseFrame>(
                &tokio::time::timeout(Duration::from_secs(1), read_frame_async(&mut fresh))
                    .await
                    .unwrap()
                    .unwrap(),
            )
            .unwrap()
            .body,
            RecorderResponseBody::Identity(RpcResult::Ok(recorder_id)) if recorder_id == "node-1"
        ));
        drop(client);
        drop(fresh);
        shutdown.shutdown.send_replace(true);
        let exit = tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
        assert!(exit.result.is_ok());
        assert_eq!(exit.tasks, RecorderTaskDisposition::Quiesced);
    }

    #[tokio::test]
    async fn actual_listener_limits_shared_request_decode_to_32() {
        let _test_lock = request_decode_test_lock().lock().await;
        let calls = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(tokio::sync::Notify::new());
        let (events_tx, mut events) = tokio::sync::mpsc::unbounded_channel();
        let _hook = install_request_decode_test_hook(RequestDecodeTestHook {
            request_ids: Arc::new((91_000..91_033).collect()),
            events: events_tx,
            release: Some(Arc::clone(&release)),
        });
        let (address, shutdown, server) = start_actual_recorder_server(CountingIdentity {
            calls: Arc::clone(&calls),
        })
        .await;
        let mut clients = Vec::new();
        for _ in 0..33 {
            clients.push(authenticated_tcp_hello(address).await);
        }
        // HELLO authentication/identity is deliberately outside the scoped
        // request-decode test gate; measure only request backend dispatch.
        calls.store(0, Ordering::SeqCst);
        for (offset, client) in clients.iter_mut().enumerate() {
            write_value_async(
                client,
                &RequestFrame {
                    version: WIRE_VERSION,
                    request_id: 91_000 + u64::try_from(offset).unwrap(),
                    // Gate orchestration, rather than the request deadline,
                    // controls this concurrency proof under a loaded suite.
                    remaining_deadline_ms: 30_000,
                    body: RecorderRequestBody::Identity,
                },
            )
            .await
            .unwrap();
        }
        for _ in 0..MAX_SERVER_DECODE_CONCURRENCY {
            assert_eq!(
                next_decode_event(&mut events).await,
                RequestDecodeTestEvent::Entered
            );
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(100), events.recv())
                .await
                .is_err()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        release.notify_waiters();
        assert_eq!(
            next_decode_event(&mut events).await,
            RequestDecodeTestEvent::Entered
        );
        release.notify_waiters();
        for client in &mut clients {
            let response: ResponseFrame = decode_exact(
                &tokio::time::timeout(Duration::from_secs(1), read_frame_async(client))
                    .await
                    .unwrap()
                    .unwrap(),
            )
            .unwrap();
            assert!(matches!(
                response.body,
                RecorderResponseBody::Identity(RpcResult::Ok(_))
                    | RecorderResponseBody::Identity(RpcResult::Overloaded)
            ));
        }
        drop(clients);
        shutdown.shutdown.send_replace(true);
        let exit = tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
        assert!(exit.result.is_ok());
        assert_eq!(exit.tasks, RecorderTaskDisposition::Quiesced);
        assert!(
            (MAX_SERVER_DECODE_CONCURRENCY..=MAX_SERVER_DECODE_CONCURRENCY + 1)
                .contains(&calls.load(Ordering::SeqCst)),
            "all requests decoded; operation admission may overload one while the shared gate drains"
        );
    }

    #[test]
    fn client_maps_malicious_framed_response_to_decode_or_sent_mutation_unknown() {
        let summary = rhiza_quepaxa::RecorderSummary {
            recorder_id: "r".into(),
            slot: 1,
            step: 1,
            first_current: None,
            aggregate_prior: None,
        };
        let oversized_proof = DecisionProof::FastPath {
            cluster_id: "rhiza:sql:cluster-a".into(),
            slot: 1,
            epoch: 1,
            config_id: 1,
            config_digest: LogHash::ZERO,
            proposal: rhiza_quepaxa::Proposal::new(
                rhiza_quepaxa::ProposalPriority::from_u64(1),
                "node-2",
                1,
                rhiza_quepaxa::AcceptedValue {
                    command_hash: LogHash::ZERO,
                    prev_hash: LogHash::ZERO,
                    entry_hash: LogHash::ZERO,
                },
            ),
            summaries: vec![summary; 16_384],
        };
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (observed_tx, observed_rx) = mpsc::sync_channel(2);
        let server = thread::spawn(move || {
            for expected_mutation in [false, true] {
                let (mut stream, _) = listener.accept().unwrap();
                let _: Hello = decode_exact(&read_frame_sync(&mut stream).unwrap()).unwrap();
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
                observed_tx
                    .send(matches!(&request.body, RecorderRequestBody::Record(_)))
                    .unwrap();
                assert_eq!(
                    matches!(&request.body, RecorderRequestBody::Record(_)),
                    expected_mutation
                );
                let response = ResponseFrame {
                    version: WIRE_VERSION,
                    request_id: request.request_id,
                    // Deliberately a valid but operation-mismatched envelope:
                    // bounded decode must fail before operation matching.
                    body: RecorderResponseBody::InspectDecisionProof(RpcResult::Ok(Some(
                        oversized_proof.clone(),
                    ))),
                };
                let bytes = bounded_postcard_encode(&response, MAX_HTTP_BODY_BYTES).unwrap();
                assert!(bytes.len() < MAX_HTTP_BODY_BYTES);
                assert!(decode_framed::<ResponseFrame>(&bytes).is_err());
                stream.write_all(&frame_length(&bytes).unwrap()).unwrap();
                stream.write_all(&bytes).unwrap();
                stream.flush().unwrap();
            }
        });
        let client = TcpPostcardRecorderClient::new_with_transport_and_timeout(
            address,
            "node-1",
            "node-2",
            "peer-token-2",
            7,
            ClientTransport::Plain,
            Duration::from_secs(2),
        )
        .unwrap();
        let context = rhiza_quepaxa::RecorderRpcContext::with_timeout(Duration::from_secs(2));
        assert!(matches!(
            client.recorder_id(&context),
            Err(Error::Decode(message)) if message == "recorder decode heap budget exceeded"
        ));
        assert!(!observed_rx.recv_timeout(Duration::from_secs(1)).unwrap());
        assert!(matches!(
            client.record(
                &context,
                RecordRequest {
                    cluster_id: "rhiza:sql:cluster-a".into(),
                    epoch: 1,
                    config_id: 1,
                    config_digest: LogHash::ZERO,
                    slot: 1,
                    step: 1,
                    proposal: rhiza_quepaxa::Proposal::nil(),
                    command: None,
                },
            ),
            Err(Error::UnknownOutcome)
        ));
        assert!(observed_rx.recv_timeout(Duration::from_secs(1)).unwrap());
        server.join().unwrap();
    }

    #[test]
    fn framed_oversized_mutation_is_definite_before_socket_checkout() {
        let client =
            TcpPostcardRecorderClient::new("127.0.0.1:9", "node-1", "node-2", "peer-token-2", 7)
                .unwrap();
        let command = StoredCommand::new(
            rhiza_core::EntryType::Command,
            vec![0_u8; MAX_HTTP_BODY_BYTES],
        );
        let result = client.exchange(
            &rhiza_quepaxa::RecorderRpcContext::with_timeout(Duration::from_secs(5)),
            RecorderRequestBody::StoreCommand {
                cluster_id: "rhiza:sql:cluster-a".into(),
                epoch: 1,
                config_id: 1,
                config_digest: LogHash::ZERO,
                command_hash: command.hash(),
                command,
            },
            true,
            true,
        );
        assert!(matches!(result, Err(Error::Decode(_))));
        assert_eq!(client.consensus.state.lock().unwrap().open, 0);
        assert!(client.consensus.state.lock().unwrap().idle.is_empty());
    }

    #[tokio::test]
    async fn framed_oversized_backend_response_is_a_typed_bounded_error() {
        let calls = Arc::new(AtomicUsize::new(0));
        let command = Arc::new(StoredCommand::new(
            rhiza_core::EntryType::Command,
            vec![0_u8; MAX_HTTP_BODY_BYTES],
        ));
        let slots = Arc::new(tokio::sync::Semaphore::new(1));
        let (mut client, server_stream) = tokio::io::duplex(16 * 1024);
        let server = tokio::spawn(serve_connection(
            server_stream,
            OversizedFetch {
                command,
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
        write_value_async(
            &mut client,
            &RequestFrame {
                version: WIRE_VERSION,
                request_id: 9,
                remaining_deadline_ms: 1_000,
                body: RecorderRequestBody::FetchCommand {
                    cluster_id: "rhiza:sql:cluster-a".into(),
                    epoch: 1,
                    config_id: 1,
                    config_digest: LogHash::ZERO,
                    command_hash: LogHash::ZERO,
                },
            },
        )
        .await
        .unwrap();
        let response: ResponseFrame =
            decode_exact(&read_frame_async(&mut client).await.unwrap()).unwrap();
        assert_eq!(response.request_id, 9);
        assert!(matches!(
            response.body,
            RecorderResponseBody::FetchCommand(RpcResult::Error(error))
                if matches!(error.code, crate::RecorderWireErrorCode::Decode)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        drop(client);
        assert!(server.await.unwrap().is_ok());
    }

    #[tokio::test]
    async fn request_expired_before_dispatch_never_reaches_recorder() {
        let calls = Arc::new(AtomicUsize::new(0));
        let command = StoredCommand::new(rhiza_core::EntryType::Command, b"expired".to_vec());
        let permit = Arc::new(
            Arc::new(tokio::sync::Semaphore::new(1))
                .acquire_owned()
                .await
                .unwrap(),
        );

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
            RecorderResponseBody::StoreCommand(RpcResult::Error(error))
                if matches!(error.code, crate::RecorderWireErrorCode::RpcDeadlineExceeded)
        ));
    }

    #[tokio::test]
    async fn saturated_server_rejects_hello_without_calling_recorder() {
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
            HelloReply::Rejected
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        drop(client);
        drop(held);
        assert!(server.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn saturated_server_returns_operation_overload_after_hello() {
        let calls = Arc::new(AtomicUsize::new(0));
        let slots = Arc::new(tokio::sync::Semaphore::new(1));
        let (mut client, server_stream) = tokio::io::duplex(4096);
        let server = tokio::spawn(serve_connection(
            server_stream,
            CountingMutation {
                calls: Arc::clone(&calls),
            },
            peers().into(),
            7,
            Arc::clone(&slots),
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
        let held = slots.acquire_owned().await.unwrap();
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
    async fn hello_identity_shutdown_waits_for_the_blocking_backend() {
        let (started_tx, started_rx) = mpsc::channel();
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let completed = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (ingress, lifecycle) = test_ingress_lifecycle();
        let TestIngressControl {
            shutdown,
            force: _force,
            started: ingress_started,
            listener_dropped,
        } = ingress;
        let mut server = tokio::spawn(serve_recorder_tcp(
            listener,
            BlockingIdentity {
                started: started_tx,
                release: Arc::clone(&release),
                completed: Arc::clone(&completed),
            },
            peers(),
            7,
            lifecycle,
        ));
        ingress_started.await.unwrap();
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
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        shutdown.send_replace(true);
        listener_dropped.await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut server)
                .await
                .is_err(),
            "shutdown drained before the admitted HELLO identity backend completed"
        );
        let (released, ready) = &*release;
        *released.lock().unwrap() = true;
        ready.notify_all();
        let exit = tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("server did not drain after identity release")
            .unwrap();
        assert!(exit.result.is_ok());
        assert_eq!(exit.tasks, RecorderTaskDisposition::Quiesced);
        assert_eq!(completed.load(Ordering::SeqCst), 1);
        drop(stream);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn server_deadline_returns_while_admitted_mutation_finishes_and_shutdown_drains_it() {
        let (started_tx, started_rx) = mpsc::channel();
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let completed = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (ingress, lifecycle) = test_ingress_lifecycle();
        let TestIngressControl {
            shutdown,
            force: _force,
            started: ingress_started,
            listener_dropped,
        } = ingress;
        let server = tokio::spawn(serve_recorder_tcp(
            listener,
            BlockingMutation {
                started: started_tx,
                release: Arc::clone(&release),
                completed: Arc::clone(&completed),
            },
            peers(),
            7,
            lifecycle,
        ));
        ingress_started.await.unwrap();
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
        shutdown.send_replace(true);
        listener_dropped.await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!server.is_finished());
        let (released, ready) = &*release;
        *released.lock().unwrap() = true;
        ready.notify_all();
        let exit = server.await.unwrap();
        assert!(exit.result.is_ok());
        assert_eq!(exit.tasks, RecorderTaskDisposition::Quiesced);
        assert_eq!(completed.load(Ordering::SeqCst), 1);
        let response = response
            .expect("server must answer the advertised deadline")
            .unwrap();
        assert!(matches!(
            decode_exact::<ResponseFrame>(&response).unwrap().body,
            RecorderResponseBody::StoreCommand(RpcResult::Error(error))
                if matches!(error.code, crate::RecorderWireErrorCode::RpcDeadlineExceeded)
        ));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn slow_reader_holds_framed_admission_permits_until_connection_close() {
        let payload = Arc::new(StoredCommand::new(
            rhiza_core::EntryType::Command,
            b"gated response".to_vec(),
        ));
        let calls = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let request_ids = (1..=u64::try_from(DEFAULT_PEER_CONCURRENCY).unwrap())
            .collect::<std::collections::BTreeSet<_>>();
        let (entered_tx, mut entered_rx) = tokio::sync::mpsc::unbounded_channel();
        let response_gate =
            install_response_write_test_gate(address, request_ids.clone(), entered_tx);
        let (_control, lifecycle) = test_ingress_lifecycle();
        let server = tokio::spawn(serve_recorder_tcp(
            listener,
            OversizedFetch {
                command: Arc::clone(&payload),
                calls: Arc::clone(&calls),
            },
            peers(),
            7,
            lifecycle,
        ));
        let mut slow_readers = Vec::new();
        for request_id in request_ids.iter().copied() {
            let mut stream = authenticated_tcp_hello(address).await;
            write_value_async(
                &mut stream,
                &RequestFrame {
                    version: WIRE_VERSION,
                    request_id,
                    remaining_deadline_ms: u32::try_from(CALL_TIMEOUT.as_millis()).unwrap(),
                    body: RecorderRequestBody::FetchCommand {
                        cluster_id: "rhiza:sql:cluster-a".into(),
                        epoch: 1,
                        config_id: 1,
                        config_digest: LogHash::ZERO,
                        command_hash: payload.hash(),
                    },
                },
            )
            .await
            .unwrap();
            slow_readers.push(stream);
        }
        let mut entered = std::collections::BTreeSet::new();
        for _ in 0..DEFAULT_PEER_CONCURRENCY {
            let request_id = tokio::time::timeout(Duration::from_secs(2), entered_rx.recv())
                .await
                .expect("admitted recorder response did not enter the write gate")
                .expect("response write gate closed before all admitted writes entered");
            assert!(
                entered.insert(request_id),
                "response write gate observed request {request_id} twice"
            );
        }
        assert_eq!(entered, request_ids);
        assert_eq!(calls.load(Ordering::SeqCst), DEFAULT_PEER_CONCURRENCY);

        // Every backend has returned, but each admitted response write is
        // causally paused while retaining its operation permit.
        // A new HELLO must therefore be rejected instead of borrowing a slot
        // released before its predecessor's response completed.
        let saturated =
            TcpPostcardRecorderClient::new(address, "node-1", "node-2", "peer-token-2", 7).unwrap();
        assert!(tokio::task::spawn_blocking(move || {
            saturated.recorder_id(&rhiza_quepaxa::RecorderRpcContext::default_timeout())
        })
        .await
        .unwrap()
        .is_err());
        assert_eq!(calls.load(Ordering::SeqCst), DEFAULT_PEER_CONCURRENCY);

        drop(slow_readers);
        response_gate.release();
        let recovered =
            TcpPostcardRecorderClient::new(address, "node-1", "node-2", "peer-token-2", 7).unwrap();
        let recovered = tokio::task::spawn_blocking(move || {
            let deadline = Instant::now() + Duration::from_secs(2);
            loop {
                match recovered.recorder_id(&rhiza_quepaxa::RecorderRpcContext::default_timeout()) {
                    Ok(id) => return id,
                    Err(_) if Instant::now() < deadline => thread::yield_now(),
                    Err(error) => panic!("framed permits were not recovered: {error}"),
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(recovered, "node-1");
        server.abort();
    }

    #[test]
    fn postcard_decoder_rejects_trailing_bytes_and_wrong_hello_version() {
        assert_eq!(WIRE_VERSION, 5);
        assert_eq!(RECORDER_TLS_ALPN, b"rhiza-recorder/5");
        let hello = Hello {
            version: WIRE_VERSION,
            node_id: "node-1".into(),
            recovery_generation: 7,
            token: "peer-token-1".into(),
        };
        let mut encoded = postcard::to_allocvec(&hello).unwrap();
        encoded.push(0);
        assert!(decode_exact::<Hello>(&encoded).is_err());

        for version in [WIRE_VERSION - 1, WIRE_VERSION + 1] {
            let wrong_version = Hello {
                version,
                node_id: "node-1".into(),
                recovery_generation: 7,
                token: "peer-token-1".into(),
            };
            assert!(!hello_authenticated(&wrong_version, &[], 7));
        }
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
