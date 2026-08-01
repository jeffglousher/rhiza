use std::{
    fmt,
    future::Future,
    sync::{
        atomic::{AtomicU32, Ordering},
        mpsc, Arc,
    },
    thread,
    time::{Duration, Instant},
};

#[cfg(test)]
use std::sync::atomic::AtomicUsize;

use postcard_rpc::{
    endpoint,
    header::{VarHeader, VarKey, VarSeq, VarSeqKind},
    host_client::{HostClient, HostErr, RpcFrame, WireRx, WireSpawn, WireTx},
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
    await_unless_forced, bounded_postcard_size, frame_length, read_frame_async,
    read_frame_with_signals, response_matches, response_operation, run_recorder_ingress,
    write_value_async_with_timeout, Hello, HelloReply, Operation, RecorderConnectionSignals,
    RecorderIngressExit, RecorderIngressLifecycle, RecorderRequestBody, RecorderResponseBody,
    RecorderTlsClientConfig, RecorderTlsServerConfig, CALL_TIMEOUT, CONNECT_TIMEOUT,
    MAX_SERVER_CONNECTIONS, MAX_SERVER_DECODE_CONCURRENCY, WIRE_VERSION,
};
use crate::{
    preserve_mutation_outcome,
    recorder_decode::{decode_postcard_exact_bounded, RecorderDecodeLimits},
    validate_recorder_tcp_endpoint, PeerConfig, DEFAULT_PEER_CONCURRENCY, MAX_HTTP_BODY_BYTES,
    QUORUM_RECORD_REQUEST_TIMEOUT, READ_FENCE_REQUEST_TIMEOUT,
};

#[cfg(test)]
use super::decode_exact;

const POSTCARD_RPC_WIRE_VERSION: u16 = WIRE_VERSION + 1;
const POSTCARD_RPC_TLS_ALPN: &[u8] = b"rhiza-recorder-prpc/5";
const LANE_IN_FLIGHT: usize = 8;
const BRIDGE_DEPTH: usize = 128;
const VAR_HEADER_MAX_BYTES: usize = 13;
const POSTCARD_U32_MAX_BYTES: usize = 5;
const POSTCARD_USIZE_MAX_BYTES: usize = 10;
const OPAQUE_BODY_LIMIT: usize =
    MAX_HTTP_BODY_BYTES - VAR_HEADER_MAX_BYTES - POSTCARD_U32_MAX_BYTES - POSTCARD_USIZE_MAX_BYTES;

type OpaqueRequest = (u32, Vec<u8>);
type OpaqueResponse = Vec<u8>;

#[cfg(test)]
struct TestPermitLifecycleHook {
    sequence: u32,
    response_attempted: mpsc::Sender<()>,
    backend_permit_dropped: mpsc::Sender<()>,
}

#[cfg(test)]
static TEST_PERMIT_LIFECYCLE_HOOK: std::sync::Mutex<Option<TestPermitLifecycleHook>> =
    std::sync::Mutex::new(None);

#[cfg(test)]
struct TestDispatchDeadlineGate {
    sequence: u32,
    entered: tokio::sync::mpsc::UnboundedSender<Instant>,
    release: Arc<tokio::sync::Notify>,
}

#[cfg(test)]
static TEST_DISPATCH_DEADLINE_GATE: std::sync::Mutex<Option<TestDispatchDeadlineGate>> =
    std::sync::Mutex::new(None);

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
enum TestInnerDecodeEvent {
    Entered,
    Failed(String),
}

#[cfg(test)]
#[derive(Clone)]
struct TestInnerDecodeHook {
    sequences: Arc<std::collections::BTreeSet<u32>>,
    events: tokio::sync::mpsc::UnboundedSender<TestInnerDecodeEvent>,
    release: Option<Arc<tokio::sync::Notify>>,
}

#[cfg(test)]
static TEST_INNER_DECODE_HOOK: std::sync::OnceLock<
    std::sync::Mutex<Option<Arc<TestInnerDecodeHook>>>,
> = std::sync::OnceLock::new();

#[cfg(test)]
static TEST_INNER_DECODE_LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> =
    std::sync::OnceLock::new();

#[cfg(test)]
static TEST_GLOBAL_HOOK_LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> =
    std::sync::OnceLock::new();

#[cfg(test)]
static TEST_CONNECTION_LIMIT: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
struct TestConnectionLimitGuard(usize);

#[cfg(test)]
impl Drop for TestConnectionLimitGuard {
    fn drop(&mut self) {
        TEST_CONNECTION_LIMIT.store(self.0, Ordering::SeqCst);
    }
}

#[cfg(test)]
fn override_test_connection_limit(limit: usize) -> TestConnectionLimitGuard {
    TestConnectionLimitGuard(TEST_CONNECTION_LIMIT.swap(limit, Ordering::SeqCst))
}

#[cfg(test)]
fn test_inner_decode_hook_slot() -> &'static std::sync::Mutex<Option<Arc<TestInnerDecodeHook>>> {
    TEST_INNER_DECODE_HOOK.get_or_init(|| std::sync::Mutex::new(None))
}

#[cfg(test)]
fn test_inner_decode_lock() -> &'static tokio::sync::Mutex<()> {
    TEST_INNER_DECODE_LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

#[cfg(test)]
fn test_global_hook_lock() -> &'static tokio::sync::Mutex<()> {
    TEST_GLOBAL_HOOK_LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

#[cfg(test)]
struct TestInnerDecodeHookGuard;

#[cfg(test)]
impl Drop for TestInnerDecodeHookGuard {
    fn drop(&mut self) {
        *test_inner_decode_hook_slot().lock().unwrap() = None;
    }
}

#[cfg(test)]
fn install_test_inner_decode_hook(hook: TestInnerDecodeHook) -> TestInnerDecodeHookGuard {
    let mut slot = test_inner_decode_hook_slot().lock().unwrap();
    assert!(
        slot.is_none(),
        "postcard inner decode hook already installed"
    );
    *slot = Some(Arc::new(hook));
    TestInnerDecodeHookGuard
}

#[cfg(test)]
async fn test_inner_decode_hook_after_permit(sequence: VarSeq) -> Option<Arc<TestInnerDecodeHook>> {
    let VarSeq::Seq4(sequence) = sequence else {
        return None;
    };
    let hook = test_inner_decode_hook_slot().lock().unwrap().clone()?;
    if !hook.sequences.contains(&sequence) {
        return None;
    }
    let _ = hook.events.send(TestInnerDecodeEvent::Entered);
    if let Some(release) = &hook.release {
        release.notified().await;
    }
    Some(hook)
}

#[cfg(test)]
async fn wait_for_test_dispatch_deadline_gate(sequence: VarSeq, deadline: Instant) {
    let VarSeq::Seq4(sequence) = sequence else {
        return;
    };
    let gate = TEST_DISPATCH_DEADLINE_GATE.lock().ok().and_then(|gate| {
        gate.as_ref().and_then(|gate| {
            (gate.sequence == sequence).then(|| (gate.entered.clone(), Arc::clone(&gate.release)))
        })
    });
    let Some((entered, release)) = gate else {
        return;
    };
    let _ = entered.send(deadline);
    release.notified().await;
}

#[cfg(test)]
fn notify_test_timeout_response_attempt(sequence: VarSeq, body: &RecorderResponseBody) {
    let VarSeq::Seq4(sequence) = sequence else {
        return;
    };
    let _ = body;
    if let Ok(hook) = TEST_PERMIT_LIFECYCLE_HOOK.lock() {
        if hook.as_ref().is_some_and(|hook| hook.sequence == sequence) {
            let _ = hook
                .as_ref()
                .expect("checked test hook")
                .response_attempted
                .send(());
        }
    }
}

#[cfg(test)]
fn notify_test_backend_permit_dropped(sequence: VarSeq) {
    let VarSeq::Seq4(sequence) = sequence else {
        return;
    };
    if let Ok(hook) = TEST_PERMIT_LIFECYCLE_HOOK.lock() {
        if hook.as_ref().is_some_and(|hook| hook.sequence == sequence) {
            let _ = hook
                .as_ref()
                .expect("checked test hook")
                .backend_permit_dropped
                .send(());
        }
    }
}

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

pub async fn serve_recorder_postcard_rpc<R>(
    listener: tokio::net::TcpListener,
    recorder: R,
    peers: Vec<PeerConfig>,
    recovery_generation: u64,
    lifecycle: RecorderIngressLifecycle,
) -> RecorderIngressExit
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    serve_recorder_postcard_rpc_inner(
        listener,
        recorder,
        peers,
        recovery_generation,
        None,
        lifecycle,
    )
    .await
}

pub async fn serve_recorder_postcard_rpc_tls<R>(
    listener: tokio::net::TcpListener,
    recorder: R,
    peers: Vec<PeerConfig>,
    recovery_generation: u64,
    tls: RecorderPostcardRpcTlsServerConfig,
    lifecycle: RecorderIngressLifecycle,
) -> RecorderIngressExit
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    serve_recorder_postcard_rpc_inner(
        listener,
        recorder,
        peers,
        recovery_generation,
        Some(tls.inner),
        lifecycle,
    )
    .await
}

async fn serve_recorder_postcard_rpc_inner<R>(
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
    let peers: Arc<[PeerConfig]> = peers.into();
    let slots = Arc::new(tokio::sync::Semaphore::new(DEFAULT_PEER_CONCURRENCY));
    let decode_slots = Arc::new(tokio::sync::Semaphore::new(MAX_SERVER_DECODE_CONCURRENCY));
    let connection_limit = {
        #[cfg(test)]
        {
            let override_limit = TEST_CONNECTION_LIMIT.load(Ordering::SeqCst);
            if override_limit != 0 {
                override_limit
            } else {
                MAX_SERVER_CONNECTIONS
            }
        }
        #[cfg(not(test))]
        {
            MAX_SERVER_CONNECTIONS
        }
    };
    run_recorder_ingress(
        listener,
        lifecycle,
        Arc::clone(&slots),
        DEFAULT_PEER_CONCURRENCY,
        connection_limit,
        "recorder postcard-rpc TCP accept failed",
        move |stream, peer_address, shutdown, force, connection| {
            let recorder = recorder.clone();
            let peers = Arc::clone(&peers);
            let slots = Arc::clone(&slots);
            let decode_slots = Arc::clone(&decode_slots);
            let tls = tls.clone();
            async move {
                let _connection = connection;
                let signals = RecorderConnectionSignals { shutdown, force };
                let result = if let Some(config) = tls {
                    serve_postcard_rpc_tls_connection(
                        stream,
                        config,
                        recorder,
                        peers,
                        recovery_generation,
                        slots,
                        decode_slots,
                        signals,
                    )
                    .await
                } else {
                    serve_postcard_rpc_connection_with_decode_slots(
                        stream,
                        recorder,
                        peers,
                        recovery_generation,
                        slots,
                        decode_slots,
                        Some(signals),
                    )
                    .await
                };
                if let Err(error) = &result {
                    tracing::debug!(
                        peer = %peer_address,
                        %error,
                        "recorder postcard-rpc connection closed"
                    );
                }
                result
            }
        },
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn serve_postcard_rpc_tls_connection<R>(
    stream: tokio::net::TcpStream,
    config: Arc<rustls::ServerConfig>,
    recorder: R,
    peers: Arc<[PeerConfig]>,
    recovery_generation: u64,
    slots: Arc<tokio::sync::Semaphore>,
    decode_slots: Arc<tokio::sync::Semaphore>,
    mut signals: RecorderConnectionSignals,
) -> Result<(), String>
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    let acceptor = tokio_rustls::TlsAcceptor::from(config);
    let handshake = tokio::time::timeout(CONNECT_TIMEOUT, acceptor.accept(stream));
    tokio::pin!(handshake);
    let tls_stream = tokio::select! {
        biased;
        () = super::wait_for_ingress_signal(&mut signals.force) => return Ok(()),
        () = super::wait_for_ingress_signal(&mut signals.shutdown) => return Ok(()),
        handshake = &mut handshake => match handshake {
            Ok(Ok(tls_stream)) => tls_stream,
            Ok(Err(_)) => return Err("recorder postcard-rpc TLS handshake failed".to_string()),
            Err(_) => return Err("recorder postcard-rpc TLS handshake timed out".to_string()),
        },
    };
    if tls_stream.get_ref().1.alpn_protocol() != Some(POSTCARD_RPC_TLS_ALPN) {
        return Err("recorder postcard-rpc TLS ALPN negotiation failed".to_string());
    }
    serve_postcard_rpc_connection_with_decode_slots(
        tls_stream,
        recorder,
        peers,
        recovery_generation,
        slots,
        decode_slots,
        Some(signals),
    )
    .await
}

#[cfg(test)]
async fn serve_postcard_rpc_connection<R, S>(
    stream: S,
    recorder: R,
    peers: Arc<[PeerConfig]>,
    recovery_generation: u64,
    slots: Arc<tokio::sync::Semaphore>,
) -> Result<(), String>
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    serve_postcard_rpc_connection_with_decode_slots(
        stream,
        recorder,
        peers,
        recovery_generation,
        slots,
        Arc::new(tokio::sync::Semaphore::new(MAX_SERVER_DECODE_CONCURRENCY)),
        None,
    )
    .await
}

async fn serve_postcard_rpc_connection_with_decode_slots<R, S>(
    mut stream: S,
    recorder: R,
    peers: Arc<[PeerConfig]>,
    recovery_generation: u64,
    slots: Arc<tokio::sync::Semaphore>,
    decode_slots: Arc<tokio::sync::Semaphore>,
    mut signals: Option<RecorderConnectionSignals>,
) -> Result<(), String>
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let hello_bytes = tokio::time::timeout(
        CALL_TIMEOUT,
        read_frame_with_signals(&mut stream, &mut signals),
    )
    .await
    .map_err(|_| "recorder postcard-rpc HELLO timed out".to_string())??;
    let Some(hello_bytes) = hello_bytes else {
        return Ok(());
    };
    let hello: Hello = decode_postcard_exact_bounded(
        &hello_bytes,
        RecorderDecodeLimits::for_wire_bytes(MAX_HTTP_BODY_BYTES),
    )
    .map_err(|error| error.to_string())?;
    if hello.version != POSTCARD_RPC_WIRE_VERSION
        || hello.recovery_generation != recovery_generation
        || !crate::peer_credentials_authenticated(&hello.node_id, &hello.token, &peers)
    {
        let rejection = write_value_async_with_timeout(
            &mut stream,
            &HelloReply::Rejected,
            "recorder postcard-rpc HELLO rejection",
        );
        let _ = await_unless_forced(&mut signals, rejection).await;
        return Err("recorder postcard-rpc HELLO rejected".into());
    }
    let permit = match slots.clone().try_acquire_owned() {
        Ok(permit) => Arc::new(permit),
        Err(_) => {
            let rejection = write_value_async_with_timeout(
                &mut stream,
                &HelloReply::Rejected,
                "recorder postcard-rpc HELLO overload rejection",
            );
            let _ = await_unless_forced(&mut signals, rejection).await;
            return Err("recorder postcard-rpc HELLO overloaded".into());
        }
    };
    let identity_recorder = recorder.clone();
    // The connection keeps a response-side reference through the accepted
    // HELLO write. The blocking side owns its own reference if force or a
    // deadline detaches identity execution.
    let backend_permit = Arc::clone(&permit);
    let identity = tokio::task::spawn_blocking(move || {
        let _permit = backend_permit;
        identity_recorder.recorder_id(&rhiza_quepaxa::RecorderRpcContext::with_timeout(
            CALL_TIMEOUT,
        ))
    });
    let Some(recorder_id) = await_unless_forced(&mut signals, identity).await else {
        return Ok(());
    };
    let recorder_id = recorder_id
        .map_err(|error| format!("recorder identity task failed: {error}"))?
        .map_err(|error| error.to_string())?;
    let hello_reply = HelloReply::Accepted {
        version: POSTCARD_RPC_WIRE_VERSION,
        recorder_id,
    };
    let hello_response = write_value_async_with_timeout(
        &mut stream,
        &hello_reply,
        "recorder postcard-rpc HELLO response",
    );
    let Some(hello_response) = await_unless_forced(&mut signals, hello_response).await else {
        return Ok(());
    };
    hello_response?;
    drop(permit);
    let authenticated_peer_id = hello.node_id;

    let (mut reader, writer) = tokio::io::split(stream);
    let writer = Arc::new(tokio::sync::Mutex::new(writer));
    let response_force = signals.as_ref().map(|signals| signals.force.clone());
    let mut calls = tokio::task::JoinSet::new();
    let mut abort_calls = false;
    let mut session_result = async {
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
            // Shutdown may cancel a partial frame only because this socket is
            // immediately dropped and never resumed. Response completion still
            // cannot cancel the read and desynchronize a live session.
            let bytes = match read_frame_with_signals(&mut reader, &mut signals).await {
                Ok(Some(bytes)) => bytes,
                Ok(None) => return Ok(()),
                Err(error) if error == "connection closed" => {
                    abort_calls = true;
                    return Ok(());
                }
                Err(error) => return Err(error),
            };
            let (header, payload) = VarHeader::take_from_slice(&bytes)
                .ok_or_else(|| "invalid recorder postcard-rpc header".to_string())?;
            if !matches!(header.seq_no, VarSeq::Seq4(_)) {
                return Err("recorder postcard-rpc requires Seq4".into());
            }
            let operation = operation_for_key(header.key)
                .ok_or_else(|| "unknown recorder postcard-rpc endpoint".to_string())?;
            let (deadline_ms, encoded_body) = decode_opaque_request(payload)?;
            if deadline_ms == 0 {
                return Err("invalid recorder postcard-rpc deadline".into());
            }
            // Decode the peer's relative budget into one local absolute deadline
            // before any permit acquisition or task scheduling. Queue/scheduler
            // delay must consume the same budget as backend execution.
            let request_seq = header.seq_no;
            let dispatch_deadline =
                Instant::now() + Duration::from_millis(u64::from(deadline_ms)).min(CALL_TIMEOUT);
            #[cfg(test)]
            wait_for_test_dispatch_deadline_gate(request_seq, dispatch_deadline).await;
            let body: RecorderRequestBody = {
                let _decode_permit = decode_slots
                    .acquire()
                    .await
                    .map_err(|_| "recorder postcard-rpc decode semaphore closed".to_string())?;
                #[cfg(test)]
                let test_hook = test_inner_decode_hook_after_permit(request_seq).await;
                match decode_postcard_exact_bounded(
                    encoded_body,
                    RecorderDecodeLimits::for_wire_bytes(OPAQUE_BODY_LIMIT),
                ) {
                    Ok(body) => body,
                    Err(error) => {
                        #[cfg(test)]
                        if let Some(hook) = test_hook {
                            let _ = hook
                                .events
                                .send(TestInnerDecodeEvent::Failed(error.to_string()));
                        }
                        send_response(
                            &writer,
                            response_force.clone(),
                            operation,
                            request_seq,
                            super::error_response(operation, Error::Decode(error.to_string())),
                        )
                        .await?;
                        continue;
                    }
                }
            };
            if response_operation(&body) != operation {
                return Err("recorder postcard-rpc endpoint payload mismatch".into());
            }
            let writer = Arc::clone(&writer);
            let call_force = response_force.clone();
            if dispatch_deadline <= Instant::now() {
                send_response(
                    &writer,
                    call_force,
                    operation,
                    request_seq,
                    super::error_response(operation, Error::RpcDeadlineExceeded),
                )
                .await?;
                continue;
            }
            let permit = match slots.clone().try_acquire_owned() {
                Ok(permit) => Arc::new(permit),
                Err(_) => {
                    send_response(
                        &writer,
                        call_force,
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
                // One reference stays with this response task until encoding and
                // the serialized socket write finish (or the connection aborts).
                // The blocking backend owns a second reference, so a detached
                // timeout keeps shutdown accounting until that backend exits.
                let response_permit = permit;
                let backend_permit = Arc::clone(&response_permit);
                let dispatched = tokio::task::spawn_blocking(move || {
                    let response = if dispatch_deadline <= Instant::now() {
                        super::error_response(operation, Error::RpcDeadlineExceeded)
                    } else {
                        let context =
                            rhiza_quepaxa::RecorderRpcContext::with_deadline(dispatch_deadline);
                        super::dispatch(
                            call_recorder,
                            body,
                            &context,
                            &call_authenticated_peer_id,
                            &call_peers,
                        )
                    };
                    // This is deliberately explicit: on a detached timeout the
                    // backend's clone is released only after dispatch returns.
                    drop(backend_permit);
                    #[cfg(test)]
                    notify_test_backend_permit_dropped(request_seq);
                    response
                });
                let response =
                    match tokio::time::timeout_at(dispatch_deadline.into(), dispatched).await {
                        Ok(Ok(response)) => {
                            send_response(
                                &writer,
                                call_force.clone(),
                                operation,
                                request_seq,
                                response,
                            )
                            .await
                        }
                        Ok(Err(_)) => {
                            send_response(
                                &writer,
                                call_force.clone(),
                                operation,
                                request_seq,
                                super::error_response(operation, operation.panic_error()),
                            )
                            .await
                        }
                        Err(_) => {
                            send_response(
                                &writer,
                                call_force,
                                operation,
                                request_seq,
                                super::error_response(operation, Error::RpcDeadlineExceeded),
                            )
                            .await
                        }
                    };
                // Keep the response-side Arc alive through the completed writer
                // operation. This explicit drop prevents last-use analysis from
                // releasing it immediately after cloning for the backend.
                drop(response_permit);
                response
            });
        }
    }
    .await;

    let mut forced = signals
        .as_ref()
        .is_some_and(|signals| *signals.force.borrow());
    if forced || abort_calls || session_result.is_err() {
        calls.abort_all();
    }
    while !calls.is_empty() {
        let completed = if forced {
            calls.join_next().await
        } else if let Some(signals) = signals.as_mut() {
            tokio::select! {
                biased;
                () = super::wait_for_ingress_signal(&mut signals.force) => {
                    forced = true;
                    calls.abort_all();
                    continue;
                }
                completed = calls.join_next() => completed,
            }
        } else {
            calls.join_next().await
        };
        match completed {
            Some(Ok(Ok(()))) => {}
            Some(Ok(Err(error))) if session_result.is_ok() => {
                session_result = Err(error);
                calls.abort_all();
            }
            Some(Err(error)) if session_result.is_ok() => {
                session_result = Err(format!(
                    "recorder postcard-rpc response task failed: {error}"
                ));
                calls.abort_all();
            }
            Some(_) => {}
            None => break,
        }
    }
    session_result
}

fn postcard_encoded_len<T: serde::Serialize>(value: &T) -> Result<usize, String> {
    let mut scratch = [0_u8; POSTCARD_USIZE_MAX_BYTES];
    postcard::to_slice(value, &mut scratch)
        .map(|encoded| encoded.len())
        .map_err(|error| error.to_string())
}

fn decode_opaque_request(payload: &[u8]) -> Result<(u32, &[u8]), String> {
    let (deadline_ms, payload) =
        postcard::take_from_bytes::<u32>(payload).map_err(|error| error.to_string())?;
    let (body_len, body) =
        postcard::take_from_bytes::<usize>(payload).map_err(|error| error.to_string())?;
    if body_len != body.len() {
        return Err("recorder postcard-rpc opaque request has trailing bytes".into());
    }
    if body.len() > OPAQUE_BODY_LIMIT {
        return Err("recorder postcard-rpc opaque request body exceeds limit".into());
    }
    Ok((deadline_ms, body))
}

fn decode_opaque_response(payload: &[u8]) -> Result<&[u8], String> {
    let (body_len, body) =
        postcard::take_from_bytes::<usize>(payload).map_err(|error| error.to_string())?;
    if body_len != body.len() {
        return Err("recorder postcard-rpc opaque response has trailing bytes".into());
    }
    if body.len() > OPAQUE_BODY_LIMIT {
        return Err("recorder postcard-rpc opaque response body exceeds limit".into());
    }
    Ok(body)
}

fn build_opaque_bytes<T: serde::Serialize>(
    header: Option<VarHeader>,
    deadline: Option<u32>,
    body: &T,
) -> Result<Vec<u8>, String> {
    let body_size = bounded_postcard_size(body, OPAQUE_BODY_LIMIT)?;
    let body_len_size = postcard_encoded_len(&body_size)?;
    let deadline_size = deadline
        .as_ref()
        .map(postcard_encoded_len)
        .transpose()?
        .unwrap_or(0);
    let mut header_buffer = [0_u8; VAR_HEADER_MAX_BYTES];
    let header_size = header
        .map(|header| {
            header
                .write_to_slice(&mut header_buffer)
                .map(|(used, _)| used.len())
                .ok_or_else(|| "recorder postcard-rpc header exceeds limit".to_string())
        })
        .transpose()?
        .unwrap_or(0);
    let total = header_size
        .checked_add(deadline_size)
        .and_then(|size| size.checked_add(body_len_size))
        .and_then(|size| size.checked_add(body_size))
        .ok_or_else(|| "recorder postcard-rpc frame exceeds limit".to_string())?;
    if total == 0 || total > MAX_HTTP_BODY_BYTES {
        return Err("recorder postcard-rpc frame exceeds limit".into());
    }
    // A request/response outer envelope contains a length-prefixed encoded
    // body. Compose all three parts directly into one exact frame rather than
    // retaining an inner Vec beside a second full payload frame.
    let mut frame = vec![0_u8; total];
    frame[..header_size].copy_from_slice(&header_buffer[..header_size]);
    let mut offset = header_size;
    if let Some(deadline) = deadline {
        offset += postcard::to_slice(&deadline, &mut frame[offset..])
            .map_err(|error| error.to_string())?
            .len();
    }
    offset += postcard::to_slice(&body_size, &mut frame[offset..])
        .map_err(|error| error.to_string())?
        .len();
    let written = postcard::to_slice(body, &mut frame[offset..])
        .map_err(|error| error.to_string())?
        .len();
    if offset + written != total {
        return Err("recorder postcard-rpc serialization size changed".into());
    }
    Ok(frame)
}

fn preflight_bridge_body(body: &RecorderRequestBody) -> Result<(), Error> {
    bounded_postcard_size(body, OPAQUE_BODY_LIMIT)
        .map(|_| ())
        .map_err(Error::Decode)
}

async fn send_response<W>(
    writer: &Arc<tokio::sync::Mutex<W>>,
    mut force: Option<tokio::sync::watch::Receiver<bool>>,
    operation: Operation,
    seq_no: VarSeq,
    body: RecorderResponseBody,
) -> Result<(), String>
where
    W: AsyncWrite + Unpin,
{
    #[cfg(test)]
    notify_test_timeout_response_attempt(seq_no, &body);
    let header = VarHeader {
        key: VarKey::Key8(response_key(operation)),
        seq_no,
    };
    let frame = match build_opaque_bytes(Some(header), None, &body) {
        Ok(frame) => frame,
        Err(_) => build_opaque_bytes(
            Some(header),
            None,
            &super::error_response(
                operation,
                Error::Decode("recorder postcard-rpc response exceeds frame limit".into()),
            ),
        )
        .map_err(|_| "recorder postcard-rpc response fallback exceeds frame limit".to_string())?,
    };
    let write = async {
        let mut writer = tokio::time::timeout(CALL_TIMEOUT, writer.lock())
            .await
            .map_err(|_| "recorder postcard-rpc writer lock timed out".to_string())?;
        tokio::time::timeout(CALL_TIMEOUT, write_raw_frame(&mut *writer, &frame))
            .await
            .map_err(|_| "recorder postcard-rpc response timed out".to_string())?
    };
    match force.as_mut() {
        Some(force) => tokio::select! {
            biased;
            // Cancelling the lock/write future releases a held mutex guard
            // immediately. The connection owner observes the same signal,
            // aborts/reaps its call JoinSet, and drops both socket halves.
            () = super::wait_for_ingress_signal(force) => Ok(()),
            result = write => result,
        },
        None => write.await,
    }
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
    sequence: u32,
    deadline: Instant,
    reply: mpsc::SyncSender<rhiza_quepaxa::Result<RecorderResponseBody>>,
}

struct CompletedCall {
    session_id: u64,
    result: rhiza_quepaxa::Result<RecorderResponseBody>,
    wire_failed: bool,
    reply: mpsc::SyncSender<rhiza_quepaxa::Result<RecorderResponseBody>>,
}

#[derive(Debug)]
enum EndpointError {
    Host(HostErr<WireError>),
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
    next_sequence: AtomicU32,
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
            next_sequence: AtomicU32::new(0),
        })
    }

    fn exchange(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
        body: RecorderRequestBody,
        consensus: bool,
        mutating: bool,
    ) -> rhiza_quepaxa::Result<RecorderResponseBody> {
        self.exchange_with_timeout(context, body, consensus, mutating, self.call_timeout)
    }

    fn exchange_with_timeout(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
        body: RecorderRequestBody,
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
        let operation = response_operation(&body);
        // This preflight occurs before the bridge queue. An oversized public
        // caller value is a definite local Decode error, including mutations.
        preflight_bridge_body(&body)?;
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
                sequence: self.next_sequence.fetch_add(1, Ordering::Relaxed),
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
        let response = loop {
            match context.check() {
                Ok(()) => {}
                Err(_) if mutating => return Err(Error::UnknownOutcome),
                Err(error) => return Err(error),
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(if mutating {
                    Error::UnknownOutcome
                } else {
                    Error::RpcDeadlineExceeded
                });
            }
            match receive.recv_timeout(remaining.min(Duration::from_millis(10))) {
                Ok(response) => break response,
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(if mutating {
                        Error::UnknownOutcome
                    } else {
                        Error::Io("recorder postcard-rpc worker closed".into())
                    });
                }
            }
        };
        let response = match response {
            Ok(response) => response,
            // Queue admission succeeded, so the lane may already have handed
            // the mutation to the remote recorder. Never collapse that into a
            // retryable local I/O error.
            Err(_) if mutating => return Err(Error::UnknownOutcome),
            Err(error) => return Err(error),
        };
        match context.check() {
            Ok(()) => Ok(response),
            Err(_) if mutating => Err(Error::UnknownOutcome),
            Err(error) => Err(error),
        }
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
                    let _ = request.reply.send(Err(Error::RpcDeadlineExceeded));
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
                            let _ = request.reply.send(Err(error));
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
    if request.deadline <= Instant::now() {
        return CompletedCall {
            session_id,
            result: Err(Error::RpcDeadlineExceeded),
            wire_failed: false,
            reply: request.reply,
        };
    }
    let remaining = request.deadline.saturating_duration_since(Instant::now());
    let frame = match build_opaque_bytes(
        None,
        Some(
            u32::try_from(remaining.as_millis())
                .unwrap_or(u32::MAX)
                .max(1),
        ),
        &request.body,
    ) {
        Ok(body) => RpcFrame {
            header: VarHeader {
                key: VarKey::Key8(request_key(request.operation)),
                seq_no: VarSeq::Seq4(request.sequence),
            },
            // `send_resp_raw` owns this header separately; `body` is only the
            // endpoint's opaque request payload.
            body,
        },
        Err(error) => {
            return CompletedCall {
                session_id,
                result: Err(Error::Decode(error)),
                wire_failed: false,
                reply: request.reply,
            };
        }
    };
    let future = send_endpoint(&client, request.operation, frame);
    let result = tokio::time::timeout_at(request.deadline.into(), future).await;
    let (result, wire_failed) = match result {
        Err(_) => (Err(Error::RpcDeadlineExceeded), true),
        Ok(Err(EndpointError::Host(HostErr::Postcard(error)))) => {
            (Err(Error::Decode(error.to_string())), false)
        }
        Ok(Err(EndpointError::Host(error))) => (Err(Error::Io(error.to_string())), true),
        Ok(Ok(response)) => match decode_opaque_response(&response.body).and_then(|body| {
            decode_postcard_exact_bounded(
                body,
                RecorderDecodeLimits::for_wire_bytes(OPAQUE_BODY_LIMIT),
            )
            .map_err(|error| error.to_string())
        }) {
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
    request: RpcFrame,
) -> Result<RpcFrame, EndpointError> {
    // postcard-rpc's HostClient makes its final header+body copy internally.
    // `preflight_bridge_body` reserved the largest possible VarHeader before
    // admission, so that copy remains bounded by MAX_HTTP_BODY_BYTES even
    // though the dependency owns the last transport allocation.
    client
        .send_resp_raw(request, response_key(operation))
        .await
        .map_err(EndpointError::Host)
}

async fn connect_session(
    config: &ConnectionConfig,
    deadline: Instant,
) -> Result<HostClient<WireError>, Error> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(Error::RpcDeadlineExceeded);
    }
    let addresses = tokio::time::timeout(
        remaining.min(CONNECT_TIMEOUT),
        tokio::net::lookup_host(&config.address),
    )
    .await
    .map_err(|_| {
        connect_timeout_error(
            deadline,
            "recorder postcard-rpc address resolution timed out",
        )
    })?
    .map_err(|error| Error::Io(format!("cannot resolve recorder TCP address: {error}")))?
    .collect::<Vec<_>>();
    if addresses.is_empty() {
        return Err(Error::Io(
            "recorder TCP address resolved to no endpoints".into(),
        ));
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
            Ok(Err(error)) => last_error = Some(Error::Io(error.to_string())),
            Err(_) => {
                last_error = Some(connect_timeout_error(
                    deadline,
                    "recorder postcard-rpc TCP connect timed out",
                ));
            }
        }
    }
    let socket = socket.ok_or_else(|| {
        if deadline <= Instant::now() {
            Error::RpcDeadlineExceeded
        } else {
            last_error.unwrap_or_else(|| Error::Io("recorder TCP connect failed".into()))
        }
    })?;
    socket
        .set_nodelay(true)
        .map_err(|error| Error::Io(format!("cannot set recorder TCP_NODELAY: {error}")))?;
    let mut stream: BoxedIo = match &config.transport {
        ClientTransport::Plain => Box::new(socket),
        ClientTransport::Tls(tls) => {
            let connector = tokio_rustls::TlsConnector::from(Arc::clone(&tls.inner));
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(Error::RpcDeadlineExceeded);
            }
            let stream = tokio::time::timeout(
                remaining,
                connector.connect(tls.server_name.clone(), socket),
            )
            .await
            .map_err(|_| {
                connect_timeout_error(deadline, "recorder postcard-rpc TLS handshake timed out")
            })?
            .map_err(|_| Error::Io("recorder postcard-rpc TLS handshake failed".into()))?;
            if stream.get_ref().1.alpn_protocol() != Some(POSTCARD_RPC_TLS_ALPN) {
                return Err(Error::Io(
                    "recorder postcard-rpc TLS ALPN negotiation failed".into(),
                ));
            }
            Box::new(stream)
        }
    };
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(Error::RpcDeadlineExceeded);
    }
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
    .map_err(|_| connect_timeout_error(deadline, "recorder postcard-rpc HELLO timed out"))?
    .map_err(Error::Io)?;
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(Error::RpcDeadlineExceeded);
    }
    let reply = tokio::time::timeout(remaining, read_frame_async(&mut stream))
        .await
        .map_err(|_| connect_timeout_error(deadline, "recorder postcard-rpc HELLO timed out"))?
        .map_err(Error::Io)?;
    let reply: HelloReply = decode_postcard_exact_bounded(
        &reply,
        RecorderDecodeLimits::for_wire_bytes(MAX_HTTP_BODY_BYTES),
    )
    .map_err(|error| Error::Decode(error.to_string()))?;
    match reply {
        HelloReply::Accepted {
            version,
            recorder_id,
        } if version == POSTCARD_RPC_WIRE_VERSION && recorder_id == config.expected_recorder_id => {
        }
        HelloReply::Accepted { .. } => {
            return Err(Error::Io("recorder postcard-rpc identity mismatch".into()));
        }
        HelloReply::Rejected => {
            return Err(Error::Io("recorder postcard-rpc HELLO rejected".into()))
        }
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

fn connect_timeout_error(deadline: Instant, message: &'static str) -> Error {
    if deadline <= Instant::now() {
        Error::RpcDeadlineExceeded
    } else {
        Error::Io(message.into())
    }
}

impl RecorderRpc for TcpPostcardRpcRecorderClient {
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
        match self.exchange(
            context,
            RecorderRequestBody::StoreCommand {
                cluster_id,
                epoch,
                config_id,
                config_digest,
                command_hash,
                command,
            },
            true,
            true,
        )? {
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
        match self.exchange(
            context,
            RecorderRequestBody::FetchCommand {
                cluster_id,
                epoch,
                config_id,
                config_digest,
                command_hash,
            },
            false,
            false,
        )? {
            RecorderResponseBody::FetchCommand(result) => result.into_result(),
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
        match self.exchange(
            context,
            RecorderRequestBody::InstallDecisionProof {
                proof,
                members: membership.members().to_vec(),
            },
            true,
            true,
        )? {
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
        match self.exchange(
            context,
            RecorderRequestBody::InspectDecisionProof { slot },
            false,
            false,
        )? {
            RecorderResponseBody::InspectDecisionProof(result) => result.into_result(),
            _ => Err(Error::Decode("recorder response operation mismatch".into())),
        }
    }

    fn inspect_record_summary(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
        slot: u64,
    ) -> rhiza_quepaxa::Result<Option<RecordSummary>> {
        match self.exchange(
            context,
            RecorderRequestBody::InspectRecordSummary { slot },
            false,
            false,
        )? {
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

#[cfg(test)]
mod tests {
    use super::super::RpcResult;
    use super::*;
    use socket2::Socket;
    use std::io::Write;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Condvar, Mutex,
    };

    #[derive(Clone)]
    struct SlowFirstInspection;

    impl RecorderRpc for SlowFirstInspection {
        fn recorder_id(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
        ) -> rhiza_quepaxa::Result<String> {
            Ok("node-1".into())
        }

        fn inspect_record_summary(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
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

    #[derive(Clone)]
    struct BlockingIdentity {
        started: mpsc::Sender<()>,
        release: Arc<(Mutex<bool>, Condvar)>,
        completed: Arc<AtomicUsize>,
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

    #[derive(Clone)]
    struct CountingIdentity {
        calls: Arc<AtomicUsize>,
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

    #[derive(Clone)]
    struct ProofCounter {
        calls: Arc<AtomicUsize>,
    }

    impl RecorderRpc for ProofCounter {
        fn recorder_id(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
        ) -> rhiza_quepaxa::Result<String> {
            Ok("node-1".into())
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

    #[derive(Clone)]
    struct IdentityRecorder;

    impl RecorderRpc for IdentityRecorder {
        fn recorder_id(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
        ) -> rhiza_quepaxa::Result<String> {
            Ok("node-1".into())
        }

        fn inspect_record_summary(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
            _slot: u64,
        ) -> rhiza_quepaxa::Result<Option<RecordSummary>> {
            Ok(None)
        }
    }

    #[derive(Clone)]
    struct OversizedFetch {
        calls: Arc<AtomicUsize>,
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
            Ok(Some(StoredCommand::new(
                rhiza_core::EntryType::Command,
                vec![0_u8; MAX_HTTP_BODY_BYTES],
            )))
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
        fn recorder_id(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
        ) -> rhiza_quepaxa::Result<String> {
            Ok("node-1".into())
        }

        fn inspect_record_summary(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
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
            _context: &rhiza_quepaxa::RecorderRpcContext,
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

    #[derive(Clone)]
    struct BackpressuredFetches {
        started: mpsc::Sender<()>,
        completed: mpsc::Sender<()>,
        release: Arc<(Mutex<bool>, Condvar)>,
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
        calls: Arc<AtomicUsize>,
        payload: Arc<StoredCommand>,
    }

    impl RecorderRpc for BackpressuredFetches {
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
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(active, Ordering::SeqCst);
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.started.send(()).unwrap();

            let (released, ready) = &*self.release;
            let mut released = released.lock().unwrap();
            while !*released {
                released = ready.wait(released).unwrap();
            }
            drop(released);

            self.active.fetch_sub(1, Ordering::SeqCst);
            self.completed.send(()).unwrap();
            Ok(Some((*self.payload).clone()))
        }
    }

    #[derive(Clone)]
    struct LargeThenPanicFetches {
        large_hash: LogHash,
        payload: Arc<StoredCommand>,
        admitted: Arc<AtomicUsize>,
        panic_started: mpsc::Sender<()>,
    }

    impl RecorderRpc for LargeThenPanicFetches {
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
            command_hash: LogHash,
        ) -> rhiza_quepaxa::Result<Option<StoredCommand>> {
            self.admitted.fetch_add(1, Ordering::SeqCst);
            if command_hash == self.large_hash {
                return Ok(Some((*self.payload).clone()));
            }
            self.panic_started.send(()).unwrap();
            panic!("injected postcard fetch panic after admission")
        }
    }

    #[derive(Clone)]
    struct LargeThenDeadlineFetches {
        large_hash: LogHash,
        payload: Arc<StoredCommand>,
        admitted: Arc<AtomicUsize>,
        expired: mpsc::Sender<()>,
        release: Arc<(Mutex<bool>, Condvar)>,
        completed: Arc<AtomicUsize>,
    }

    impl RecorderRpc for LargeThenDeadlineFetches {
        fn recorder_id(
            &self,
            _context: &rhiza_quepaxa::RecorderRpcContext,
        ) -> rhiza_quepaxa::Result<String> {
            Ok("node-1".into())
        }

        fn fetch_command_for(
            &self,
            context: &rhiza_quepaxa::RecorderRpcContext,
            _cluster_id: String,
            _epoch: u64,
            _config_id: u64,
            _config_digest: LogHash,
            command_hash: LogHash,
        ) -> rhiza_quepaxa::Result<Option<StoredCommand>> {
            self.admitted.fetch_add(1, Ordering::SeqCst);
            if command_hash == self.large_hash {
                return Ok(Some((*self.payload).clone()));
            }
            while context.check().is_ok() {
                thread::yield_now();
            }
            self.expired.send(()).unwrap();
            let (released, ready) = &*self.release;
            let mut released = released.lock().unwrap();
            while !*released {
                released = ready.wait(released).unwrap();
            }
            self.completed.fetch_add(1, Ordering::SeqCst);
            Ok(None)
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
        _force: tokio::sync::watch::Sender<bool>,
        _started: tokio::sync::oneshot::Receiver<()>,
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
                _force: force,
                _started: started_rx,
                _listener_dropped: listener_dropped_rx,
            },
            RecorderIngressLifecycle::new(shutdown_rx, force_rx, started, listener_dropped),
        )
    }

    #[test]
    fn borrowed_opaque_envelopes_are_wire_identical_exact_and_do_not_copy_body() {
        let request_body = RecorderRequestBody::InspectDecisionProof { slot: 7 };
        let body = postcard::to_allocvec(&request_body).unwrap();
        let request = build_opaque_bytes(None, Some(50), &request_body).unwrap();
        let (deadline, borrowed_request) = decode_opaque_request(&request).unwrap();
        assert_eq!(deadline, 50);
        assert_eq!(borrowed_request, body.as_slice());
        let request_start = request.as_ptr() as usize;
        let request_end = request_start + request.len();
        assert!(
            (request_start..request_end).contains(&(borrowed_request.as_ptr() as usize)),
            "opaque request body must borrow the transport frame"
        );
        let mut request_with_trailing = request.clone();
        request_with_trailing.push(0);
        assert!(decode_opaque_request(&request_with_trailing).is_err());

        let response_body = RecorderResponseBody::InspectDecisionProof(RpcResult::Ok(None));
        let response_inner = postcard::to_allocvec(&response_body).unwrap();
        let response = build_opaque_bytes(None, None, &response_body).unwrap();
        let borrowed_response = decode_opaque_response(&response).unwrap();
        assert_eq!(borrowed_response, response_inner.as_slice());
        let response_start = response.as_ptr() as usize;
        let response_end = response_start + response.len();
        assert!(
            (response_start..response_end).contains(&(borrowed_response.as_ptr() as usize)),
            "opaque response body must borrow the RPC frame"
        );
    }

    async fn authenticated_slow_reader(address: std::net::SocketAddr) -> tokio::net::TcpStream {
        let stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let stream = stream.into_std().unwrap();
        let socket = Socket::from(stream);
        // Keep the kernel receive window too small to absorb the deliberately
        // oversized responses below. The test never reads them.
        socket.set_recv_buffer_size(1024).unwrap();
        let stream: std::net::TcpStream = socket.into();
        let mut stream = tokio::net::TcpStream::from_std(stream).unwrap();
        super::super::write_value_async(
            &mut stream,
            &Hello {
                version: POSTCARD_RPC_WIRE_VERSION,
                node_id: "node-2".into(),
                recovery_generation: 7,
                token: "peer-token-2".into(),
            },
        )
        .await
        .unwrap();
        let reply: HelloReply =
            decode_exact(&read_frame_async(&mut stream).await.unwrap()).unwrap();
        assert!(matches!(
            reply,
            HelloReply::Accepted { version, recorder_id }
                if version == POSTCARD_RPC_WIRE_VERSION && recorder_id == "node-1"
        ));
        stream
    }

    async fn send_fetch_request(
        stream: &mut tokio::net::TcpStream,
        sequence: u32,
        command_hash: LogHash,
    ) {
        send_fetch_request_with_deadline(
            stream,
            sequence,
            command_hash,
            u32::try_from(CALL_TIMEOUT.as_millis()).unwrap(),
        )
        .await;
    }

    async fn send_raw_request(
        stream: &mut tokio::net::TcpStream,
        sequence: u32,
        operation: Operation,
        body: &RecorderRequestBody,
    ) {
        let mut frame = VarHeader {
            key: VarKey::Key8(request_key(operation)),
            seq_no: VarSeq::Seq4(sequence),
        }
        .write_to_vec();
        frame.extend_from_slice(&build_opaque_bytes(None, Some(30_000), body).unwrap());
        write_raw_frame(stream, &frame).await.unwrap();
    }

    async fn next_inner_decode_event(
        events: &mut tokio::sync::mpsc::UnboundedReceiver<TestInnerDecodeEvent>,
    ) -> TestInnerDecodeEvent {
        tokio::time::timeout(Duration::from_secs(1), events.recv())
            .await
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn actual_prpc_listener_rejects_bounded_proof_before_backend_and_recovers() {
        let _lock = test_inner_decode_lock().lock().await;
        let calls = Arc::new(AtomicUsize::new(0));
        let summary = rhiza_quepaxa::RecorderSummary {
            recorder_id: "r".into(),
            slot: 1,
            step: 1,
            first_current: None,
            aggregate_prior: None,
        };
        let body = RecorderRequestBody::InstallDecisionProof {
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
        };
        assert!(bounded_postcard_size(&body, OPAQUE_BODY_LIMIT).is_ok());
        let (events_tx, mut events) = tokio::sync::mpsc::unbounded_channel();
        let _hook = install_test_inner_decode_hook(TestInnerDecodeHook {
            sequences: Arc::new([70_001].into_iter().collect()),
            events: events_tx,
            release: None,
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (control, lifecycle) = test_ingress_lifecycle();
        let server = tokio::spawn(serve_recorder_postcard_rpc(
            listener,
            ProofCounter {
                calls: Arc::clone(&calls),
            },
            peers(),
            7,
            lifecycle,
        ));
        let mut stream = authenticated_slow_reader(address).await;
        send_raw_request(&mut stream, 70_001, Operation::InstallDecisionProof, &body).await;
        assert_eq!(
            next_inner_decode_event(&mut events).await,
            TestInnerDecodeEvent::Entered
        );
        assert_eq!(
            next_inner_decode_event(&mut events).await,
            TestInnerDecodeEvent::Failed("recorder decode heap budget exceeded".into())
        );
        let frame = tokio::time::timeout(Duration::from_secs(1), read_frame_async(&mut stream))
            .await
            .unwrap()
            .unwrap();
        let (header, payload) = VarHeader::take_from_slice(&frame).unwrap();
        assert_eq!(header.seq_no, VarSeq::Seq4(70_001));
        assert_eq!(
            header.key,
            VarKey::Key8(response_key(Operation::InstallDecisionProof))
        );
        let response: RecorderResponseBody =
            decode_exact(decode_opaque_response(payload).unwrap()).unwrap();
        assert!(
            matches!(
                response,
                RecorderResponseBody::InstallDecisionProof(RpcResult::Error(ref error))
                    if matches!(error.code, crate::RecorderWireErrorCode::Decode)
                        && matches!(
                            error.detail,
                            Some(crate::RecorderWireErrorDetail::Message(ref message))
                                if message == "recorder decode heap budget exceeded"
                        )
            ),
            "unexpected bounded-decode response: {response:?}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        send_raw_request(
            &mut stream,
            70_002,
            Operation::Identity,
            &RecorderRequestBody::Identity,
        )
        .await;
        let fresh = read_frame_async(&mut stream).await.unwrap();
        let (_, payload) = VarHeader::take_from_slice(&fresh).unwrap();
        assert!(matches!(
            decode_exact::<RecorderResponseBody>(decode_opaque_response(payload).unwrap()).unwrap(),
            RecorderResponseBody::Identity(RpcResult::Ok(_))
        ));
        drop(stream);
        control.shutdown.send_replace(true);
        assert!(server.await.unwrap().result.is_ok());
    }

    #[tokio::test]
    async fn prpc_shared_32_inner_decode_gate_admits_the_33rd_after_release() {
        let _lock = test_inner_decode_lock().lock().await;
        let _connection_limit = override_test_connection_limit(40);
        let release = Arc::new(tokio::sync::Notify::new());
        let (events_tx, mut events) = tokio::sync::mpsc::unbounded_channel();
        let _hook = install_test_inner_decode_hook(TestInnerDecodeHook {
            sequences: Arc::new((71_000..71_033).collect()),
            events: events_tx,
            release: Some(Arc::clone(&release)),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (control, lifecycle) = test_ingress_lifecycle();
        let server = tokio::spawn(serve_recorder_postcard_rpc(
            listener,
            IdentityRecorder,
            peers(),
            7,
            lifecycle,
        ));
        let mut streams = Vec::new();
        for _ in 0..33 {
            streams.push(authenticated_slow_reader(address).await);
        }
        for (index, stream) in streams.iter_mut().enumerate() {
            send_raw_request(
                stream,
                71_000 + u32::try_from(index).unwrap(),
                Operation::Identity,
                &RecorderRequestBody::Identity,
            )
            .await;
        }
        for _ in 0..MAX_SERVER_DECODE_CONCURRENCY {
            assert_eq!(
                next_inner_decode_event(&mut events).await,
                TestInnerDecodeEvent::Entered
            );
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(100), events.recv())
                .await
                .is_err()
        );
        release.notify_waiters();
        assert_eq!(
            next_inner_decode_event(&mut events).await,
            TestInnerDecodeEvent::Entered
        );
        release.notify_waiters();
        for (index, stream) in streams.iter_mut().enumerate() {
            let frame = tokio::time::timeout(Duration::from_secs(1), read_frame_async(stream))
                .await
                .unwrap()
                .unwrap();
            let (header, payload) = VarHeader::take_from_slice(&frame).unwrap();
            assert_eq!(
                header.seq_no,
                VarSeq::Seq4(71_000 + u32::try_from(index).unwrap())
            );
            assert!(matches!(
                decode_exact::<RecorderResponseBody>(decode_opaque_response(payload).unwrap())
                    .unwrap(),
                RecorderResponseBody::Identity(_)
            ));
        }
        // This test proves the shared decode gate, not backend admission. The
        // independent 32-operation-slot gate may return a terminal overload
        // response for a decoded request while the earlier response tasks
        // still own their permits.
        drop(streams);
        control.shutdown.send_replace(true);
        assert!(server.await.unwrap().result.is_ok());
    }

    #[tokio::test]
    async fn prpc_default_connection_cap_is_12_and_recovers_after_close() {
        let _lock = test_inner_decode_lock().lock().await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (control, lifecycle) = test_ingress_lifecycle();
        let server = tokio::spawn(serve_recorder_postcard_rpc(
            listener,
            IdentityRecorder,
            peers(),
            7,
            lifecycle,
        ));
        let mut streams = Vec::new();
        for _ in 0..MAX_SERVER_CONNECTIONS {
            streams.push(authenticated_slow_reader(address).await);
        }
        let mut thirteenth = tokio::net::TcpStream::connect(address).await.unwrap();
        super::super::write_value_async(
            &mut thirteenth,
            &Hello {
                version: POSTCARD_RPC_WIRE_VERSION,
                node_id: "node-2".into(),
                recovery_generation: 7,
                token: "peer-token-2".into(),
            },
        )
        .await
        .unwrap();
        assert!(
            !matches!(
                tokio::time::timeout(
                    Duration::from_millis(100),
                    read_frame_async(&mut thirteenth)
                )
                .await,
                Ok(Ok(_))
            ),
            "the thirteenth connection must not reach authenticated PRPC processing"
        );
        drop(thirteenth);
        drop(streams.pop());
        let recovered = authenticated_slow_reader(address).await;
        drop(recovered);
        drop(streams);
        control.shutdown.send_replace(true);
        assert!(server.await.unwrap().result.is_ok());
    }

    fn malicious_decision_proof_response() -> RecorderResponseBody {
        let summary = rhiza_quepaxa::RecorderSummary {
            recorder_id: "r".into(),
            slot: 1,
            step: 1,
            first_current: None,
            aggregate_prior: None,
        };
        RecorderResponseBody::InspectDecisionProof(RpcResult::Ok(Some(DecisionProof::FastPath {
            cluster_id: "cluster".into(),
            slot: 1,
            epoch: 1,
            config_id: 1,
            config_digest: LogHash::ZERO,
            proposal: rhiza_quepaxa::Proposal::new(
                rhiza_quepaxa::ProposalPriority::from_u64(1),
                "node",
                1,
                rhiza_quepaxa::AcceptedValue {
                    command_hash: LogHash::ZERO,
                    prev_hash: LogHash::ZERO,
                    entry_hash: LogHash::ZERO,
                },
            ),
            summaries: vec![summary; 16_384],
        })))
    }

    #[test]
    fn malicious_opaque_response_reaches_sealed_bounded_decoder_without_partial_acceptance() {
        let response = malicious_decision_proof_response();
        let opaque = build_opaque_bytes(None, None, &response).unwrap();
        let encoded = decode_opaque_response(&opaque).unwrap();
        let decoded = decode_postcard_exact_bounded::<RecorderResponseBody>(
            encoded,
            RecorderDecodeLimits::for_wire_bytes(OPAQUE_BODY_LIMIT),
        )
        .map_err(|error| Error::Decode(error.to_string()));
        assert!(
            matches!(decoded, Err(Error::Decode(ref message)) if message == "recorder decode heap budget exceeded"),
            "the malicious sealed response must fail before producing an accepted body: {decoded:?}"
        );
    }

    struct TestResponseBarrier {
        release: Option<mpsc::SyncSender<()>>,
    }

    impl TestResponseBarrier {
        fn new(release: mpsc::SyncSender<()>) -> Self {
            Self {
                release: Some(release),
            }
        }

        fn release(&mut self) {
            if let Some(release) = self.release.take() {
                let _ = release.send(());
            }
        }
    }

    impl Drop for TestResponseBarrier {
        fn drop(&mut self) {
            self.release();
        }
    }

    #[derive(Debug, Eq, PartialEq)]
    enum PublicPrpcServerEvent {
        Hello(usize),
        Record(usize),
        FirstResponseFlushed,
    }

    fn record_request() -> RecordRequest {
        RecordRequest {
            cluster_id: "cluster".into(),
            epoch: 1,
            config_id: 1,
            config_digest: LogHash::ZERO,
            slot: 1,
            step: 1,
            proposal: rhiza_quepaxa::Proposal::nil(),
            command: None,
        }
    }

    #[test]
    fn public_prpc_malicious_response_evicts_its_lane_before_a_fresh_mutation_connection() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (events_tx, events_rx) = mpsc::sync_channel(5);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let server = thread::spawn(move || {
            for connection in 1..=2 {
                let (mut stream, _) = listener.accept().unwrap();
                let hello: Hello =
                    decode_exact(&super::super::read_frame_sync(&mut stream).unwrap()).unwrap();
                assert_eq!(hello.node_id, "node-2");
                events_tx
                    .send(PublicPrpcServerEvent::Hello(connection))
                    .unwrap();
                super::super::write_value_sync(
                    &mut stream,
                    &HelloReply::Accepted {
                        version: POSTCARD_RPC_WIRE_VERSION,
                        recorder_id: "node-1".into(),
                    },
                )
                .unwrap();
                let frame = super::super::read_frame_sync(&mut stream).unwrap();
                let (header, _) = VarHeader::take_from_slice(&frame).unwrap();
                assert_eq!(header.key, VarKey::Key8(request_key(Operation::Record)));
                events_tx
                    .send(PublicPrpcServerEvent::Record(connection))
                    .unwrap();

                if connection == 1 {
                    let raw = build_opaque_bytes(
                        Some(VarHeader {
                            key: VarKey::Key8(response_key(Operation::Record)),
                            seq_no: header.seq_no,
                        }),
                        None,
                        &malicious_decision_proof_response(),
                    )
                    .unwrap();
                    stream.write_all(&frame_length(&raw).unwrap()).unwrap();
                    stream.write_all(&raw).unwrap();
                    stream.flush().unwrap();
                    events_tx
                        .send(PublicPrpcServerEvent::FirstResponseFlushed)
                        .unwrap();
                    release_rx
                        .recv_timeout(Duration::from_secs(5))
                        .expect("client result must release the first response barrier");
                }
            }
        });
        let client =
            TcpPostcardRpcRecorderClient::new(address, "node-1", "node-2", "peer-token-2", 7)
                .unwrap();
        let mut first_response_barrier = TestResponseBarrier::new(release_tx);
        let first_context = rhiza_quepaxa::RecorderRpcContext::with_deadline(
            Instant::now() + Duration::from_secs(2),
        );
        assert!(matches!(
            client.record(&first_context, record_request()),
            Err(Error::UnknownOutcome)
        ));
        assert_eq!(
            events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            PublicPrpcServerEvent::Hello(1)
        );
        assert_eq!(
            events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            PublicPrpcServerEvent::Record(1)
        );
        assert_eq!(
            events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            PublicPrpcServerEvent::FirstResponseFlushed
        );
        first_response_barrier.release();

        let second_context = rhiza_quepaxa::RecorderRpcContext::with_deadline(
            Instant::now() + Duration::from_secs(2),
        );
        assert!(matches!(
            client.record(&second_context, record_request()),
            Err(Error::UnknownOutcome)
        ));
        assert_eq!(
            events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            PublicPrpcServerEvent::Hello(2)
        );
        assert_eq!(
            events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            PublicPrpcServerEvent::Record(2)
        );
        server.join().unwrap();
    }

    async fn send_fetch_request_with_deadline(
        stream: &mut tokio::net::TcpStream,
        sequence: u32,
        command_hash: LogHash,
        remaining_deadline_ms: u32,
    ) {
        let body = RecorderRequestBody::FetchCommand {
            cluster_id: "rhiza:sql:cluster-a".into(),
            epoch: 1,
            config_id: 1,
            config_digest: LogHash::ZERO,
            command_hash,
        };
        let opaque = (remaining_deadline_ms, postcard::to_allocvec(&body).unwrap());
        let mut frame = VarHeader {
            key: VarKey::Key8(request_key(Operation::FetchCommand)),
            seq_no: VarSeq::Seq4(sequence),
        }
        .write_to_vec();
        frame.extend_from_slice(&postcard::to_allocvec(&opaque).unwrap());
        write_raw_frame(stream, &frame).await.unwrap();
    }

    async fn send_store_command_request_with_deadline(
        stream: &mut tokio::net::TcpStream,
        sequence: u32,
        remaining_deadline_ms: u32,
    ) {
        let command = StoredCommand::new(rhiza_core::EntryType::Command, b"gate".to_vec());
        let body = RecorderRequestBody::StoreCommand {
            cluster_id: "rhiza:sql:cluster-a".into(),
            epoch: 1,
            config_id: 1,
            config_digest: LogHash::ZERO,
            command_hash: command.hash(),
            command,
        };
        let opaque = (remaining_deadline_ms, postcard::to_allocvec(&body).unwrap());
        let mut frame = VarHeader {
            key: VarKey::Key8(request_key(Operation::StoreCommand)),
            seq_no: VarSeq::Seq4(sequence),
        }
        .write_to_vec();
        frame.extend_from_slice(&postcard::to_allocvec(&opaque).unwrap());
        write_raw_frame(stream, &frame).await.unwrap();
    }

    #[test]
    fn endpoint_keys_are_unique_and_version_fenced() {
        assert_eq!(POSTCARD_RPC_WIRE_VERSION, 6);
        assert_eq!(POSTCARD_RPC_TLS_ALPN, b"rhiza-recorder-prpc/5");
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

    #[test]
    fn opaque_envelope_is_exact_and_rejects_oversized_body_before_output_allocation() {
        let body = RecorderRequestBody::Identity;
        let frame = build_opaque_bytes(None, Some(73), &body).unwrap();
        let body_size = bounded_postcard_size(&body, OPAQUE_BODY_LIMIT).unwrap();
        assert_eq!(
            frame.len(),
            postcard_encoded_len(&73_u32).unwrap()
                + postcard_encoded_len(&body_size).unwrap()
                + body_size
        );
        let (deadline, encoded_body): OpaqueRequest = decode_exact(&frame).unwrap();
        assert_eq!(deadline, 73);
        assert!(matches!(
            decode_exact::<RecorderRequestBody>(&encoded_body).unwrap(),
            RecorderRequestBody::Identity
        ));

        let oversized = RecorderRequestBody::StoreCommand {
            cluster_id: "rhiza:sql:cluster-a".into(),
            epoch: 1,
            config_id: 1,
            config_digest: LogHash::ZERO,
            command_hash: LogHash::ZERO,
            command: StoredCommand::new(
                rhiza_core::EntryType::Command,
                vec![0_u8; MAX_HTTP_BODY_BYTES],
            ),
        };
        assert!(build_opaque_bytes(None, Some(73), &oversized).is_err());
    }

    #[test]
    fn oversized_mutation_is_decode_before_postcard_bridge_admission() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let client =
            TcpPostcardRpcRecorderClient::new(address, "node-1", "node-2", "peer-token-2", 7)
                .unwrap();
        let command = StoredCommand::new(
            rhiza_core::EntryType::Command,
            vec![0_u8; MAX_HTTP_BODY_BYTES],
        );
        let result = client.store_command_for(
            &rhiza_quepaxa::RecorderRpcContext::default_timeout(),
            "rhiza:sql:cluster-a".into(),
            1,
            1,
            LogHash::ZERO,
            command.hash(),
            command,
        );
        assert!(matches!(result, Err(Error::Decode(_))));
        assert_eq!(client.next_sequence.load(Ordering::Relaxed), 0);
        assert_eq!(client.consensus.sender.capacity(), BRIDGE_DEPTH);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn oversized_backend_fetch_returns_typed_decode_response() {
        let calls = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (_control, lifecycle) = test_ingress_lifecycle();
        let server = tokio::spawn(serve_recorder_postcard_rpc(
            listener,
            OversizedFetch {
                calls: Arc::clone(&calls),
            },
            peers(),
            7,
            lifecycle,
        ));
        let client =
            TcpPostcardRpcRecorderClient::new(address, "node-1", "node-2", "peer-token-2", 7)
                .unwrap();
        let result = tokio::task::spawn_blocking(move || {
            client.fetch_command_for(
                &rhiza_quepaxa::RecorderRpcContext::default_timeout(),
                "rhiza:sql:cluster-a".into(),
                1,
                1,
                LogHash::ZERO,
                LogHash::ZERO,
            )
        })
        .await
        .unwrap();
        assert!(
            matches!(result, Err(Error::Decode(message)) if message == "recorder postcard-rpc response exceeds frame limit")
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        server.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn expired_raw_mutation_at_dispatch_gate_never_reaches_recorder_and_recovers() {
        let _global_hook_lock = test_global_hook_lock().lock().await;
        let sequence = u32::MAX - 101;
        let (entered_tx, mut entered_rx) = tokio::sync::mpsc::unbounded_channel();
        let release = Arc::new(tokio::sync::Notify::new());
        *TEST_DISPATCH_DEADLINE_GATE.lock().unwrap() = Some(TestDispatchDeadlineGate {
            sequence,
            entered: entered_tx,
            release: Arc::clone(&release),
        });
        let (started_tx, _started_rx) = mpsc::channel();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let mutations = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (_control, lifecycle) = test_ingress_lifecycle();
        let server = tokio::spawn(serve_recorder_postcard_rpc(
            listener,
            BlockingInspections {
                started: started_tx,
                release: Arc::new((Mutex::new(true), Condvar::new())),
                seen,
                mutations: Arc::clone(&mutations),
            },
            peers(),
            7,
            lifecycle,
        ));
        let mut stream = authenticated_slow_reader(address).await;
        send_store_command_request_with_deadline(&mut stream, sequence, 50).await;

        let deadline = tokio::time::timeout(Duration::from_secs(1), entered_rx.recv())
            .await
            .expect("server did not enter the dispatch-deadline gate")
            .expect("server closed the dispatch-deadline gate");
        tokio::time::sleep_until(deadline.into()).await;
        release.notify_one();

        let frame = tokio::time::timeout(Duration::from_secs(1), read_frame_async(&mut stream))
            .await
            .expect("server did not respond after dispatch-gate release")
            .unwrap();
        let (header, payload) = VarHeader::take_from_slice(&frame).unwrap();
        assert_eq!(header.seq_no, VarSeq::Seq4(sequence));
        assert_eq!(
            header.key,
            VarKey::Key8(response_key(Operation::StoreCommand))
        );
        let response: OpaqueResponse = decode_exact(payload).unwrap();
        let response: RecorderResponseBody = decode_exact(&response).unwrap();
        assert!(matches!(
            response,
            RecorderResponseBody::StoreCommand(RpcResult::Error(error))
                if matches!(error.code, crate::RecorderWireErrorCode::RpcDeadlineExceeded)
        ));
        assert_eq!(mutations.load(Ordering::SeqCst), 0);

        TEST_DISPATCH_DEADLINE_GATE.lock().unwrap().take();
        drop(stream);
        let recovered =
            TcpPostcardRpcRecorderClient::new(address, "node-1", "node-2", "peer-token-2", 7)
                .unwrap();
        assert_eq!(
            tokio::task::spawn_blocking(move || {
                recovered.recorder_id(&rhiza_quepaxa::RecorderRpcContext::default_timeout())
            })
            .await
            .unwrap()
            .unwrap(),
            "node-1"
        );
        server.abort();
    }

    #[tokio::test]
    async fn saturated_postcard_server_rejects_hello_without_backend_call() {
        let calls = Arc::new(AtomicUsize::new(0));
        let slots = Arc::new(tokio::sync::Semaphore::new(1));
        let held = Arc::clone(&slots).acquire_owned().await.unwrap();
        let (mut client, server_stream) = tokio::io::duplex(4096);
        let server = tokio::spawn(serve_postcard_rpc_connection(
            server_stream,
            CountingIdentity {
                calls: Arc::clone(&calls),
            },
            peers().into(),
            7,
            slots,
        ));
        super::super::write_value_async(
            &mut client,
            &Hello {
                version: POSTCARD_RPC_WIRE_VERSION,
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

    #[tokio::test(flavor = "multi_thread")]
    async fn postcard_hello_identity_shutdown_waits_for_the_blocking_backend() {
        let (started_tx, started_rx) = mpsc::channel();
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let completed = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (control, lifecycle) = test_ingress_lifecycle();
        let mut server = tokio::spawn(serve_recorder_postcard_rpc(
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
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        super::super::write_value_async(
            &mut stream,
            &Hello {
                version: POSTCARD_RPC_WIRE_VERSION,
                node_id: "node-2".into(),
                recovery_generation: 7,
                token: "peer-token-2".into(),
            },
        )
        .await
        .unwrap();
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        control.shutdown.send_replace(true);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut server)
                .await
                .is_err(),
            "shutdown drained before the admitted postcard HELLO identity backend completed"
        );
        let (released, ready) = &*release;
        *released.lock().unwrap() = true;
        ready.notify_all();
        let exit = tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("server did not drain after postcard identity release")
            .unwrap();
        assert!(exit.result.is_ok());
        assert_eq!(exit.tasks, super::super::RecorderTaskDisposition::Quiesced);
        assert_eq!(completed.load(Ordering::SeqCst), 1);
        drop(stream);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn slow_reader_holds_postcard_admission_permits_until_connection_close() {
        let (started_tx, started_rx) = mpsc::channel();
        let (completed_tx, completed_rx) = mpsc::channel();
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));
        let calls = Arc::new(AtomicUsize::new(0));
        let payload = Arc::new(StoredCommand::new(
            rhiza_core::EntryType::Command,
            vec![0x5a; 2 * 1024 * 1024],
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (_control, lifecycle) = test_ingress_lifecycle();
        let server = tokio::spawn(serve_recorder_postcard_rpc(
            listener,
            BackpressuredFetches {
                started: started_tx,
                completed: completed_tx,
                release: Arc::clone(&release),
                active: Arc::clone(&active),
                max_active: Arc::clone(&max_active),
                calls: Arc::clone(&calls),
                payload: Arc::clone(&payload),
            },
            peers(),
            7,
            lifecycle,
        ));
        let mut slow_reader = authenticated_slow_reader(address).await;
        let command_hash = payload.hash();
        for sequence in 1..=u32::try_from(DEFAULT_PEER_CONCURRENCY + 1).unwrap() {
            send_fetch_request(&mut slow_reader, sequence, command_hash).await;
        }
        for _ in 0..DEFAULT_PEER_CONCURRENCY {
            started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        }
        assert_eq!(calls.load(Ordering::SeqCst), DEFAULT_PEER_CONCURRENCY);
        assert_eq!(active.load(Ordering::SeqCst), DEFAULT_PEER_CONCURRENCY);
        assert_eq!(max_active.load(Ordering::SeqCst), DEFAULT_PEER_CONCURRENCY);

        let (released, ready) = &*release;
        *released.lock().unwrap() = true;
        ready.notify_all();
        for _ in 0..DEFAULT_PEER_CONCURRENCY {
            completed_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        }
        assert_eq!(active.load(Ordering::SeqCst), 0);

        // Every admitted backend call has finished, but the client is still
        // refusing to read its oversized responses. A fresh authenticated
        // connection must therefore be overloaded rather than admitted.
        let saturated =
            TcpPostcardRpcRecorderClient::new(address, "node-1", "node-2", "peer-token-2", 7)
                .unwrap();
        let saturated_result = tokio::task::spawn_blocking(move || {
            saturated.recorder_id(&rhiza_quepaxa::RecorderRpcContext::default_timeout())
        })
        .await
        .unwrap();
        assert!(
            saturated_result.is_err(),
            "slow-reader response tasks released admission permits before socket completion"
        );
        assert_eq!(calls.load(Ordering::SeqCst), DEFAULT_PEER_CONCURRENCY);
        assert_eq!(max_active.load(Ordering::SeqCst), DEFAULT_PEER_CONCURRENCY);

        drop(slow_reader);
        let recovered =
            TcpPostcardRpcRecorderClient::new(address, "node-1", "node-2", "peer-token-2", 7)
                .unwrap();
        let recovered = tokio::task::spawn_blocking(move || {
            let deadline = Instant::now() + Duration::from_secs(2);
            loop {
                match recovered.recorder_id(&rhiza_quepaxa::RecorderRpcContext::default_timeout()) {
                    Ok(id) => return id,
                    Err(error) if Instant::now() < deadline => {
                        let _ = error;
                        thread::yield_now();
                    }
                    Err(error) => {
                        panic!("permits were not recovered after slow-reader close: {error}")
                    }
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(recovered, "node-1");
        assert_eq!(calls.load(Ordering::SeqCst), DEFAULT_PEER_CONCURRENCY);
        server.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn slow_reader_holds_postcard_permits_through_panic_responses() {
        let _global_hook_lock = test_global_hook_lock().lock().await;
        let payload = Arc::new(StoredCommand::new(
            rhiza_core::EntryType::Command,
            vec![0x5a; 2 * 1024 * 1024],
        ));
        let large_hash = payload.hash();
        let admitted = Arc::new(AtomicUsize::new(0));
        let (panic_tx, panic_rx) = mpsc::channel();
        let panic_sequence = u32::try_from(DEFAULT_PEER_CONCURRENCY).unwrap();
        let (response_attempt_tx, response_attempt_rx) = mpsc::channel();
        let (backend_dropped_tx, _backend_dropped_rx) = mpsc::channel();
        *TEST_PERMIT_LIFECYCLE_HOOK.lock().unwrap() = Some(TestPermitLifecycleHook {
            sequence: panic_sequence,
            response_attempted: response_attempt_tx,
            backend_permit_dropped: backend_dropped_tx,
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (_control, lifecycle) = test_ingress_lifecycle();
        let server = tokio::spawn(serve_recorder_postcard_rpc(
            listener,
            LargeThenPanicFetches {
                large_hash,
                payload,
                admitted: Arc::clone(&admitted),
                panic_started: panic_tx,
            },
            peers(),
            7,
            lifecycle,
        ));
        let mut slow_reader = authenticated_slow_reader(address).await;
        // Saturate the writer with oversized successful responses first; the
        // final panic response must queue behind those writes.
        for sequence in 1..u32::try_from(DEFAULT_PEER_CONCURRENCY).unwrap() {
            send_fetch_request(&mut slow_reader, sequence, large_hash).await;
        }
        send_fetch_request(&mut slow_reader, panic_sequence, LogHash::ZERO).await;
        panic_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while admitted.load(Ordering::SeqCst) < DEFAULT_PEER_CONCURRENCY {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("all initial calls must be admitted before checking overload");
        assert_eq!(admitted.load(Ordering::SeqCst), DEFAULT_PEER_CONCURRENCY);
        response_attempt_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("panic response never reached the production writer path");

        // The first response blocks the writer. Panic responses are therefore
        // queued behind it and must retain their admission permits too.
        send_fetch_request(
            &mut slow_reader,
            u32::try_from(DEFAULT_PEER_CONCURRENCY + 1).unwrap(),
            LogHash::ZERO,
        )
        .await;
        assert!(
            panic_rx.recv_timeout(Duration::from_millis(100)).is_err(),
            "a panic response released admission before its socket write completed"
        );
        assert_eq!(admitted.load(Ordering::SeqCst), DEFAULT_PEER_CONCURRENCY);

        drop(slow_reader);
        let recovered =
            TcpPostcardRpcRecorderClient::new(address, "node-1", "node-2", "peer-token-2", 7)
                .unwrap();
        assert_eq!(
            tokio::task::spawn_blocking(move || {
                recovered.recorder_id(&rhiza_quepaxa::RecorderRpcContext::default_timeout())
            })
            .await
            .unwrap()
            .unwrap(),
            "node-1"
        );
        TEST_PERMIT_LIFECYCLE_HOOK.lock().unwrap().take();
        server.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn slow_reader_holds_postcard_permits_through_deadline_responses() {
        let _global_hook_lock = test_global_hook_lock().lock().await;
        let payload = Arc::new(StoredCommand::new(
            rhiza_core::EntryType::Command,
            vec![0x5a; 2 * 1024 * 1024],
        ));
        let large_hash = payload.hash();
        let admitted = Arc::new(AtomicUsize::new(0));
        let (expired_tx, expired_rx) = mpsc::channel();
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let completed = Arc::new(AtomicUsize::new(0));
        let timeout_sequence = u32::MAX - 11;
        let (response_attempt_tx, response_attempt_rx) = mpsc::channel();
        let (backend_dropped_tx, backend_dropped_rx) = mpsc::channel();
        *TEST_PERMIT_LIFECYCLE_HOOK.lock().unwrap() = Some(TestPermitLifecycleHook {
            sequence: timeout_sequence,
            response_attempted: response_attempt_tx,
            backend_permit_dropped: backend_dropped_tx,
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (_control, lifecycle) = test_ingress_lifecycle();
        let server = tokio::spawn(serve_recorder_postcard_rpc(
            listener,
            LargeThenDeadlineFetches {
                large_hash,
                payload,
                admitted: Arc::clone(&admitted),
                expired: expired_tx,
                release: Arc::clone(&release),
                completed: Arc::clone(&completed),
            },
            peers(),
            7,
            lifecycle,
        ));
        let mut slow_reader = authenticated_slow_reader(address).await;
        for sequence in 1..u32::try_from(DEFAULT_PEER_CONCURRENCY).unwrap() {
            send_fetch_request(&mut slow_reader, sequence, large_hash).await;
        }
        // The backend must enter before its advertised deadline; the event
        // below, not a scheduling sleep, determines when the timeout branch
        // has actually run.
        send_fetch_request_with_deadline(&mut slow_reader, timeout_sequence, LogHash::ZERO, 5_000)
            .await;
        expired_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("production timeout_at branch did not expire the admitted backend");
        tokio::time::timeout(Duration::from_secs(2), async {
            while admitted.load(Ordering::SeqCst) < DEFAULT_PEER_CONCURRENCY {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("all initial calls must be admitted before checking overload");

        response_attempt_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("server timeout branch did not attempt the queued deadline response");
        let (released, ready) = &*release;
        *released.lock().unwrap() = true;
        ready.notify_all();
        tokio::time::timeout(Duration::from_secs(2), async {
            while completed.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("timed-out backend did not later finish and release its Arc clone");
        backend_dropped_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("backend Arc permit clone was not dropped after dispatch returned");

        for _ in 0..3 {
            let probe =
                TcpPostcardRpcRecorderClient::new(address, "node-1", "node-2", "peer-token-2", 7)
                    .unwrap();
            assert!(tokio::task::spawn_blocking(move || {
                probe.recorder_id(&rhiza_quepaxa::RecorderRpcContext::default_timeout())
            })
            .await
            .unwrap()
            .is_err());
        }
        assert!(
            expired_rx.recv_timeout(Duration::from_millis(100)).is_err(),
            "a deadline response released admission before its socket write completed"
        );
        assert_eq!(admitted.load(Ordering::SeqCst), DEFAULT_PEER_CONCURRENCY);

        drop(slow_reader);
        TEST_PERMIT_LIFECYCLE_HOOK.lock().unwrap().take();
        let recovered =
            TcpPostcardRpcRecorderClient::new(address, "node-1", "node-2", "peer-token-2", 7)
                .unwrap();
        assert_eq!(
            tokio::task::spawn_blocking(move || {
                recovered.recorder_id(&rhiza_quepaxa::RecorderRpcContext::default_timeout())
            })
            .await
            .unwrap()
            .unwrap(),
            "node-1"
        );
        server.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn late_response_after_timeout_is_dropped_and_next_call_recovers() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (_control, lifecycle) = test_ingress_lifecycle();
        let server = tokio::spawn(serve_recorder_postcard_rpc(
            listener,
            SlowFirstInspection,
            peers(),
            7,
            lifecycle,
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
        assert!(tokio::task::spawn_blocking(move || {
            timed_out
                .inspect_record_summary(&rhiza_quepaxa::RecorderRpcContext::default_timeout(), 1)
        })
        .await
        .unwrap()
        .is_err());
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(tokio::task::spawn_blocking(move || {
            client.inspect_record_summary(&rhiza_quepaxa::RecorderRpcContext::default_timeout(), 2)
        })
        .await
        .unwrap()
        .unwrap()
        .is_none());
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
            client.observe_read_fence(
                &rhiza_quepaxa::RecorderRpcContext::default_timeout(),
                ReadFenceRequest {
                    cluster_id: "cluster".into(),
                    epoch: 1,
                    config_id: 1,
                    config_digest: LogHash::ZERO,
                    slot: 1,
                },
            )
        })
        .await
        .unwrap();

        assert!(matches!(result, Err(Error::RpcDeadlineExceeded)));
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
            client.record(
                &rhiza_quepaxa::RecorderRpcContext::default_timeout(),
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
            )
        })
        .await
        .unwrap();

        assert!(matches!(result, Err(Error::UnknownOutcome)));
        assert!(started.elapsed() < Duration::from_millis(1_500));
        blackhole.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn queue_admitted_mutation_transport_failure_remains_unknown() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (request_tx, request_rx) = tokio::sync::oneshot::channel();
        let blackhole = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let hello: Hello = decode_exact(&read_frame_async(&mut stream).await.unwrap()).unwrap();
            assert_eq!(hello.version, POSTCARD_RPC_WIRE_VERSION);
            super::super::write_value_async(
                &mut stream,
                &HelloReply::Accepted {
                    version: POSTCARD_RPC_WIRE_VERSION,
                    recorder_id: "node-1".into(),
                },
            )
            .await
            .unwrap();
            read_frame_async(&mut stream).await.unwrap();
            let _ = request_tx.send(());
            std::future::pending::<()>().await;
        });
        let client = TcpPostcardRpcRecorderClient::new_with_transport_and_timeout(
            address,
            "node-1",
            "node-2",
            "peer-token-2",
            7,
            ClientTransport::Plain,
            Duration::from_millis(100),
        )
        .unwrap();
        let command = StoredCommand::new(rhiza_core::EntryType::Command, b"queued".to_vec());
        let call = tokio::task::spawn_blocking(move || {
            client.store_command_for(
                &rhiza_quepaxa::RecorderRpcContext::default_timeout(),
                "rhiza:sql:cluster-a".into(),
                1,
                1,
                LogHash::ZERO,
                command.hash(),
                command,
            )
        });
        tokio::time::timeout(Duration::from_secs(1), request_rx)
            .await
            .expect("bridge-admitted mutation never reached the transport")
            .unwrap();
        assert!(matches!(call.await.unwrap(), Err(Error::UnknownOutcome)));
        blackhole.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn server_deadline_returns_while_admitted_mutation_finishes_and_shutdown_drains_it() {
        let (started_tx, started_rx) = mpsc::channel();
        let release = Arc::new((Mutex::new(false), Condvar::new()));
        let completed = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (control, lifecycle) = test_ingress_lifecycle();
        let server = tokio::spawn(serve_recorder_postcard_rpc(
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
        let request = RpcFrame {
            header: VarHeader {
                key: VarKey::Key8(request_key(Operation::StoreCommand)),
                seq_no: VarSeq::Seq4(1),
            },
            body: build_opaque_bytes(None, Some(50), &request).unwrap(),
        };
        let response = tokio::time::timeout(
            Duration::from_millis(300),
            send_endpoint(&client, Operation::StoreCommand, request),
        )
        .await;
        started_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        control.shutdown.send_replace(true);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!server.is_finished());
        let (released, ready) = &*release;
        *released.lock().unwrap() = true;
        ready.notify_all();
        let exit = server.await.unwrap();
        assert!(exit.result.is_ok());
        assert_eq!(exit.tasks, super::super::RecorderTaskDisposition::Quiesced);
        assert_eq!(completed.load(Ordering::SeqCst), 1);
        client.close();
        let response = response
            .expect("server must answer the advertised deadline")
            .unwrap();
        assert!(matches!(
            decode_exact::<RecorderResponseBody>(decode_opaque_response(&response.body).unwrap())
                .unwrap(),
            RecorderResponseBody::StoreCommand(RpcResult::Error(error))
                if matches!(error.code, crate::RecorderWireErrorCode::RpcDeadlineExceeded)
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
        assert!(tokio::task::spawn_blocking(move || {
            blackholed.recorder_id(&rhiza_quepaxa::RecorderRpcContext::default_timeout())
        })
        .await
        .unwrap()
        .is_err());
        tokio::time::timeout(Duration::from_secs(1), closed_rx)
            .await
            .expect("timed-out session socket must close")
            .unwrap();
        assert_eq!(
            tokio::task::spawn_blocking(move || {
                client.recorder_id(&rhiza_quepaxa::RecorderRpcContext::default_timeout())
            })
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
        let first = tokio::task::spawn_blocking(move || {
            connecting.recorder_id(&rhiza_quepaxa::RecorderRpcContext::default_timeout())
        });
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
                    sequence: u32::try_from(slot).expect("test sequence fits u32"),
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
                sequence: 129,
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
                    (worker..10_000).step_by(4).find_map(|slot| {
                        client
                            .inspect_record_summary(
                                &rhiza_quepaxa::RecorderRpcContext::default_timeout(),
                                slot,
                            )
                            .err()
                    })
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
        let (_control, lifecycle) = test_ingress_lifecycle();
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
            lifecycle,
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
                    client.inspect_record_summary(
                        &rhiza_quepaxa::RecorderRpcContext::default_timeout(),
                        u64::try_from(slot).unwrap(),
                    )
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
                sequence: 1,
                deadline: Instant::now() + Duration::from_millis(50),
                reply,
            })
            .unwrap_or_else(|_| panic!("short-lived request should enter the bounded queue"));
        assert!(matches!(
            receive.recv_timeout(Duration::from_millis(75)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        let (released, ready) = &*release;
        *released.lock().unwrap() = true;
        ready.notify_all();
        for blocker in blockers {
            assert!(blocker.await.unwrap().is_ok());
        }
        assert_eq!(
            client
                .recorder_id(&rhiza_quepaxa::RecorderRpcContext::default_timeout())
                .unwrap(),
            "node-1"
        );
        assert!(matches!(
            receive.recv_timeout(Duration::from_secs(1)),
            Ok(Err(Error::RpcDeadlineExceeded))
        ));
        assert_eq!(mutations.load(Ordering::SeqCst), 0);
        let mut seen = seen.lock().unwrap().clone();
        seen.sort_unstable();
        assert_eq!(
            seen,
            (1..=u64::try_from(LANE_IN_FLIGHT).unwrap()).collect::<Vec<_>>()
        );
        server.abort();
    }

    #[tokio::test]
    async fn force_cancels_backpressured_direct_error_and_overload_writes() {
        struct BackpressuredWriter {
            entered: Option<tokio::sync::oneshot::Sender<()>>,
        }

        impl AsyncWrite for BackpressuredWriter {
            fn poll_write(
                mut self: std::pin::Pin<&mut Self>,
                _context: &mut std::task::Context<'_>,
                _buffer: &[u8],
            ) -> std::task::Poll<std::io::Result<usize>> {
                if let Some(entered) = self.entered.take() {
                    let _ = entered.send(());
                }
                std::task::Poll::Pending
            }

            fn poll_flush(
                self: std::pin::Pin<&mut Self>,
                _context: &mut std::task::Context<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                std::task::Poll::Pending
            }

            fn poll_shutdown(
                self: std::pin::Pin<&mut Self>,
                _context: &mut std::task::Context<'_>,
            ) -> std::task::Poll<std::io::Result<()>> {
                std::task::Poll::Pending
            }
        }

        for body in [
            super::super::error_response(Operation::Identity, Error::Decode("bad frame".into())),
            super::super::overloaded_response(Operation::Identity),
        ] {
            let (entered, entered_rx) = tokio::sync::oneshot::channel();
            let writer = Arc::new(tokio::sync::Mutex::new(BackpressuredWriter {
                entered: Some(entered),
            }));
            let (force, force_rx) = tokio::sync::watch::channel(false);
            let task_writer = Arc::clone(&writer);
            let response = tokio::spawn(async move {
                send_response(
                    &task_writer,
                    Some(force_rx),
                    Operation::Identity,
                    VarSeq::Seq4(7),
                    body,
                )
                .await
            });
            tokio::time::timeout(Duration::from_secs(1), entered_rx)
                .await
                .expect("direct response did not enter the writer")
                .unwrap();
            force.send_replace(true);
            assert!(tokio::time::timeout(Duration::from_secs(1), response)
                .await
                .expect("force did not cancel a backpressured direct response")
                .unwrap()
                .is_ok());
        }
    }
}
