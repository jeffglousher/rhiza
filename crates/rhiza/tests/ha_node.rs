use std::{
    future::{poll_fn, Future},
    net::SocketAddr,
    path::Path,
    sync::{mpsc, Arc, Condvar, Mutex},
    task::Poll,
    time::Duration,
};

use rhiza_archive::{CheckpointIdentity, ObjectArchiveStore};
use rhiza_core::{EntryType, LogHash, StoredCommand};
use rhiza_node::HttpRecorderClient;
use rhiza_obj_store::{ObjStore, ObjStoreConfig};
use rhiza_quepaxa::{Membership, RecordSummary, RecorderFileStore, RecorderRpc};
use rhizadb::{
    DurabilityMode, Error, ExecutionProfile, HaNodeError, HaRecorderTransport, HaServeConfig,
    HaStartupConfig, HaStartupMode, LogPeer, NodeConfig, PeerConfig,
};
use tokio::net::{TcpListener, TcpStream};

const HANG_GUARD: Duration = Duration::from_secs(10);
const SHUTDOWN_HANG_GUARD: Duration = Duration::from_secs(30);
const NODE_IDS: [&str; 3] = ["node-1", "node-2", "node-3"];
const PEER_TOKENS: [&str; 3] = ["peer-token-1", "peer-token-2", "peer-token-3"];

fn archive(root: &Path) -> ObjectArchiveStore {
    ObjectArchiveStore::new_checkpoint_for_single_process(
        ObjStore::new(ObjStoreConfig::Local {
            root: root.to_path_buf(),
        })
        .unwrap(),
        CheckpointIdentity::new(
            "rhiza:sql:cluster-a",
            1,
            1,
            LogHash::digest(&[b"rhiza-test-config"]),
            1,
        ),
    )
}

fn node_config(
    data_dir: &Path,
    node_index: usize,
    recorder_addresses: &[SocketAddr],
    service_addresses: &[SocketAddr],
) -> NodeConfig {
    let peers = NODE_IDS
        .iter()
        .enumerate()
        .map(|(index, node_id)| {
            PeerConfig::new_with_log_url(
                *node_id,
                format!("http://{}", recorder_addresses[index]),
                format!("http://{}", service_addresses[index]),
                PEER_TOKENS[index],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    NodeConfig::new(
        "cluster-a",
        NODE_IDS[node_index],
        data_dir.to_path_buf(),
        1,
        1,
        peers,
        "client-token",
    )
    .unwrap()
    .with_execution_profile(ExecutionProfile::Sqlite)
    .unwrap()
}

fn recorder_clients(
    local_node_index: usize,
    recorder_addresses: &[SocketAddr],
) -> Vec<(String, Box<dyn RecorderRpc>)> {
    NODE_IDS
        .iter()
        .enumerate()
        .map(|(index, node_id)| {
            let client = HttpRecorderClient::new(
                format!("http://{}", recorder_addresses[index]),
                NODE_IDS[local_node_index],
                PEER_TOKENS[local_node_index],
            )
            .unwrap();
            (
                (*node_id).to_owned(),
                Box::new(client) as Box<dyn RecorderRpc>,
            )
        })
        .collect()
}

fn file_recorder_clients(root: &Path) -> Vec<(String, Box<dyn RecorderRpc>)> {
    let membership = Membership::new(NODE_IDS).unwrap();
    NODE_IDS
        .iter()
        .map(|node_id| {
            let recorder = RecorderFileStore::new_with_membership(
                root.join(node_id),
                *node_id,
                "rhiza:sql:cluster-a",
                1,
                1,
                membership.clone(),
            )
            .unwrap();
            (
                (*node_id).to_owned(),
                Box::new(recorder) as Box<dyn RecorderRpc>,
            )
        })
        .collect()
}

async fn listeners() -> (Vec<TcpListener>, Vec<SocketAddr>) {
    let mut listeners = Vec::with_capacity(3);
    let mut addresses = Vec::with_capacity(3);
    for _ in 0..3 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        addresses.push(listener.local_addr().unwrap());
        listeners.push(listener);
    }
    (listeners, addresses)
}

#[tokio::test]
async fn immediate_shutdown_cancels_owned_preparation_without_publishing_or_mutating() {
    let root = tempfile::tempdir().unwrap();
    let archive = archive(&root.path().join("archive"));
    archive.initialize_checkpoint().await.unwrap();
    let (mut recorder_listeners, recorder_addresses) = listeners().await;
    let (mut service_listeners, service_addresses) = listeners().await;
    let data_dir = root.path().join("node-1");
    let recorder_address = recorder_addresses[0];
    let service_address = service_addresses[0];
    let node = HaStartupConfig::new(
        node_config(&data_dir, 0, &recorder_addresses, &service_addresses),
        archive,
        DurabilityMode::Sync,
        60_000,
        HaStartupMode::Bootstrap,
    )
    .start(HaServeConfig::new(
        recorder_listeners.remove(0),
        service_listeners.remove(0),
        HaRecorderTransport::Http,
        file_recorder_clients(&root.path().join("recorders")),
        Vec::new(),
    ));

    node.shutdown_with_timeout(Duration::from_millis(100))
        .await
        .unwrap();

    assert!(!data_dir.exists());
    let recorder = TcpListener::bind(recorder_address).await.unwrap();
    let service = TcpListener::bind(service_address).await.unwrap();
    drop((recorder, service));
}

#[tokio::test]
async fn shutdown_request_immediately_closes_ready_handle_admission() {
    let root = tempfile::tempdir().unwrap();
    let archive = archive(&root.path().join("archive"));
    archive.initialize_checkpoint().await.unwrap();
    let (mut recorder_listeners, recorder_addresses) = listeners().await;
    let (mut service_listeners, service_addresses) = listeners().await;
    let node = HaStartupConfig::new(
        node_config(
            &root.path().join("node-1"),
            0,
            &recorder_addresses,
            &service_addresses,
        ),
        archive,
        DurabilityMode::Sync,
        60_000,
        HaStartupMode::Bootstrap,
    )
    .start(HaServeConfig::new(
        recorder_listeners.remove(0),
        service_listeners.remove(0),
        HaRecorderTransport::Http,
        file_recorder_clients(&root.path().join("recorders")),
        Vec::<Arc<dyn LogPeer>>::new(),
    ));
    let handle = tokio::time::timeout(HANG_GUARD, node.ready())
        .await
        .expect("HA node must become ready")
        .unwrap();
    let mut shutdown = Box::pin(node.shutdown());

    let first_shutdown_poll = poll_fn(|context| Poll::Ready(shutdown.as_mut().poll(context))).await;
    assert!(first_shutdown_poll.is_pending());

    let mut put = Box::pin(handle.put("after-shutdown-request", "key", "value"));
    let first_put_poll = poll_fn(|context| Poll::Ready(put.as_mut().poll(context))).await;
    let admission_closed = matches!(first_put_poll, Poll::Ready(Err(Error::Closed)));
    drop(put);
    shutdown.await.unwrap();

    assert!(
        admission_closed,
        "a new operation was admitted after shutdown was requested"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejoin_serves_quarantined_ingress_before_ready_and_shutdown_flushes_then_releases_listeners(
) {
    let root = tempfile::tempdir().unwrap();
    let archive = archive(&root.path().join("archive"));
    archive.initialize_checkpoint().await.unwrap();
    let (mut recorder_listeners, recorder_addresses) = listeners().await;
    let (mut service_listeners, service_addresses) = listeners().await;
    let mut nodes = Vec::with_capacity(3);

    for (node_index, node_id) in NODE_IDS.iter().enumerate() {
        let node = HaStartupConfig::new(
            node_config(
                &root.path().join(node_id),
                node_index,
                &recorder_addresses,
                &service_addresses,
            ),
            archive.clone(),
            DurabilityMode::Periodic {
                interval: Duration::from_secs(60 * 60),
            },
            60_000,
            HaStartupMode::Rejoin,
        );
        let serve = HaServeConfig::new(
            recorder_listeners.remove(0),
            service_listeners.remove(0),
            HaRecorderTransport::Http,
            recorder_clients(node_index, &recorder_addresses),
            Vec::<Arc<dyn LogPeer>>::new(),
        )
        .with_tail_token("tail-token");
        nodes.push(node.start(serve));

        if node_index == 0 {
            let probe = HttpRecorderClient::new(
                format!("http://{}", recorder_addresses[0]),
                NODE_IDS[1],
                PEER_TOKENS[1],
            )
            .unwrap();
            let identity_probe = probe.clone();
            assert_eq!(
                tokio::task::spawn_blocking(move || {
                    identity_probe
                        .recorder_id(&rhiza_quepaxa::RecorderRpcContext::default_timeout())
                })
                .await
                .unwrap()
                .unwrap(),
                NODE_IDS[0]
            );
            let membership = Membership::new(NODE_IDS).unwrap();
            let command = StoredCommand::new(EntryType::Noop, Vec::new());
            let mutation_error = tokio::task::spawn_blocking(move || {
                probe.store_command_for(
                    &rhiza_quepaxa::RecorderRpcContext::default_timeout(),
                    "rhiza:sql:cluster-a".into(),
                    1,
                    1,
                    membership.digest(),
                    command.hash(),
                    command,
                )
            })
            .await
            .unwrap()
            .unwrap_err();
            assert!(mutation_error.to_string().contains("quarantined"));
            assert!(!nodes[0].is_ready());
            tokio::select! {
                biased;
                _ = nodes[0].ready() => panic!("ready completed before recorder quorum"),
                () = std::future::ready(()) => {}
            }
        }
    }

    let handles = tokio::time::timeout(HANG_GUARD, async {
        tokio::try_join!(nodes[0].ready(), nodes[1].ready(), nodes[2].ready())
    })
    .await
    .expect("three-node bootstrap must not hang")
    .unwrap();
    assert!(nodes.iter().all(|node| node.is_ready()));

    // `ready` publishes application admission after the recorder tasks are
    // spawned. Confirm every recorder has crossed its own HTTP startup edge
    // before issuing the first all-recorder mutation; this keeps the test's
    // quarantined-ingress assertion separate from connection warm-up races.
    for (index, address) in recorder_addresses.iter().enumerate() {
        let client =
            HttpRecorderClient::new(format!("http://{address}"), NODE_IDS[0], PEER_TOKENS[0])
                .unwrap();
        let recorder_id = tokio::task::spawn_blocking(move || {
            client.recorder_id(&rhiza_quepaxa::RecorderRpcContext::default_timeout())
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(recorder_id, NODE_IDS[index]);
    }

    let first_handle = handles.0;
    first_handle
        .put("before-shutdown", "key", "value")
        .await
        .unwrap();
    let first_recorder_address = recorder_addresses[0];
    let first_service_address = service_addresses[0];
    let first = nodes.remove(0);
    first.shutdown().await.unwrap();

    let checkpoint = archive.load_checkpoint().await.unwrap().unwrap();
    assert!(
        checkpoint.manifest().tip().index() >= 1,
        "shutdown must flush the final applied tip even when the periodic worker is idle"
    );
    assert!(matches!(
        first_handle.put("after-shutdown", "key", "value").await,
        Err(Error::Closed)
    ));
    assert!(TcpStream::connect(first_recorder_address).await.is_err());
    assert!(TcpStream::connect(first_service_address).await.is_err());
    let rebound_recorder = TcpListener::bind(first_recorder_address).await.unwrap();
    let rebound_service = TcpListener::bind(first_service_address).await.unwrap();
    drop((rebound_recorder, rebound_service));

    for node in nodes {
        node.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn monitor_reports_a_terminal_startup_failure() {
    let root = tempfile::tempdir().unwrap();
    let archive = archive(&root.path().join("archive"));
    archive.initialize_checkpoint().await.unwrap();
    let (mut recorder_listeners, recorder_addresses) = listeners().await;
    let (mut service_listeners, service_addresses) = listeners().await;
    let node = HaStartupConfig::new(
        node_config(
            &root.path().join("node-1"),
            0,
            &recorder_addresses,
            &service_addresses,
        ),
        archive,
        DurabilityMode::Sync,
        60_000,
        HaStartupMode::Bootstrap,
    )
    .start(
        HaServeConfig::new(
            recorder_listeners.remove(0),
            service_listeners.remove(0),
            HaRecorderTransport::Http,
            file_recorder_clients(&root.path().join("recorders")),
            Vec::<Arc<dyn LogPeer>>::new(),
        )
        .with_tail_token(""),
    );

    let error = tokio::time::timeout(HANG_GUARD, node.monitor())
        .await
        .expect("monitor must observe the terminal startup failure")
        .unwrap_err();
    assert!(
        error.to_string().contains("tail") || error.to_string().contains("token"),
        "{error}"
    );
    let _ = node.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn archive_startup_failure_releases_owned_resources() {
    let root = tempfile::tempdir().unwrap();
    let broken_archive_root = root.path().join("broken-archive");
    let broken_archive = archive(&broken_archive_root);
    broken_archive.initialize_checkpoint().await.unwrap();
    let data_dir = root.path().join("node-1");
    let (mut first_recorder_listeners, first_recorder_addresses) = listeners().await;
    let (mut first_service_listeners, first_service_addresses) = listeners().await;
    let failing = HaStartupConfig::new(
        node_config(
            &data_dir,
            0,
            &first_recorder_addresses,
            &first_service_addresses,
        ),
        broken_archive,
        DurabilityMode::Sync,
        60_000,
        HaStartupMode::Bootstrap,
    );
    std::fs::remove_dir_all(&broken_archive_root).unwrap();
    std::fs::write(&broken_archive_root, b"not an object-store directory").unwrap();

    let recorder_address = first_recorder_addresses[0];
    let node = failing.start(HaServeConfig::new(
        first_recorder_listeners.remove(0),
        first_service_listeners.remove(0),
        HaRecorderTransport::Http,
        file_recorder_clients(&root.path().join("failing-recorders")),
        Vec::<Arc<dyn LogPeer>>::new(),
    ));
    let error = tokio::time::timeout(HANG_GUARD, node.monitor())
        .await
        .expect("coordinator failure must be terminal")
        .unwrap_err();
    assert!(
        error.to_string().contains("archive")
            || error.to_string().contains("checkpoint")
            || error.to_string().contains("directory"),
        "{error}"
    );
    let _ = node.shutdown().await;
    assert!(TcpStream::connect(recorder_address).await.is_err());
    drop(TcpListener::bind(recorder_address).await.unwrap());
    let lock_path = data_dir.join(".node.lock");
    if lock_path.exists() {
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(lock_path)
            .unwrap();
        lock.try_lock()
            .expect("startup failure must release the runtime data lock");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_deadline_detaches_unfinished_startup_io_without_late_publication() {
    let root = tempfile::tempdir().unwrap();
    let archive = archive(&root.path().join("archive"));
    archive.initialize_checkpoint().await.unwrap();
    let (mut recorder_listeners, recorder_addresses) = listeners().await;
    let (mut service_listeners, service_addresses) = listeners().await;
    let prepared = HaStartupConfig::new(
        node_config(
            &root.path().join("node-1"),
            0,
            &recorder_addresses,
            &service_addresses,
        ),
        archive,
        DurabilityMode::Sync,
        60_000,
        HaStartupMode::Bootstrap,
    );
    let (recorders, startup_blocked, release) =
        blocking_file_recorder_clients(&root.path().join("recorders"));
    let recorder_address = recorder_addresses[0];
    let service_address = service_addresses[0];
    let node = prepared.start(HaServeConfig::new(
        recorder_listeners.remove(0),
        service_listeners.remove(0),
        HaRecorderTransport::Http,
        recorders,
        Vec::<Arc<dyn LogPeer>>::new(),
    ));

    tokio::task::spawn_blocking(move || startup_blocked.recv().unwrap())
        .await
        .unwrap();
    let shutdown_error = tokio::time::timeout(
        Duration::from_secs(1),
        node.shutdown_with_timeout(Duration::from_millis(100)),
    )
    .await
    .expect("shutdown must return at its own deadline without waiting for startup I/O")
    .unwrap_err();
    assert!(
        matches!(
            shutdown_error,
            HaNodeError::ShutdownDeadlineExceeded {
                phase: rhizadb::HaShutdownPhase::Supervisor,
                ..
            }
        ),
        "unexpected shutdown error: {shutdown_error:?}"
    );
    assert!(TcpStream::connect(recorder_address).await.is_err());

    release.release();
    assert!(TcpStream::connect(recorder_address).await.is_err());
    let rebound = TcpListener::bind(recorder_address).await.unwrap();
    let rebound_service = TcpListener::bind(service_address).await.unwrap();
    drop((rebound, rebound_service));

    let lock_path = root.path().join("node-1/.node.lock");
    tokio::time::timeout(SHUTDOWN_HANG_GUARD, async {
        loop {
            let lock = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&lock_path)
                .unwrap();
            if lock.try_lock().is_ok() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled runtime startup must eventually release the data lock");
}

type BlockingRecorderClients = (
    Vec<(String, Box<dyn RecorderRpc>)>,
    mpsc::Receiver<()>,
    StartupRelease,
);

fn blocking_file_recorder_clients(root: &Path) -> BlockingRecorderClients {
    let membership = Membership::new(NODE_IDS).unwrap();
    let (started, startup_blocked) = mpsc::sync_channel(3);
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let recorders = NODE_IDS
        .iter()
        .map(|node_id| {
            let recorder = RecorderFileStore::new_with_membership(
                root.join(node_id),
                *node_id,
                "rhiza:sql:cluster-a",
                1,
                1,
                membership.clone(),
            )
            .unwrap();
            (
                (*node_id).to_owned(),
                Box::new(BlockingStartupRecorder {
                    recorder,
                    started: started.clone(),
                    gate: gate.clone(),
                }) as Box<dyn RecorderRpc>,
            )
        })
        .collect();
    (recorders, startup_blocked, StartupRelease(gate))
}

struct BlockingStartupRecorder {
    recorder: RecorderFileStore,
    started: mpsc::SyncSender<()>,
    gate: Arc<(Mutex<bool>, Condvar)>,
}

impl RecorderRpc for BlockingStartupRecorder {
    fn recorder_id(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
    ) -> rhiza_quepaxa::Result<String> {
        RecorderRpc::recorder_id(&self.recorder, context)
    }

    fn inspect_record_summary(
        &self,
        context: &rhiza_quepaxa::RecorderRpcContext,
        slot: u64,
    ) -> rhiza_quepaxa::Result<Option<RecordSummary>> {
        // The test needs only the first blocked startup RPC. Detached peers
        // may enter after its receiver is intentionally dropped.
        let _ = self.started.send(());
        let (released, condition) = &*self.gate;
        let mut released = released.lock().unwrap();
        while !*released {
            released = condition.wait(released).unwrap();
        }
        RecorderRpc::inspect_record_summary(&self.recorder, context, slot)
    }
}

struct StartupRelease(Arc<(Mutex<bool>, Condvar)>);

impl StartupRelease {
    fn release(&self) {
        let (released, condition) = &*self.0;
        *released.lock().unwrap() = true;
        condition.notify_all();
    }
}

impl Drop for StartupRelease {
    fn drop(&mut self) {
        self.release();
    }
}
