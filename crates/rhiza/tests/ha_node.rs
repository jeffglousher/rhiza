use std::{
    net::SocketAddr,
    path::Path,
    sync::{mpsc, Arc, Condvar, Mutex},
    time::Duration,
};

use rhiza_archive::{CheckpointIdentity, ObjectArchiveStore};
use rhiza_core::{EntryType, StoredCommand};
use rhiza_node::HttpRecorderClient;
use rhiza_obj_store::{ObjStore, ObjStoreConfig};
use rhiza_quepaxa::{Membership, RecordSummary, RecorderFileStore, RecorderRpc};
use rhizadb::{
    DurabilityMode, Error, ExecutionProfile, HaRecorderTransport, HaServeConfig, HaStartupConfig,
    HaStartupMode, LogPeer, NodeConfig, PeerConfig,
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
        CheckpointIdentity::new("rhiza:sql:cluster-a", 1, 1, 1),
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejoin_serves_quarantined_ingress_before_ready_and_shutdown_flushes_then_releases_listeners(
) {
    let root = tempfile::tempdir().unwrap();
    let archive = archive(&root.path().join("archive"));
    archive.initialize_checkpoint().await.unwrap();
    let (mut recorder_listeners, recorder_addresses) = listeners().await;
    let (mut service_listeners, service_addresses) = listeners().await;
    let mut nodes = Vec::with_capacity(3);

    for node_index in 0..3 {
        let prepared = HaStartupConfig::new(
            node_config(
                &root.path().join(NODE_IDS[node_index]),
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
        )
        .prepare()
        .await
        .unwrap();
        let serve = HaServeConfig::new(
            recorder_listeners.remove(0),
            service_listeners.remove(0),
            HaRecorderTransport::Http,
            recorder_clients(node_index, &recorder_addresses),
            Vec::<Arc<dyn LogPeer>>::new(),
        )
        .with_tail_token("tail-token");
        nodes.push(prepared.start(serve).await.unwrap());

        if node_index == 0 {
            let probe = HttpRecorderClient::new(
                format!("http://{}", recorder_addresses[0]),
                NODE_IDS[1],
                PEER_TOKENS[1],
            )
            .unwrap();
            let identity_probe = probe.clone();
            assert_eq!(
                tokio::task::spawn_blocking(move || identity_probe.recorder_id())
                    .await
                    .unwrap()
                    .unwrap(),
                NODE_IDS[0]
            );
            let membership = Membership::new(NODE_IDS).unwrap();
            let command = StoredCommand::new(EntryType::Noop, Vec::new());
            let mutation_error = tokio::task::spawn_blocking(move || {
                probe.store_command_for(
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
    )
    .prepare()
    .await
    .unwrap();
    let node = prepared
        .start(
            HaServeConfig::new(
                recorder_listeners.remove(0),
                service_listeners.remove(0),
                HaRecorderTransport::Http,
                file_recorder_clients(&root.path().join("recorders")),
                Vec::<Arc<dyn LogPeer>>::new(),
            )
            .with_tail_token(""),
        )
        .await
        .unwrap();

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
async fn coordinator_open_failure_cleans_the_runtime_and_releases_recorder_ingress() {
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
    )
    .prepare()
    .await
    .unwrap();
    std::fs::remove_dir_all(&broken_archive_root).unwrap();
    std::fs::write(&broken_archive_root, b"not an object-store directory").unwrap();

    let recorder_address = first_recorder_addresses[0];
    let node = failing
        .start(HaServeConfig::new(
            first_recorder_listeners.remove(0),
            first_service_listeners.remove(0),
            HaRecorderTransport::Http,
            file_recorder_clients(&root.path().join("failing-recorders")),
            Vec::<Arc<dyn LogPeer>>::new(),
        ))
        .await
        .unwrap();
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
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(data_dir.join(".node.lock"))
        .unwrap();
    lock.try_lock()
        .expect("coordinator failure must release the runtime data lock");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_bounds_a_blocked_runtime_open_and_releases_recorder_ingress() {
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
    )
    .prepare()
    .await
    .unwrap();
    let (recorders, startup_blocked, release) =
        blocking_file_recorder_clients(&root.path().join("recorders"));
    let recorder_address = recorder_addresses[0];
    let node = prepared
        .start(HaServeConfig::new(
            recorder_listeners.remove(0),
            service_listeners.remove(0),
            HaRecorderTransport::Http,
            recorders,
            Vec::<Arc<dyn LogPeer>>::new(),
        ))
        .await
        .unwrap();

    tokio::task::spawn_blocking(move || startup_blocked.recv().unwrap())
        .await
        .unwrap();
    let shutdown_error = tokio::time::timeout(SHUTDOWN_HANG_GUARD, node.shutdown())
        .await
        .expect("shutdown must honor the HA shutdown budget")
        .unwrap_err();
    assert!(
        shutdown_error.to_string().contains("pending consensus")
            || shutdown_error.to_string().contains("deadline"),
        "{shutdown_error}"
    );
    assert!(TcpStream::connect(recorder_address).await.is_err());
    let rebound = TcpListener::bind(recorder_address).await.unwrap();
    drop(rebound);

    release.release();
}

fn blocking_file_recorder_clients(
    root: &Path,
) -> (
    Vec<(String, Box<dyn RecorderRpc>)>,
    mpsc::Receiver<()>,
    StartupRelease,
) {
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
    fn recorder_id(&self) -> rhiza_quepaxa::Result<String> {
        self.recorder.recorder_id()
    }

    fn inspect_record_summary(&self, slot: u64) -> rhiza_quepaxa::Result<Option<RecordSummary>> {
        self.started.send(()).unwrap();
        let (released, condition) = &*self.gate;
        let mut released = released.lock().unwrap();
        while !*released {
            released = condition.wait(released).unwrap();
        }
        self.recorder.inspect_record_summary(slot)
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
