#[cfg(test)]
use std::path::Path;
use std::{
    env, fmt, fs,
    io::{self, Read},
    path::PathBuf,
    process,
    sync::Arc,
    time::Duration,
};

use reqwest::{header, Method, RequestBuilder, Response};
use rhiza_archive::{CheckpointIdentity, CheckpointTip, GcPlan, GcPolicy, ObjectArchiveStore};
use rhiza_client::RhizaClient;
use rhiza_core::{
    ConfigChange, ConfigurationState, ExecutionProfile, LogAnchor, LogEntry, LogHash, StoredCommand,
};
use rhiza_node::{
    effective_cluster_id, execution_profile_compiled, install_successor_recorder, node_router,
    node_router_with_admin_and_tasks, recorder_router_for_generation, serve_recorder_tcp,
    serve_recorder_tcp_tls, validate_recorder_tcp_endpoint, AdminActivateRequest,
    AdminActivateResponse, AdminCompactRequest, AdminCompactResponse, AdminConfig,
    AdminErrorResponse, AdminInstallSuccessorRequest, AdminInstallSuccessorResponse,
    AdminStatusResponse, AdminStopRequest, AdminStopResponse, AdminSuccessorBundle,
    AdminTaskTracker, CertifiedTailErrorResponse, CertifiedTailResponse, CheckpointCoordinator,
    DurabilityMode, HttpLogPeer, HttpRecorderClient, LogPeer, NodeConfig, NodeError, NodeRuntime,
    PeerConfig, ReadConsistency, RecorderIngressLifecycle, RecorderTlsClientConfig,
    RecorderTlsServerConfig, StopInformation, TcpPostcardRecorderClient, ADMIN_ACTIVATE_PATH,
    ADMIN_COMPACT_PATH, ADMIN_INSTALL_SUCCESSOR_PATH, ADMIN_STATUS_PATH, ADMIN_STOP_PATH,
    CERTIFIED_TAIL_PATH, LIVEZ_PATH, PROTOCOL_VERSION, READYZ_PATH, RECOVERY_GENERATION_HEADER,
    TAIL_CLUSTER_ID_HEADER, TAIL_CONFIG_ID_HEADER, TAIL_EPOCH_HEADER,
    TAIL_MEMBERSHIP_DIGEST_HEADER, TAIL_PROTOCOL_VERSION, TAIL_VERSION_HEADER, VERSION_HEADER,
};
#[cfg(feature = "sql")]
use rhiza_node::{
    run_e2e, E2eConfig, ReadRequest, ReadResponse, SqlExecuteRequest, SqlExecuteResponse,
    SqlQueryRequest, SqlQueryResponse, WriteRequest, WriteResponse,
};
#[cfg(feature = "recorder-postcard-rpc")]
use rhiza_node::{
    serve_recorder_postcard_rpc, serve_recorder_postcard_rpc_tls,
    RecorderPostcardRpcTlsClientConfig, RecorderPostcardRpcTlsServerConfig,
    TcpPostcardRpcRecorderClient,
};
#[cfg(feature = "graph")]
use rhiza_node::{
    GraphDeleteDocumentRequest, GraphGetDocumentRequest, GraphGetDocumentResponse,
    GraphMutationResponse, GraphPutDocumentRequest, GraphQueryRequest, GraphQueryResponse,
    GraphQueryStatementDto, GraphValueDto,
};
#[cfg(feature = "kv")]
use rhiza_node::{
    KvBatchMemberRequest, KvBatchRequest, KvBatchResponse, KvDeleteRequest, KvGetRequest,
    KvGetResponse, KvMutationResponse, KvPutRequest, KvScanRequest, KvScanResponse,
    MAX_KV_SCAN_ROWS,
};
use rhiza_obj_store::{ObjStore, ObjStoreConfig};
use rhiza_quepaxa::{
    DecisionProof, Membership, RecorderFileStore, RecorderPreflight, RecorderRpc,
    ThreeNodeConsensus,
};
#[cfg(feature = "sql")]
use rhiza_sql::{SqlStatement, SqlValue};
use rhizadb::{
    HaCertifiedTailError, HaCertifiedTailSource, HaNode, HaNodeError, HaPredecessor,
    HaRecorderTransport, HaServeConfig, HaStartupConfig, HaStartupMode, HaSuccessorNode,
    HaSuccessorPrestageConfig,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

type RecorderClients = Vec<(String, Box<dyn RecorderRpc>)>;

const MIN_GCS_CHECKPOINT_LEASE_MS: u64 = 60_000;

#[tokio::main]
async fn main() {
    process::exit(run(env::args().skip(1)).await);
}

async fn run(args: impl IntoIterator<Item = String>) -> i32 {
    let command = match parse_command(args) {
        Ok(command) => command,
        Err(error) => {
            eprintln!("{error}");
            usage();
            return 2;
        }
    };

    match command {
        Command::Status(args) => match request_health(&args).await {
            Ok(()) => {
                println!("ok");
                0
            }
            Err(error) => fail("status", error),
        },
        #[cfg(feature = "sql")]
        Command::E2e(config) => match run_e2e(config).await {
            Ok(report) => {
                println!(
                    "rhiza e2e ok: applied_index={} restored_value={} objects={}",
                    report.applied_index,
                    report.restored_value,
                    report.object_keys.len()
                );
                0
            }
            Err(error) => fail("e2e", error),
        },
        Command::Serve(config) => match serve(*config).await {
            Ok(()) => 0,
            Err(error) => fail("serve", error),
        },
        Command::PrestageServe(config) => match run_prestage_serve(*config).await {
            Ok(()) => 0,
            Err(error) => fail("prestage serve", error),
        },
        Command::InitCheckpoint(config) => match run_init_checkpoint(config).await {
            Ok(tip) => {
                println!("checkpoint initialized: durable_tip={}", tip.index());
                0
            }
            Err(error) => fail("init-checkpoint", error),
        },
        Command::RollCheckpoint(config) => match run_roll_checkpoint(config).await {
            Ok((old, new)) => {
                println!(
                    "checkpoint rolled: old_durable_tip={} new_durable_tip={}",
                    old.index(),
                    new.index()
                );
                0
            }
            Err(error) => fail("roll-checkpoint", error),
        },
        Command::CheckpointInspect(config) => match run_checkpoint_inspect(config).await {
            Ok(json) => {
                println!("{json}");
                0
            }
            Err(error) => fail("checkpoint inspect", error),
        },
        Command::CheckpointCompact(config) => match run_checkpoint_compact(*config).await {
            Ok(json) => {
                println!("{json}");
                0
            }
            Err(error) => fail("checkpoint compact", error),
        },
        Command::ValidateConfigBundle(config_id) => match config_id
            .map(Ok)
            .unwrap_or_else(validate_config_bundle_stdin)
        {
            Ok(config_id) => {
                println!("{{\"config_id\":{config_id}}}");
                0
            }
            Err(error) => fail("validate-config-bundle", error),
        },
        Command::GcPlan(config) => match run_gc_plan(config).await {
            Ok(json) => {
                println!("{json}");
                0
            }
            Err(error) => fail("gc plan", error),
        },
        Command::GcInspect(config) => match run_gc_inspect(config).await {
            Ok(json) => {
                println!("{json}");
                0
            }
            Err(error) => fail("gc inspect", error),
        },
        Command::GcApply(config) => match run_gc_apply(config).await {
            Ok(json) => {
                println!("{json}");
                0
            }
            Err(error) => fail("gc apply", error),
        },
        Command::MembershipStatus(config) => match run_membership_status(*config).await {
            Ok(json) => {
                println!("{json}");
                0
            }
            Err(error) => fail("membership status", error),
        },
        Command::MembershipStop(config) => match run_membership_stop(*config).await {
            Ok(json) => {
                println!("{json}");
                0
            }
            Err(error) => fail("membership stop", error),
        },
        Command::MembershipInstallSuccessor(config) => {
            match run_membership_install_successor(*config).await {
                Ok(json) => {
                    println!("{json}");
                    0
                }
                Err(error) => fail("membership install-successor", error),
            }
        }
        Command::MembershipActivate(config) => match run_membership_activate(*config).await {
            Ok(json) => {
                println!("{json}");
                0
            }
            Err(error) => fail("membership activate", error),
        },
        #[cfg(feature = "sql")]
        Command::Write(args) => match request_write(&args).await {
            Ok(response) => {
                println!(
                    "applied_index={} hash={}",
                    response.applied_index,
                    response.hash.to_hex()
                );
                0
            }
            Err(error) => fail("write", error),
        },
        #[cfg(feature = "sql")]
        Command::Read(args) => match request_read(&args).await {
            Ok(response) => finish_read(&args, response),
            Err(error) => fail("read", error),
        },
        #[cfg(feature = "sql")]
        Command::SqlExecute(args) => match request_sql_execute(&args).await {
            Ok(response) => match serde_json::to_string(&response) {
                Ok(json) => {
                    println!("{json}");
                    0
                }
                Err(error) => fail("sql execute", error.to_string()),
            },
            Err(error) => fail("sql execute", error),
        },
        #[cfg(feature = "sql")]
        Command::SqlQuery(args) => match request_sql_query(&args).await {
            Ok(response) => match serde_json::to_string(&response) {
                Ok(json) => {
                    println!("{json}");
                    0
                }
                Err(error) => fail("sql query", error.to_string()),
            },
            Err(error) => fail("sql query", error),
        },
        #[cfg(feature = "graph")]
        Command::GraphQuery(args) => match request_graph_query(&args).await {
            Ok(response) => match serde_json::to_string(&response) {
                Ok(json) => {
                    println!("{json}");
                    0
                }
                Err(error) => fail("graph query", error.to_string()),
            },
            Err(error) => fail("graph query", error),
        },
        #[cfg(feature = "graph")]
        Command::GraphPutDocument(args) => match request_graph_put_document(&args).await {
            Ok(response) => match serde_json::to_string(&response) {
                Ok(json) => {
                    println!("{json}");
                    0
                }
                Err(error) => fail("graph put-document", error.to_string()),
            },
            Err(error) => fail("graph put-document", error),
        },
        #[cfg(feature = "graph")]
        Command::GraphDeleteDocument(args) => match request_graph_delete_document(&args).await {
            Ok(response) => match serde_json::to_string(&response) {
                Ok(json) => {
                    println!("{json}");
                    0
                }
                Err(error) => fail("graph delete-document", error.to_string()),
            },
            Err(error) => fail("graph delete-document", error),
        },
        #[cfg(feature = "graph")]
        Command::GraphGetDocument(args) => match request_graph_get_document(&args).await {
            Ok(response) => match serde_json::to_string(&response) {
                Ok(json) => {
                    println!("{json}");
                    0
                }
                Err(error) => fail("graph get-document", error.to_string()),
            },
            Err(error) => fail("graph get-document", error),
        },
        #[cfg(feature = "kv")]
        Command::KvGet(args) => match request_kv_get(&args).await {
            Ok(response) => match serde_json::to_string(&response) {
                Ok(json) => {
                    println!("{json}");
                    0
                }
                Err(error) => fail("kv get", error.to_string()),
            },
            Err(error) => fail("kv get", error),
        },
        #[cfg(feature = "kv")]
        Command::KvScan(args) => match request_kv_scan(&args).await {
            Ok(response) => match serde_json::to_string(&response) {
                Ok(json) => {
                    println!("{json}");
                    0
                }
                Err(error) => fail("kv scan", error.to_string()),
            },
            Err(error) => fail("kv scan", error),
        },
        #[cfg(feature = "kv")]
        Command::KvPut(args) => match request_kv_put(&args).await {
            Ok(response) => match serde_json::to_string(&response) {
                Ok(json) => {
                    println!("{json}");
                    0
                }
                Err(error) => fail("kv put", error.to_string()),
            },
            Err(error) => fail("kv put", error),
        },
        #[cfg(feature = "kv")]
        Command::KvDelete(args) => match request_kv_delete(&args).await {
            Ok(response) => match serde_json::to_string(&response) {
                Ok(json) => {
                    println!("{json}");
                    0
                }
                Err(error) => fail("kv delete", error.to_string()),
            },
            Err(error) => fail("kv delete", error),
        },
        #[cfg(feature = "kv")]
        Command::KvBatch(args) => match request_kv_batch(&args).await {
            Ok(response) => match serde_json::to_string(&response) {
                Ok(json) => {
                    println!("{json}");
                    0
                }
                Err(error) => fail("kv batch", error.to_string()),
            },
            Err(error) => fail("kv batch", error),
        },
        Command::Health(args) => match request_health(&args).await {
            Ok(()) => {
                println!("ok");
                0
            }
            Err(error) => fail("health", error),
        },
    }
}

enum Command {
    Status(HealthArgs),
    #[cfg(feature = "sql")]
    E2e(E2eConfig),
    Serve(Box<ServeConfig>),
    PrestageServe(Box<PrestageServeConfig>),
    InitCheckpoint(CheckpointCommandConfig),
    RollCheckpoint(RollCheckpointConfig),
    CheckpointInspect(CheckpointCommandConfig),
    CheckpointCompact(Box<AdminCommandConfig>),
    ValidateConfigBundle(Option<u64>),
    GcPlan(GcPlanConfig),
    GcInspect(GcInspectConfig),
    GcApply(GcInspectConfig),
    MembershipStatus(Box<AdminCommandConfig>),
    MembershipStop(Box<AdminCommandConfig>),
    MembershipInstallSuccessor(Box<AdminCommandConfig>),
    MembershipActivate(Box<AdminCommandConfig>),
    #[cfg(feature = "sql")]
    Write(WriteArgs),
    #[cfg(feature = "sql")]
    Read(ReadArgs),
    #[cfg(feature = "sql")]
    SqlExecute(SqlExecuteArgs),
    #[cfg(feature = "sql")]
    SqlQuery(SqlQueryArgs),
    #[cfg(feature = "graph")]
    GraphQuery(GraphQueryArgs),
    #[cfg(feature = "graph")]
    GraphPutDocument(GraphPutDocumentArgs),
    #[cfg(feature = "graph")]
    GraphDeleteDocument(GraphDeleteDocumentArgs),
    #[cfg(feature = "graph")]
    GraphGetDocument(GraphGetDocumentArgs),
    #[cfg(feature = "kv")]
    KvGet(KvGetArgs),
    #[cfg(feature = "kv")]
    KvScan(KvScanArgs),
    #[cfg(feature = "kv")]
    KvPut(KvPutArgs),
    #[cfg(feature = "kv")]
    KvDelete(KvDeleteArgs),
    #[cfg(feature = "kv")]
    KvBatch(KvBatchArgs),
    Health(HealthArgs),
}

#[cfg(feature = "sql")]
struct WriteArgs {
    urls: Vec<String>,
    token: String,
    request_id: String,
    key: String,
    value: String,
}

#[cfg(feature = "sql")]
struct ReadArgs {
    urls: Vec<String>,
    token: String,
    key: String,
    consistency: Option<ReadConsistency>,
    expect: Option<String>,
}

#[cfg(feature = "sql")]
struct SqlExecuteArgs {
    urls: Vec<String>,
    token: String,
    request_id: String,
    statement: SqlStatement,
}

#[cfg(feature = "sql")]
struct SqlQueryArgs {
    urls: Vec<String>,
    token: String,
    statement: SqlStatement,
    consistency: Option<ReadConsistency>,
    max_rows: Option<u32>,
}

#[cfg(feature = "graph")]
struct GraphQueryArgs {
    urls: Vec<String>,
    token: String,
    statement: GraphQueryStatementDto,
    consistency: Option<ReadConsistency>,
    max_rows: Option<u32>,
}

#[cfg(feature = "graph")]
struct GraphPutDocumentArgs {
    urls: Vec<String>,
    token: String,
    request: GraphPutDocumentRequest,
}

#[cfg(feature = "graph")]
struct GraphDeleteDocumentArgs {
    urls: Vec<String>,
    token: String,
    request: GraphDeleteDocumentRequest,
}

#[cfg(feature = "graph")]
struct GraphGetDocumentArgs {
    urls: Vec<String>,
    token: String,
    request: GraphGetDocumentRequest,
}

#[cfg(feature = "kv")]
struct KvGetArgs {
    urls: Vec<String>,
    token: String,
    request: KvGetRequest,
}

#[cfg(feature = "kv")]
struct KvScanArgs {
    urls: Vec<String>,
    token: String,
    request: KvScanRequest,
}

#[cfg(feature = "kv")]
struct KvPutArgs {
    urls: Vec<String>,
    token: String,
    request: KvPutRequest,
}

#[cfg(feature = "kv")]
struct KvDeleteArgs {
    urls: Vec<String>,
    token: String,
    request: KvDeleteRequest,
}

#[cfg(feature = "kv")]
struct KvBatchArgs {
    urls: Vec<String>,
    token: String,
    request: KvBatchRequest,
}

struct HealthArgs {
    url: String,
    ready: bool,
}

#[derive(Clone)]
struct AdminClientConfig {
    url: String,
    token: String,
}

const ADMIN_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const ADMIN_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const HEALTH_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const HEALTH_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const SERVE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(25);

impl fmt::Debug for AdminClientConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminClientConfig")
            .field("url", &self.url)
            .field("token", &"[redacted]")
            .finish()
    }
}

#[derive(Clone, Debug)]
enum AdminTarget {
    Live(AdminClientConfig),
    Offline,
}

#[derive(Clone, Debug)]
struct AdminCommandConfig {
    target: AdminTarget,
    serve: Option<Box<ServeConfig>>,
    operation_id: Option<String>,
    successor: Option<ConfigurationBundle>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigurationBundleDocument {
    config_id: u64,
    members: Vec<MemberDocument>,
    #[serde(default)]
    predecessor: Option<PredecessorDocument>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct MemberDocument {
    node_id: String,
    url: String,
    #[serde(default)]
    log_url: Option<String>,
    #[serde(default)]
    recorder_tcp_addr: Option<String>,
    #[serde(default)]
    recorder_tls_server_name: Option<String>,
    token: String,
}

#[derive(Clone, Debug)]
struct RecorderTcpPeer {
    address: String,
    tls_server_name: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PredecessorDocument {
    members: Vec<String>,
    stop_entry: LogEntry,
    stop_proof: DecisionProof,
}

#[derive(Clone)]
struct ConfigurationBundle {
    config_id: u64,
    peers: Vec<PeerConfig>,
    recorder_tcp_peers: Vec<Option<RecorderTcpPeer>>,
    membership: Membership,
    configuration_state: ConfigurationState,
    predecessor: Option<PredecessorConfiguration>,
}

impl fmt::Debug for ConfigurationBundle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConfigurationBundle")
            .field("config_id", &self.config_id)
            .field("peers", &self.peers)
            .field("recorder_tcp_peers", &self.recorder_tcp_peers)
            .field("membership", &self.membership.members())
            .field("configuration_state", &self.configuration_state)
            .field("predecessor", &self.predecessor)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RecorderTransport {
    Http,
    TcpPostcard,
    TcpTlsPostcard,
    #[cfg(feature = "recorder-postcard-rpc")]
    TcpPostcardRpc,
    #[cfg(feature = "recorder-postcard-rpc")]
    TcpTlsPostcardRpc,
}

impl RecorderTransport {
    fn is_tcp(self) -> bool {
        !matches!(self, Self::Http)
    }

    fn is_tls(self) -> bool {
        match self {
            Self::TcpTlsPostcard => true,
            #[cfg(feature = "recorder-postcard-rpc")]
            Self::TcpTlsPostcardRpc => true,
            _ => false,
        }
    }

    fn selector(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::TcpPostcard | Self::TcpTlsPostcard => "tcp-postcard",
            #[cfg(feature = "recorder-postcard-rpc")]
            Self::TcpPostcardRpc | Self::TcpTlsPostcardRpc => "tcp-postcard-rpc",
        }
    }
}

#[derive(Clone, Debug)]
struct RecorderTcpConfig {
    listen: String,
    tls: Option<RecorderTlsFiles>,
}

#[derive(Clone, Debug)]
struct RecorderTlsFiles {
    certificate: PathBuf,
    private_key: PathBuf,
    ca_bundle: PathBuf,
}

#[derive(Clone, Debug)]
struct PredecessorConfiguration {
    membership: Membership,
    stop: StopInformation,
}

impl ConfigurationBundle {
    fn require_predecessor(&self) -> Result<&PredecessorConfiguration, String> {
        self.predecessor
            .as_ref()
            .ok_or_else(|| "configuration bundle is missing predecessor transition material".into())
    }
}

#[derive(Clone)]
struct ServeConfig {
    execution_profile: ExecutionProfile,
    logical_cluster_id: String,
    cluster_id: String,
    node_id: String,
    data_dir: PathBuf,
    epoch: u64,
    bundle: ConfigurationBundle,
    client_token: String,
    admin_token: Option<String>,
    tail_token: Option<String>,
    client_listen: String,
    recorder_listen: String,
    recorder_transport: RecorderTransport,
    recorder_tcp: Option<RecorderTcpConfig>,
    recovery_generation: u64,
    remote: Option<RemoteCheckpointConfig>,
}

impl fmt::Debug for ServeConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServeConfig")
            .field("execution_profile", &self.execution_profile)
            .field("logical_cluster_id", &self.logical_cluster_id)
            .field("cluster_id", &self.cluster_id)
            .field("node_id", &self.node_id)
            .field("data_dir", &self.data_dir)
            .field("epoch", &self.epoch)
            .field("configuration", &self.bundle)
            .field("client_token", &"[redacted]")
            .field(
                "admin_token",
                &self.admin_token.as_ref().map(|_| "[redacted]"),
            )
            .field(
                "tail_token",
                &self.tail_token.as_ref().map(|_| "[redacted]"),
            )
            .field("client_listen", &self.client_listen)
            .field("recorder_listen", &self.recorder_listen)
            .field("recorder_transport", &self.recorder_transport)
            .field("recorder_tcp", &self.recorder_tcp)
            .field("recovery_generation", &self.recovery_generation)
            .field(
                "object_store",
                &self.remote.as_ref().map(RemoteCheckpointConfig::provider),
            )
            .finish()
    }
}

#[derive(Clone)]
struct RemoteCheckpointConfig {
    object_store: ObjStoreConfig,
    durability: DurabilityMode,
    lease_duration_ms: u64,
    startup: StartupMode,
}

struct PrestageServeConfig {
    target: ServeConfig,
    source_bundle: ConfigurationBundle,
    source_archive: ObjectArchiveStore,
    prestage_dir: PathBuf,
    transition_bundle_file: PathBuf,
    tail_token: String,
}

impl PrestageServeConfig {
    fn from_env() -> Result<Self, String> {
        let target = ServeConfig::from_env()?;
        let remote = target
            .remote
            .as_ref()
            .ok_or_else(|| "prestage serve requires RHIZA_OBJECT_STORE".to_string())?;
        if remote.startup != StartupMode::Rejoin {
            return Err("prestage serve requires RHIZA_STARTUP_MODE=rejoin".into());
        }
        if target.bundle.predecessor.is_some() {
            return Err("prestage serve requires an unbound successor draft bundle".into());
        }
        let source_bundle_file = env::var("RHIZA_PRESTAGE_SOURCE_BUNDLE_FILE")
            .map_err(|_| "RHIZA_PRESTAGE_SOURCE_BUNDLE_FILE is required".to_string())?;
        let source_bundle =
            parse_configuration_bundle(&fs::read_to_string(&source_bundle_file).map_err(
                |error| format!("cannot read RHIZA_PRESTAGE_SOURCE_BUNDLE_FILE: {error}"),
            )?)?;
        if source_bundle.predecessor.is_some()
            || source_bundle
                .config_id
                .checked_add(1)
                .is_none_or(|next| next != target.bundle.config_id)
        {
            return Err(
                "prestage source must be Active(S) and target draft must be exactly S+1".into(),
            );
        }
        let tail_token = target
            .tail_token
            .clone()
            .ok_or_else(|| "prestage serve requires RHIZA_TAIL_TOKEN".to_string())?;
        let transition_bundle_file = PathBuf::from(
            env::var("RHIZA_PRESTAGE_TRANSITION_BUNDLE_FILE")
                .map_err(|_| "RHIZA_PRESTAGE_TRANSITION_BUNDLE_FILE is required".to_string())?,
        );
        let prestage_dir = env::var("RHIZA_PRESTAGE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| target.data_dir.with_extension("prestage"));
        let store = open_object_store(&remote.object_store)?;
        if !store.supports_strong_cross_process_cas() {
            return Err("prestage serve requires strong cross-process compare-and-swap".into());
        }
        let source_archive = ObjectArchiveStore::new_checkpoint(
            store,
            CheckpointIdentity::new(
                target.cluster_id.clone(),
                target.epoch,
                source_bundle.config_id,
                source_bundle.membership.digest(),
                target.recovery_generation,
            ),
        )
        .map_err(|error| error.to_string())?;
        Ok(Self {
            target,
            source_bundle,
            source_archive,
            prestage_dir,
            transition_bundle_file,
            tail_token,
        })
    }
}

impl RemoteCheckpointConfig {
    fn provider(&self) -> &'static str {
        match self.object_store {
            ObjStoreConfig::Local { .. } => "local",
            ObjStoreConfig::S3 { .. } => "s3",
            ObjStoreConfig::Gcs { .. } => "gcs",
            ObjStoreConfig::AzureBlob { .. } => "azure",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StartupMode {
    Bootstrap,
    Rejoin,
    Disaster,
}

#[derive(Clone)]
struct CheckpointCommandConfig {
    cluster_id: String,
    epoch: u64,
    config_id: u64,
    config_digest: LogHash,
    recovery_generation: u64,
    object_store: ObjStoreConfig,
}

impl CheckpointCommandConfig {
    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self, String> {
        let execution_profile = execution_profile(&mut lookup)?;
        let logical_cluster_id = required_env(&mut lookup, "RHIZA_CLUSTER_ID")?;
        let cluster_id = effective_cluster_id(execution_profile, &logical_cluster_id)
            .map_err(|error| error.to_string())?;
        let epoch = positive_env(&mut lookup, "RHIZA_EPOCH")?;
        let bundle = load_configuration_bundle(&mut lookup, |path| fs::read_to_string(path))?;
        let config_id = bundle.config_id;
        let config_digest = bundle.membership.digest();
        let recovery_generation = positive_env(&mut lookup, "RHIZA_RECOVERY_GENERATION")?;
        let mode = required_env(&mut lookup, "RHIZA_OBJECT_STORE")?;
        let object_store = parse_object_store_with_lookup(&mode, false, &mut lookup)?;
        Ok(Self {
            cluster_id,
            epoch,
            config_id,
            config_digest,
            recovery_generation,
            object_store,
        })
    }

    fn identity(&self) -> CheckpointIdentity {
        CheckpointIdentity::new(
            self.cluster_id.clone(),
            self.epoch,
            self.config_id,
            self.config_digest,
            self.recovery_generation,
        )
    }

    fn archive(&self) -> Result<ObjectArchiveStore, String> {
        let store = open_object_store(&self.object_store)?;
        if !store.supports_strong_cross_process_cas() {
            return Err("serving checkpoints require strong cross-process compare-and-swap".into());
        }
        ObjectArchiveStore::new_checkpoint(store, self.identity())
            .map_err(|error| error.to_string())
    }
}

#[derive(Clone)]
struct RollCheckpointConfig {
    base: CheckpointCommandConfig,
    from_generation: u64,
    to_generation: u64,
}

#[derive(Clone)]
struct GcPlanConfig {
    base: CheckpointCommandConfig,
    operation_id: String,
    retain_generations: usize,
    grace_ms: u64,
    min_age_ms: u64,
}

#[derive(Clone)]
struct GcInspectConfig {
    base: CheckpointCommandConfig,
    plan_hash: String,
}

impl ServeConfig {
    fn from_env() -> Result<Self, String> {
        Self::from_lookup(|name| env::var(name).ok())
    }

    fn from_lookup(mut lookup: impl FnMut(&str) -> Option<String>) -> Result<Self, String> {
        let execution_profile = execution_profile(&mut lookup)?;
        let cluster_id_source = required_env(&mut lookup, "RHIZA_CLUSTER_ID")?;
        let cluster_id = effective_cluster_id(execution_profile, &cluster_id_source)
            .map_err(|error| error.to_string())?;
        let profile_prefix = format!("rhiza:{}:", execution_profile.as_str());
        let logical_cluster_id = cluster_id
            .strip_prefix(&profile_prefix)
            .expect("effective cluster id has the requested profile")
            .to_owned();
        let node_id = node_id_from_env(&mut lookup)?;
        let data_dir = PathBuf::from(required_env(&mut lookup, "RHIZA_DATA_DIR")?);
        let epoch = positive_env(&mut lookup, "RHIZA_EPOCH")?;
        let client_token = required_env(&mut lookup, "RHIZA_CLIENT_TOKEN")?;
        let admin_token = lookup("RHIZA_ADMIN_TOKEN")
            .map(|token| {
                AdminConfig::new(token.clone())
                    .map(|_| token)
                    .map_err(|error| format!("invalid RHIZA_ADMIN_TOKEN: {error}"))
            })
            .transpose()?;
        let tail_token = lookup("RHIZA_TAIL_TOKEN")
            .map(|token| {
                if token.is_empty() || token.chars().any(char::is_whitespace) {
                    Err("RHIZA_TAIL_TOKEN must be nonblank and contain no whitespace".to_string())
                } else {
                    Ok(token)
                }
            })
            .transpose()?;
        let bundle = load_configuration_bundle(&mut lookup, |path| fs::read_to_string(path))?;
        let client_listen = lookup("RHIZA_CLIENT_LISTEN").unwrap_or_else(|| "0.0.0.0:8080".into());
        let recorder_listen =
            lookup("RHIZA_RECORDER_LISTEN").unwrap_or_else(|| "0.0.0.0:8081".into());
        let requested_recorder_transport = match lookup("RHIZA_RECORDER_TRANSPORT").as_deref() {
            None | Some("http") => RecorderTransport::Http,
            Some("tcp-postcard") => RecorderTransport::TcpPostcard,
            Some("tcp-postcard-rpc") => {
                #[cfg(feature = "recorder-postcard-rpc")]
                {
                    RecorderTransport::TcpPostcardRpc
                }
                #[cfg(not(feature = "recorder-postcard-rpc"))]
                {
                    return Err(
                        "RHIZA_RECORDER_TRANSPORT=tcp-postcard-rpc is not compiled; enable the recorder-postcard-rpc feature"
                            .into(),
                    );
                }
            }
            Some(_) => {
                return Err(
                    "RHIZA_RECORDER_TRANSPORT must be http|tcp-postcard|tcp-postcard-rpc".into(),
                )
            }
        };
        let recorder_tls_enabled = match lookup("RHIZA_RECORDER_TLS").as_deref() {
            None | Some("off") => false,
            Some("on") => true,
            Some(_) => return Err("RHIZA_RECORDER_TLS must be on|off".into()),
        };
        if recorder_tls_enabled && requested_recorder_transport == RecorderTransport::Http {
            return Err(
                "RHIZA_RECORDER_TLS=on requires RHIZA_RECORDER_TRANSPORT=tcp-postcard|tcp-postcard-rpc"
                    .into(),
            );
        }
        let recorder_transport = match (requested_recorder_transport, recorder_tls_enabled) {
            (RecorderTransport::TcpPostcard, true) => RecorderTransport::TcpTlsPostcard,
            #[cfg(feature = "recorder-postcard-rpc")]
            (RecorderTransport::TcpPostcardRpc, true) => RecorderTransport::TcpTlsPostcardRpc,
            (transport, _) => transport,
        };
        let tcp_listen = optional_env(&mut lookup, "RHIZA_RECORDER_TCP_LISTEN")?;
        let tcp_requested = recorder_transport.is_tcp() || tcp_listen.is_some();
        let recorder_tcp = if tcp_requested {
            let listen = tcp_listen.ok_or_else(|| {
                "RHIZA_RECORDER_TCP_LISTEN is required for recorder TCP".to_string()
            })?;
            listen.parse::<std::net::SocketAddr>().map_err(|_| {
                "RHIZA_RECORDER_TCP_LISTEN must be an IP socket address".to_string()
            })?;
            let tls = if recorder_transport.is_tls() {
                Some(RecorderTlsFiles {
                    certificate: PathBuf::from(required_env(
                        &mut lookup,
                        "RHIZA_RECORDER_TLS_CERT_FILE",
                    )?),
                    private_key: PathBuf::from(required_env(
                        &mut lookup,
                        "RHIZA_RECORDER_TLS_KEY_FILE",
                    )?),
                    ca_bundle: PathBuf::from(required_env(
                        &mut lookup,
                        "RHIZA_RECORDER_TLS_CA_FILE",
                    )?),
                })
            } else {
                for name in [
                    "RHIZA_RECORDER_TLS_CERT_FILE",
                    "RHIZA_RECORDER_TLS_KEY_FILE",
                    "RHIZA_RECORDER_TLS_CA_FILE",
                ] {
                    if lookup(name).is_some() {
                        return Err(format!("{name} is irrelevant unless RHIZA_RECORDER_TLS=on"));
                    }
                }
                None
            };
            Some(RecorderTcpConfig { listen, tls })
        } else {
            for name in [
                "RHIZA_RECORDER_TLS_CERT_FILE",
                "RHIZA_RECORDER_TLS_KEY_FILE",
                "RHIZA_RECORDER_TLS_CA_FILE",
            ] {
                if lookup(name).is_some() {
                    return Err(format!("{name} is irrelevant unless RHIZA_RECORDER_TLS=on"));
                }
            }
            None
        };
        if recorder_transport.is_tcp() && bundle.recorder_tcp_peers.iter().any(Option::is_none) {
            return Err(format!(
                "{} requires recorder_tcp_addr for every bundle member",
                recorder_transport.selector()
            ));
        }
        if recorder_transport.is_tls()
            && bundle
                .recorder_tcp_peers
                .iter()
                .flatten()
                .any(|peer| peer.tls_server_name.is_none())
        {
            return Err(format!(
                "{} with RHIZA_RECORDER_TLS=on requires recorder_tls_server_name for every bundle member",
                recorder_transport.selector()
            ));
        }
        if !recorder_transport.is_tls()
            && bundle
                .recorder_tcp_peers
                .iter()
                .flatten()
                .any(|peer| peer.tls_server_name.is_some())
        {
            return Err(
                "recorder_tls_server_name is irrelevant unless RHIZA_RECORDER_TLS=on".into(),
            );
        }
        let object_store_mode = lookup("RHIZA_OBJECT_STORE");
        let (recovery_generation, remote) = match object_store_mode {
            Some(mode) => {
                let object_store = parse_object_store_with_lookup(&mode, false, &mut lookup)?;
                let recovery_generation = positive_env(&mut lookup, "RHIZA_RECOVERY_GENERATION")?;
                let startup =
                    parse_startup_mode(required_env(&mut lookup, "RHIZA_STARTUP_MODE")?.as_str())?;
                let durability = parse_durability(&mut lookup)?;
                let lease_duration_ms =
                    optional_positive_env(&mut lookup, "RHIZA_CHECKPOINT_LEASE_MS")?
                        .unwrap_or(300_000);
                if matches!(&object_store, ObjStoreConfig::Gcs { .. })
                    && lease_duration_ms < MIN_GCS_CHECKPOINT_LEASE_MS
                {
                    return Err(format!(
                        "RHIZA_CHECKPOINT_LEASE_MS must be at least {MIN_GCS_CHECKPOINT_LEASE_MS} for gcs"
                    ));
                }
                (
                    recovery_generation,
                    Some(RemoteCheckpointConfig {
                        object_store,
                        durability,
                        lease_duration_ms,
                        startup,
                    }),
                )
            }
            None => {
                for name in [
                    "RHIZA_DURABILITY_MODE",
                    "RHIZA_DURABILITY_MAX_LAG",
                    "RHIZA_DURABILITY_INTERVAL",
                    "RHIZA_CHECKPOINT_LEASE_MS",
                    "RHIZA_STARTUP_MODE",
                ] {
                    if lookup(name).is_some() {
                        return Err(format!("{name} is irrelevant without RHIZA_OBJECT_STORE"));
                    }
                }
                let generation =
                    optional_positive_env(&mut lookup, "RHIZA_RECOVERY_GENERATION")?.unwrap_or(1);
                (generation, None)
            }
        };

        let config = Self {
            execution_profile,
            logical_cluster_id,
            cluster_id,
            node_id,
            data_dir,
            epoch,
            bundle,
            client_token,
            admin_token,
            tail_token,
            client_listen,
            recorder_listen,
            recorder_transport,
            recorder_tcp,
            recovery_generation,
            remote,
        };
        config.node_config()?;
        if let Some(admin_token) = &config.admin_token {
            if admin_token == &config.client_token
                || config
                    .bundle
                    .peers
                    .iter()
                    .any(|peer| peer.token() == admin_token)
            {
                return Err(
                    "RHIZA_ADMIN_TOKEN must be distinct from client and peer tokens".into(),
                );
            }
        }
        if let Some(tail_token) = &config.tail_token {
            if tail_token == &config.client_token
                || config.admin_token.as_ref() == Some(tail_token)
                || config
                    .bundle
                    .peers
                    .iter()
                    .any(|peer| peer.token() == tail_token)
            {
                return Err(
                    "RHIZA_TAIL_TOKEN must be distinct from client, admin, and peer tokens".into(),
                );
            }
        }
        Ok(config)
    }

    fn local_peer_token(&self) -> Result<&str, String> {
        self.bundle
            .peers
            .iter()
            .find(|peer| peer.node_id() == self.node_id)
            .map(PeerConfig::token)
            .ok_or_else(|| "peer set must include RHIZA_NODE_ID".into())
    }

    fn node_config(&self) -> Result<NodeConfig, String> {
        let mut config = NodeConfig::new_with_configuration(
            self.logical_cluster_id.clone(),
            self.node_id.clone(),
            self.data_dir.clone(),
            self.epoch,
            self.bundle.membership.clone(),
            self.bundle.configuration_state.clone(),
            self.bundle.peers.clone(),
            self.client_token.clone(),
        )
        .map_err(|error| error.to_string())?
        .with_execution_profile(self.execution_profile)
        .map_err(|error| error.to_string())?;
        if let Some(predecessor) = &self.bundle.predecessor {
            config = config.with_log_initial_configuration(ConfigurationState::active(
                predecessor.stop.entry.config_id,
                predecessor.membership.digest(),
            ));
            config = config.with_predecessor_stop_entry(predecessor.stop.entry.clone());
        }
        config
            .with_recovery_generation(self.recovery_generation)
            .map_err(|error| error.to_string())
    }

    fn admin_config(&self) -> Result<Option<AdminConfig>, String> {
        self.admin_token
            .clone()
            .map(AdminConfig::new)
            .transpose()
            .map_err(|error| error.to_string())
    }
}

fn parse_command<I, S>(args: I) -> Result<Command, String>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let mut args = args.into_iter().map(Into::into);
    let Some(command) = args.next() else {
        return Err("missing command".into());
    };
    match command.as_str() {
        "status" => parse_status(args).map(Command::Status),
        #[cfg(feature = "sql")]
        "e2e" => parse_e2e(args).map(Command::E2e),
        "serve" => {
            reject_extra_args(args)?;
            ServeConfig::from_env().map(Box::new).map(Command::Serve)
        }
        "prestage" => match args.next().as_deref() {
            Some("serve") => {
                reject_extra_args(args)?;
                PrestageServeConfig::from_env()
                    .map(Box::new)
                    .map(Command::PrestageServe)
            }
            Some(other) => Err(format!("unknown prestage subcommand: {other}")),
            None => Err("missing prestage subcommand: serve".into()),
        },
        "init-checkpoint" => {
            reject_extra_args(args)?;
            CheckpointCommandConfig::from_lookup(|name| env::var(name).ok())
                .map(Command::InitCheckpoint)
        }
        "roll-checkpoint" => parse_roll_checkpoint(args).map(Command::RollCheckpoint),
        "checkpoint" => parse_checkpoint_command(args),
        "validate-config-bundle" => {
            let args = args.collect::<Vec<_>>();
            if args == ["--stdin"] {
                Ok(Command::ValidateConfigBundle(None))
            } else {
                reject_extra_args(args.into_iter())?;
                parse_validate_config_bundle(
                    |name| env::var(name).ok(),
                    |path| fs::read_to_string(path),
                )
            }
        }
        "gc" => parse_gc_command(args),
        "membership" => parse_membership_command(args),
        #[cfg(feature = "sql")]
        "write" => parse_write(args).map(Command::Write),
        #[cfg(feature = "sql")]
        "read" => parse_read(args).map(Command::Read),
        #[cfg(feature = "sql")]
        "sql" => parse_sql_command(args),
        #[cfg(feature = "graph")]
        "graph" => parse_graph_command(args),
        #[cfg(feature = "kv")]
        "kv" => parse_kv_command(args),
        "health" => parse_health(args).map(Command::Health),
        _ => Err(format!("unknown command: {command}")),
    }
}

fn parse_validate_config_bundle(
    lookup: impl FnMut(&str) -> Option<String>,
    read_file: impl FnOnce(&str) -> std::io::Result<String>,
) -> Result<Command, String> {
    load_configuration_bundle(lookup, read_file)
        .map(|bundle| Command::ValidateConfigBundle(Some(bundle.config_id)))
}

fn validate_config_bundle_stdin() -> Result<u64, String> {
    let mut json = String::new();
    io::stdin()
        .read_to_string(&mut json)
        .map_err(|error| format!("cannot read configuration bundle from stdin: {error}"))?;
    parse_configuration_bundle(&json).map(|bundle| bundle.config_id)
}

#[cfg(feature = "sql")]
fn parse_write(args: impl IntoIterator<Item = String>) -> Result<WriteArgs, String> {
    parse_write_with_lookup(args, |name| env::var(name).ok())
}

#[cfg(feature = "sql")]
fn parse_write_with_lookup(
    args: impl IntoIterator<Item = String>,
    lookup: impl FnMut(&str) -> Option<String>,
) -> Result<WriteArgs, String> {
    let mut urls = Vec::new();
    let mut token = None;
    let mut request_id = None;
    let mut key = None;
    let mut value = None;
    let mut args = args.into_iter();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--url" => urls.push(next_value(&mut args, "--url")?),
            "--token" => token = Some(next_value(&mut args, "--token")?),
            "--request-id" => request_id = Some(next_value(&mut args, "--request-id")?),
            "--key" => key = Some(next_value(&mut args, "--key")?),
            "--value" => value = Some(next_value(&mut args, "--value")?),
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }

    Ok(WriteArgs {
        urls: required_urls(urls)?,
        token: client_token(token, lookup)?,
        request_id: required_arg(request_id, "--request-id")?,
        key: required_arg(key, "--key")?,
        value: required_arg(value, "--value")?,
    })
}

#[cfg(feature = "sql")]
fn parse_read(args: impl IntoIterator<Item = String>) -> Result<ReadArgs, String> {
    parse_read_with_lookup(args, |name| env::var(name).ok())
}

#[cfg(feature = "sql")]
fn parse_read_with_lookup(
    args: impl IntoIterator<Item = String>,
    lookup: impl FnMut(&str) -> Option<String>,
) -> Result<ReadArgs, String> {
    let mut urls = Vec::new();
    let mut token = None;
    let mut key = None;
    let mut consistency = None;
    let mut expect = None;
    let mut args = args.into_iter();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--url" => urls.push(next_value(&mut args, "--url")?),
            "--token" => token = Some(next_value(&mut args, "--token")?),
            "--key" => key = Some(next_value(&mut args, "--key")?),
            "--consistency" => {
                consistency = Some(parse_read_consistency(&next_value(
                    &mut args,
                    "--consistency",
                )?)?);
            }
            "--expect" => expect = Some(next_value(&mut args, "--expect")?),
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }

    Ok(ReadArgs {
        urls: required_urls(urls)?,
        token: client_token(token, lookup)?,
        key: required_arg(key, "--key")?,
        consistency,
        expect,
    })
}

#[cfg(feature = "sql")]
fn parse_sql_command(args: impl IntoIterator<Item = String>) -> Result<Command, String> {
    let mut args = args.into_iter();
    match args.next().as_deref() {
        Some("execute") => {
            parse_sql_execute(args, |name| env::var(name).ok()).map(Command::SqlExecute)
        }
        Some("query") => parse_sql_query(args, |name| env::var(name).ok()).map(Command::SqlQuery),
        Some(other) => Err(format!("unknown sql command: {other}")),
        None => Err("missing sql command: execute|query".into()),
    }
}

#[cfg(feature = "sql")]
fn parse_sql_execute(
    args: impl IntoIterator<Item = String>,
    lookup: impl FnMut(&str) -> Option<String>,
) -> Result<SqlExecuteArgs, String> {
    let mut urls = Vec::new();
    let mut token = None;
    let mut request_id = None;
    let mut sql = None;
    let mut parameters = Vec::new();
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--url" => urls.push(next_value(&mut args, "--url")?),
            "--token" => token = Some(next_value(&mut args, "--token")?),
            "--request-id" => request_id = Some(next_value(&mut args, "--request-id")?),
            "--sql" => sql = Some(next_value(&mut args, "--sql")?),
            "--params-json" => {
                parameters = parse_sql_parameters(&next_value(&mut args, "--params-json")?)?
            }
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }
    Ok(SqlExecuteArgs {
        urls: required_urls(urls)?,
        token: client_token(token, lookup)?,
        request_id: required_arg(request_id, "--request-id")?,
        statement: SqlStatement {
            sql: required_arg(sql, "--sql")?,
            parameters,
        },
    })
}

#[cfg(feature = "sql")]
fn parse_sql_query(
    args: impl IntoIterator<Item = String>,
    lookup: impl FnMut(&str) -> Option<String>,
) -> Result<SqlQueryArgs, String> {
    let mut urls = Vec::new();
    let mut token = None;
    let mut sql = None;
    let mut parameters = Vec::new();
    let mut consistency = None;
    let mut max_rows = None;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--url" => urls.push(next_value(&mut args, "--url")?),
            "--token" => token = Some(next_value(&mut args, "--token")?),
            "--sql" => sql = Some(next_value(&mut args, "--sql")?),
            "--params-json" => {
                parameters = parse_sql_parameters(&next_value(&mut args, "--params-json")?)?
            }
            "--consistency" => {
                consistency = Some(parse_read_consistency(&next_value(
                    &mut args,
                    "--consistency",
                )?)?)
            }
            "--max-rows" => {
                max_rows = Some(
                    next_value(&mut args, "--max-rows")?
                        .parse::<u32>()
                        .map_err(|_| "--max-rows must be a positive integer".to_string())?,
                )
            }
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }
    Ok(SqlQueryArgs {
        urls: required_urls(urls)?,
        token: client_token(token, lookup)?,
        statement: SqlStatement {
            sql: required_arg(sql, "--sql")?,
            parameters,
        },
        consistency,
        max_rows,
    })
}

#[cfg(feature = "sql")]
fn parse_sql_parameters(value: &str) -> Result<Vec<SqlValue>, String> {
    serde_json::from_str(value).map_err(|error| format!("invalid --params-json: {error}"))
}

#[cfg(feature = "graph")]
fn parse_graph_command(args: impl IntoIterator<Item = String>) -> Result<Command, String> {
    let mut args = args.into_iter();
    match args.next().as_deref() {
        Some("query") => {
            parse_graph_query(args, |name| env::var(name).ok()).map(Command::GraphQuery)
        }
        Some("put-document") => parse_graph_put_document(args, |name| env::var(name).ok())
            .map(Command::GraphPutDocument),
        Some("delete-document") => parse_graph_delete_document(args, |name| env::var(name).ok())
            .map(Command::GraphDeleteDocument),
        Some("get-document") => parse_graph_get_document(args, |name| env::var(name).ok())
            .map(Command::GraphGetDocument),
        Some(other) => Err(format!("unknown graph command: {other}")),
        None => {
            Err("missing graph command: query|put-document|delete-document|get-document".into())
        }
    }
}

#[cfg(feature = "graph")]
fn parse_graph_query(
    args: impl IntoIterator<Item = String>,
    lookup: impl FnMut(&str) -> Option<String>,
) -> Result<GraphQueryArgs, String> {
    let mut urls = Vec::new();
    let mut token = None;
    let mut cypher = None;
    let mut parameters = std::collections::BTreeMap::new();
    let mut consistency = None;
    let mut max_rows = None;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--url" => urls.push(next_value(&mut args, "--url")?),
            "--token" => token = Some(next_value(&mut args, "--token")?),
            "--cypher" => cypher = Some(next_value(&mut args, "--cypher")?),
            "--params-json" => {
                parameters = serde_json::from_str(&next_value(&mut args, "--params-json")?)
                    .map_err(|error| format!("invalid --params-json: {error}"))?
            }
            "--consistency" => {
                consistency = Some(parse_read_consistency(&next_value(
                    &mut args,
                    "--consistency",
                )?)?)
            }
            "--max-rows" => {
                max_rows = Some(
                    next_value(&mut args, "--max-rows")?
                        .parse::<u32>()
                        .map_err(|_| "--max-rows must be a positive integer".to_string())?,
                )
            }
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }
    Ok(GraphQueryArgs {
        urls: required_urls(urls)?,
        token: client_token(token, lookup)?,
        statement: GraphQueryStatementDto {
            cypher: required_arg(cypher, "--cypher")?,
            parameters,
        },
        consistency,
        max_rows,
    })
}

#[cfg(feature = "graph")]
fn parse_graph_put_document(
    args: impl IntoIterator<Item = String>,
    lookup: impl FnMut(&str) -> Option<String>,
) -> Result<GraphPutDocumentArgs, String> {
    let mut urls = Vec::new();
    let mut token = None;
    let mut request_id = None;
    let mut id = None;
    let mut value = None;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--url" => urls.push(next_value(&mut args, "--url")?),
            "--token" => token = Some(next_value(&mut args, "--token")?),
            "--request-id" => request_id = Some(next_value(&mut args, "--request-id")?),
            "--id" => id = Some(next_value(&mut args, "--id")?),
            "--value-json" => value = Some(next_value(&mut args, "--value-json")?),
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }
    let value_json = required_arg(value, "--value-json")?;
    let value: GraphValueDto = serde_json::from_str(&value_json)
        .map_err(|error| format!("invalid --value-json: {error}"))?;
    Ok(GraphPutDocumentArgs {
        urls: required_urls(urls)?,
        token: client_token(token, lookup)?,
        request: GraphPutDocumentRequest {
            request_id: required_arg(request_id, "--request-id")?,
            id: required_arg(id, "--id")?,
            value,
        },
    })
}

#[cfg(feature = "graph")]
fn parse_graph_delete_document(
    args: impl IntoIterator<Item = String>,
    lookup: impl FnMut(&str) -> Option<String>,
) -> Result<GraphDeleteDocumentArgs, String> {
    let mut urls = Vec::new();
    let mut token = None;
    let mut request_id = None;
    let mut id = None;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--url" => urls.push(next_value(&mut args, "--url")?),
            "--token" => token = Some(next_value(&mut args, "--token")?),
            "--request-id" => request_id = Some(next_value(&mut args, "--request-id")?),
            "--id" => id = Some(next_value(&mut args, "--id")?),
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }
    Ok(GraphDeleteDocumentArgs {
        urls: required_urls(urls)?,
        token: client_token(token, lookup)?,
        request: GraphDeleteDocumentRequest {
            request_id: required_arg(request_id, "--request-id")?,
            id: required_arg(id, "--id")?,
        },
    })
}

#[cfg(feature = "graph")]
fn parse_graph_get_document(
    args: impl IntoIterator<Item = String>,
    lookup: impl FnMut(&str) -> Option<String>,
) -> Result<GraphGetDocumentArgs, String> {
    let mut urls = Vec::new();
    let mut token = None;
    let mut id = None;
    let mut consistency = None;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--url" => urls.push(next_value(&mut args, "--url")?),
            "--token" => token = Some(next_value(&mut args, "--token")?),
            "--id" => id = Some(next_value(&mut args, "--id")?),
            "--consistency" => {
                consistency = Some(parse_read_consistency(&next_value(
                    &mut args,
                    "--consistency",
                )?)?)
            }
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }
    Ok(GraphGetDocumentArgs {
        urls: required_urls(urls)?,
        token: client_token(token, lookup)?,
        request: GraphGetDocumentRequest {
            id: required_arg(id, "--id")?,
            consistency,
        },
    })
}

#[cfg(feature = "kv")]
fn parse_kv_command(args: impl IntoIterator<Item = String>) -> Result<Command, String> {
    let mut args = args.into_iter();
    match args.next().as_deref() {
        Some("get") => parse_kv_get(args, |name| env::var(name).ok()).map(Command::KvGet),
        Some("scan") => parse_kv_scan(args, |name| env::var(name).ok()).map(Command::KvScan),
        Some("put") => parse_kv_put(args, |name| env::var(name).ok()).map(Command::KvPut),
        Some("delete") => parse_kv_delete(args, |name| env::var(name).ok()).map(Command::KvDelete),
        Some("batch") => parse_kv_batch(args, |name| env::var(name).ok()).map(Command::KvBatch),
        Some(other) => Err(format!("unknown kv command: {other}")),
        None => Err("missing kv command: get|scan|put|delete|batch".into()),
    }
}

#[cfg(feature = "kv")]
fn parse_kv_get(
    args: impl IntoIterator<Item = String>,
    lookup: impl FnMut(&str) -> Option<String>,
) -> Result<KvGetArgs, String> {
    let mut urls = Vec::new();
    let mut token = None;
    let mut key = None;
    let mut consistency = None;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--url" => urls.push(next_value(&mut args, "--url")?),
            "--token" => token = Some(next_value(&mut args, "--token")?),
            "--key-base64" => key = Some(next_value(&mut args, "--key-base64")?),
            "--consistency" => {
                consistency = Some(parse_read_consistency(&next_value(
                    &mut args,
                    "--consistency",
                )?)?)
            }
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }
    Ok(KvGetArgs {
        urls: required_urls(urls)?,
        token: client_token(token, lookup)?,
        request: KvGetRequest {
            key: required_arg(key, "--key-base64")?,
            consistency,
        },
    })
}

#[cfg(feature = "kv")]
fn parse_kv_scan(
    args: impl IntoIterator<Item = String>,
    lookup: impl FnMut(&str) -> Option<String>,
) -> Result<KvScanArgs, String> {
    let mut urls = Vec::new();
    let mut token = None;
    let mut start = None;
    let mut end = None;
    let mut prefix = None;
    let mut cursor = None;
    let mut limit = None;
    let mut consistency = None;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--url" => urls.push(next_value(&mut args, "--url")?),
            "--token" => token = Some(next_value(&mut args, "--token")?),
            "--start-base64" => start = Some(next_value(&mut args, "--start-base64")?),
            "--end-base64" => end = Some(next_value(&mut args, "--end-base64")?),
            "--prefix-base64" => prefix = Some(next_value(&mut args, "--prefix-base64")?),
            "--cursor-base64" => cursor = Some(next_value(&mut args, "--cursor-base64")?),
            "--limit" => {
                let parsed = next_value(&mut args, "--limit")?
                    .parse::<usize>()
                    .map_err(|_| "--limit must be a positive integer".to_string())?;
                if parsed == 0 || parsed > MAX_KV_SCAN_ROWS {
                    return Err(format!("--limit must be between 1 and {MAX_KV_SCAN_ROWS}"));
                }
                limit = Some(parsed);
            }
            "--consistency" => {
                consistency = Some(parse_read_consistency(&next_value(
                    &mut args,
                    "--consistency",
                )?)?)
            }
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }
    match (&prefix, &start, &end) {
        (Some(_), None, None) | (None, Some(_), _) => {}
        _ => {
            return Err(
                "provide either --prefix-base64 alone or --start-base64 with optional --end-base64"
                    .into(),
            )
        }
    }
    Ok(KvScanArgs {
        urls: required_urls(urls)?,
        token: client_token(token, lookup)?,
        request: KvScanRequest {
            start,
            end,
            prefix,
            cursor,
            limit,
            consistency,
        },
    })
}

#[cfg(feature = "kv")]
fn parse_kv_put(
    args: impl IntoIterator<Item = String>,
    lookup: impl FnMut(&str) -> Option<String>,
) -> Result<KvPutArgs, String> {
    let mut urls = Vec::new();
    let mut token = None;
    let mut request_id = None;
    let mut key = None;
    let mut value = None;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--url" => urls.push(next_value(&mut args, "--url")?),
            "--token" => token = Some(next_value(&mut args, "--token")?),
            "--request-id" => request_id = Some(next_value(&mut args, "--request-id")?),
            "--key-base64" => key = Some(next_value(&mut args, "--key-base64")?),
            "--value-base64" => value = Some(next_value(&mut args, "--value-base64")?),
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }
    Ok(KvPutArgs {
        urls: required_urls(urls)?,
        token: client_token(token, lookup)?,
        request: KvPutRequest {
            request_id: required_arg(request_id, "--request-id")?,
            key: required_arg(key, "--key-base64")?,
            value: required_arg(value, "--value-base64")?,
        },
    })
}

#[cfg(feature = "kv")]
fn parse_kv_delete(
    args: impl IntoIterator<Item = String>,
    lookup: impl FnMut(&str) -> Option<String>,
) -> Result<KvDeleteArgs, String> {
    let mut urls = Vec::new();
    let mut token = None;
    let mut request_id = None;
    let mut key = None;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--url" => urls.push(next_value(&mut args, "--url")?),
            "--token" => token = Some(next_value(&mut args, "--token")?),
            "--request-id" => request_id = Some(next_value(&mut args, "--request-id")?),
            "--key-base64" => key = Some(next_value(&mut args, "--key-base64")?),
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }
    Ok(KvDeleteArgs {
        urls: required_urls(urls)?,
        token: client_token(token, lookup)?,
        request: KvDeleteRequest {
            request_id: required_arg(request_id, "--request-id")?,
            key: required_arg(key, "--key-base64")?,
        },
    })
}

#[cfg(feature = "kv")]
fn parse_kv_batch(
    args: impl IntoIterator<Item = String>,
    lookup: impl FnMut(&str) -> Option<String>,
) -> Result<KvBatchArgs, String> {
    let mut urls = Vec::new();
    let mut token = None;
    let mut members_json = None;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--url" => urls.push(next_value(&mut args, "--url")?),
            "--token" => token = Some(next_value(&mut args, "--token")?),
            "--members-json" => members_json = Some(next_value(&mut args, "--members-json")?),
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }
    let members_json = required_arg(members_json, "--members-json")?;
    let members: Vec<KvBatchMemberRequest> = serde_json::from_str(&members_json)
        .map_err(|error| format!("invalid --members-json: {error}"))?;
    Ok(KvBatchArgs {
        urls: required_urls(urls)?,
        token: client_token(token, lookup)?,
        request: KvBatchRequest { members },
    })
}

#[cfg(any(feature = "sql", feature = "graph", feature = "kv"))]
fn parse_read_consistency(value: &str) -> Result<ReadConsistency, String> {
    match value {
        "local" => Ok(ReadConsistency::Local),
        "read_barrier" => Ok(ReadConsistency::ReadBarrier),
        _ => value
            .strip_prefix("applied_index:")
            .ok_or_else(|| {
                "consistency must be `local`, `read_barrier`, or `applied_index:N`".to_string()
            })?
            .parse::<u64>()
            .map(ReadConsistency::AppliedIndex)
            .map_err(|_| {
                "consistency must be `local`, `read_barrier`, or `applied_index:N`".to_string()
            }),
    }
}

fn parse_status(args: impl IntoIterator<Item = String>) -> Result<HealthArgs, String> {
    parse_health(std::iter::once("--ready".into()).chain(args))
}

fn parse_health(args: impl IntoIterator<Item = String>) -> Result<HealthArgs, String> {
    let mut url = None;
    let mut ready = false;
    let mut args = args.into_iter();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--url" => url = Some(next_value(&mut args, "--url")?),
            "--ready" => ready = true,
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }

    Ok(HealthArgs {
        url: required_arg(url, "--url")?,
        ready,
    })
}

#[cfg(feature = "sql")]
fn parse_e2e(args: impl IntoIterator<Item = String>) -> Result<E2eConfig, String> {
    let mut data_dir = env::var("RHIZA_DATA_DIR").unwrap_or_else(|_| "./.rhiza-e2e".into());
    let mut object_store =
        env::var("RHIZA_OBJECT_STORE").unwrap_or_else(|_| "local:./.rhiza-objects".into());
    let mut cluster_id = env::var("RHIZA_CLUSTER_ID").unwrap_or_else(|_| "cluster-a".into());
    let mut node_id = env::var("RHIZA_NODE_ID").unwrap_or_else(|_| "node-1".into());
    let mut args = args.into_iter();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--data-dir" => data_dir = next_value(&mut args, "--data-dir")?,
            "--object-store" => object_store = next_value(&mut args, "--object-store")?,
            "--cluster-id" => cluster_id = next_value(&mut args, "--cluster-id")?,
            "--node-id" => node_id = next_value(&mut args, "--node-id")?,
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }

    Ok(E2eConfig {
        data_dir: PathBuf::from(data_dir),
        object_store: parse_object_store(&object_store)?,
        cluster_id,
        node_id,
    })
}

fn next_value(args: &mut impl Iterator<Item = String>, name: &str) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("missing value for {name}"))
}

fn required_arg(value: Option<String>, name: &str) -> Result<String, String> {
    value.ok_or_else(|| format!("missing required argument: {name}"))
}

fn required_urls(urls: Vec<String>) -> Result<Vec<String>, String> {
    if urls.is_empty() {
        Err("missing required argument: --url".into())
    } else {
        Ok(urls)
    }
}

fn client_token(
    flag: Option<String>,
    mut lookup: impl FnMut(&str) -> Option<String>,
) -> Result<String, String> {
    let token = flag
        .or_else(|| lookup("RHIZA_CLIENT_TOKEN"))
        .ok_or_else(|| {
            "missing client token: pass --token or set RHIZA_CLIENT_TOKEN".to_string()
        })?;
    validate_auth_token(&token, "client token")?;
    Ok(token)
}

fn validate_auth_token(token: &str, name: &str) -> Result<(), String> {
    if token.trim().is_empty()
        || token.chars().any(char::is_whitespace)
        || !header::HeaderValue::try_from(token).is_ok_and(|value| value.to_str().is_ok())
    {
        return Err(format!(
            "{name} must be a nonempty whitespace-free HTTP header value"
        ));
    }
    Ok(())
}

fn validate_origin_url(url: String, name: &str) -> Result<String, String> {
    let url = url.trim_end_matches('/').to_string();
    let parsed = reqwest::Url::parse(&url).map_err(|_| format!("invalid {name}"))?;
    if !matches!(parsed.scheme(), "http" | "https")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.path() != "/"
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(format!("invalid {name}"));
    }
    Ok(url)
}

fn reject_extra_args(mut args: impl Iterator<Item = String>) -> Result<(), String> {
    match args.next() {
        Some(arg) => Err(format!("unknown argument: {arg}")),
        None => Ok(()),
    }
}

fn required_env(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
) -> Result<String, String> {
    lookup(name)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("{name} is required"))
}

fn node_id_from_env(lookup: &mut impl FnMut(&str) -> Option<String>) -> Result<String, String> {
    match lookup("RHIZA_NODE_ID") {
        Some(node_id) if !node_id.is_empty() => Ok(node_id),
        Some(_) => Err("RHIZA_NODE_ID must not be empty".into()),
        None => {
            let hostname = lookup("HOSTNAME").filter(|value| !value.is_empty()).ok_or_else(|| {
                "RHIZA_NODE_ID is unset and HOSTNAME is required to derive the StatefulSet node ID"
                    .to_string()
            })?;
            let (_, ordinal) = hostname.rsplit_once('-').ok_or_else(|| {
                format!(
                    "RHIZA_NODE_ID is unset and HOSTNAME must end with a StatefulSet ordinal: {hostname}"
                )
            })?;
            if ordinal.is_empty() || !ordinal.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(format!(
                    "RHIZA_NODE_ID is unset and HOSTNAME must end with a StatefulSet ordinal: {hostname}"
                ));
            }
            let ordinal = ordinal.parse::<u64>().map_err(|_| {
                format!(
                    "RHIZA_NODE_ID is unset and HOSTNAME has an invalid StatefulSet ordinal: {hostname}"
                )
            })?;
            let member = ordinal.checked_add(1).ok_or_else(|| {
                format!(
                    "RHIZA_NODE_ID is unset and HOSTNAME has an invalid StatefulSet ordinal: {hostname}"
                )
            })?;
            Ok(format!("node-{member}"))
        }
    }
}

fn execution_profile(
    lookup: &mut impl FnMut(&str) -> Option<String>,
) -> Result<ExecutionProfile, String> {
    let profile = required_env(lookup, "RHIZA_EXECUTION_PROFILE")?
        .parse()
        .map_err(|_| "RHIZA_EXECUTION_PROFILE must be sql|graph|kv".to_string())?;
    if execution_profile_compiled(profile) {
        Ok(profile)
    } else {
        Err(format!(
            "RHIZA_EXECUTION_PROFILE={} is not compiled into this binary",
            profile.as_str()
        ))
    }
}

fn positive_env(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
) -> Result<u64, String> {
    required_env(lookup, name)?
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("{name} must be a positive integer"))
}

fn optional_positive_env(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
) -> Result<Option<u64>, String> {
    let Some(value) = lookup(name) else {
        return Ok(None);
    };
    value
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .map(Some)
        .ok_or_else(|| format!("{name} must be a positive integer"))
}

fn optional_env(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
) -> Result<Option<String>, String> {
    match lookup(name) {
        Some(value) if value.is_empty() => Err(format!("{name} must not be empty")),
        value => Ok(value),
    }
}

#[cfg(test)]
fn configuration_id(lookup: &mut impl FnMut(&str) -> Option<String>) -> Result<u64, String> {
    load_configuration_bundle(&mut *lookup, |path| fs::read_to_string(path))
        .map(|bundle| bundle.config_id)
}

fn parse_startup_mode(value: &str) -> Result<StartupMode, String> {
    match value {
        "bootstrap" => Ok(StartupMode::Bootstrap),
        "rejoin" => Ok(StartupMode::Rejoin),
        "disaster" => Ok(StartupMode::Disaster),
        _ => Err("RHIZA_STARTUP_MODE must be bootstrap|rejoin|disaster".into()),
    }
}

fn parse_durability(
    lookup: &mut impl FnMut(&str) -> Option<String>,
) -> Result<DurabilityMode, String> {
    let mode = required_env(lookup, "RHIZA_DURABILITY_MODE")?;
    let max_lag = optional_env(lookup, "RHIZA_DURABILITY_MAX_LAG")?;
    let interval = optional_env(lookup, "RHIZA_DURABILITY_INTERVAL")?;
    match mode.as_str() {
        "sync" => {
            reject_irrelevant_duration(max_lag, "RHIZA_DURABILITY_MAX_LAG", "sync")?;
            reject_irrelevant_duration(interval, "RHIZA_DURABILITY_INTERVAL", "sync")?;
            Ok(DurabilityMode::Sync)
        }
        "bounded" => {
            reject_irrelevant_duration(interval, "RHIZA_DURABILITY_INTERVAL", "bounded")?;
            let value = max_lag.ok_or_else(|| {
                "RHIZA_DURABILITY_MAX_LAG is required for bounded durability".to_string()
            })?;
            Ok(DurabilityMode::Bounded {
                max_lag: parse_positive_duration(&value)
                    .map_err(|error| format!("RHIZA_DURABILITY_MAX_LAG {error}"))?,
            })
        }
        "periodic" => {
            reject_irrelevant_duration(max_lag, "RHIZA_DURABILITY_MAX_LAG", "periodic")?;
            let value = interval.ok_or_else(|| {
                "RHIZA_DURABILITY_INTERVAL is required for periodic durability".to_string()
            })?;
            Ok(DurabilityMode::Periodic {
                interval: parse_positive_duration(&value)
                    .map_err(|error| format!("RHIZA_DURABILITY_INTERVAL {error}"))?,
            })
        }
        _ => Err("RHIZA_DURABILITY_MODE must be sync|bounded|periodic".into()),
    }
}

fn reject_irrelevant_duration(value: Option<String>, name: &str, mode: &str) -> Result<(), String> {
    if value.is_some() {
        Err(format!("{name} is irrelevant for {mode} durability"))
    } else {
        Ok(())
    }
}

fn parse_positive_duration(value: &str) -> Result<Duration, String> {
    let (number, multiplier) = if let Some(number) = value.strip_suffix("ms") {
        (number, 1_u64)
    } else if let Some(number) = value.strip_suffix('s') {
        (number, 1_000)
    } else if let Some(number) = value.strip_suffix('m') {
        (number, 60_000)
    } else if let Some(number) = value.strip_suffix('h') {
        (number, 3_600_000)
    } else {
        return Err("must be a positive duration with ms/s/m/h suffix".into());
    };
    if number.is_empty() || !number.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err("must be a positive duration with ms/s/m/h suffix".into());
    }
    let amount = number
        .parse::<u64>()
        .ok()
        .filter(|amount| *amount > 0)
        .ok_or_else(|| "must be a positive duration with ms/s/m/h suffix".to_string())?;
    let millis = amount
        .checked_mul(multiplier)
        .ok_or_else(|| "duration is too large".to_string())?;
    Ok(Duration::from_millis(millis))
}

fn parse_roll_checkpoint(
    args: impl IntoIterator<Item = String>,
) -> Result<RollCheckpointConfig, String> {
    parse_roll_checkpoint_with_lookup(args, |name| env::var(name).ok())
}

fn parse_roll_checkpoint_with_lookup(
    args: impl IntoIterator<Item = String>,
    mut lookup: impl FnMut(&str) -> Option<String>,
) -> Result<RollCheckpointConfig, String> {
    let mut from = None;
    let mut to = None;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--from-generation" => from = Some(next_value(&mut args, "--from-generation")?),
            "--to-generation" => to = Some(next_value(&mut args, "--to-generation")?),
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }
    let from_generation = parse_positive_value(
        from.or_else(|| lookup("RHIZA_FROM_GENERATION")),
        "--from-generation or RHIZA_FROM_GENERATION",
    )?;
    let to_generation = parse_positive_value(
        to.or_else(|| lookup("RHIZA_TO_GENERATION")),
        "--to-generation or RHIZA_TO_GENERATION",
    )?;
    if from_generation.checked_add(1) != Some(to_generation) {
        return Err("target recovery generation must equal source generation + 1".into());
    }
    let generation = from_generation.to_string();
    let base = CheckpointCommandConfig::from_lookup(|name| {
        if name == "RHIZA_RECOVERY_GENERATION" {
            Some(generation.clone())
        } else {
            lookup(name)
        }
    })?;
    Ok(RollCheckpointConfig {
        base,
        from_generation,
        to_generation,
    })
}

fn parse_checkpoint_command(mut args: impl Iterator<Item = String>) -> Result<Command, String> {
    let subcommand = args
        .next()
        .ok_or_else(|| "missing checkpoint subcommand".to_string())?;
    match subcommand.as_str() {
        "inspect" => {
            reject_extra_args(args)?;
            CheckpointCommandConfig::from_lookup(|name| env::var(name).ok())
                .map(Command::CheckpointInspect)
        }
        "compact" => parse_admin_command_config(args, true, true, |name| env::var(name).ok())
            .map(Box::new)
            .map(Command::CheckpointCompact),
        _ => Err(format!("unknown checkpoint subcommand: {subcommand}")),
    }
}

fn parse_membership_command(args: impl Iterator<Item = String>) -> Result<Command, String> {
    parse_membership_command_with_lookup(args, |name| env::var(name).ok())
}

fn parse_membership_command_with_lookup(
    mut args: impl Iterator<Item = String>,
    mut lookup: impl FnMut(&str) -> Option<String>,
) -> Result<Command, String> {
    let subcommand = args
        .next()
        .ok_or_else(|| "missing membership subcommand".to_string())?;
    if !matches!(
        subcommand.as_str(),
        "status" | "stop" | "install-successor" | "activate"
    ) {
        return Err(format!("unknown membership subcommand: {subcommand}"));
    }
    let mutating = subcommand != "status";
    let config = Box::new(parse_admin_command_config(
        args,
        mutating,
        mutating,
        &mut lookup,
    )?);
    match subcommand.as_str() {
        "status" => Ok(Command::MembershipStatus(config)),
        "stop" => Ok(Command::MembershipStop(config)),
        "install-successor" => Ok(Command::MembershipInstallSuccessor(config)),
        "activate" => Ok(Command::MembershipActivate(config)),
        _ => unreachable!("membership subcommand validated"),
    }
}

fn parse_admin_command_config(
    args: impl Iterator<Item = String>,
    need_serve: bool,
    require_operation_id: bool,
    mut lookup: impl FnMut(&str) -> Option<String>,
) -> Result<AdminCommandConfig, String> {
    let mut offline = false;
    let mut url = None;
    let mut token = None;
    let mut operation_id = None;
    let mut args = args;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--offline" => offline = true,
            "--admin-url" => url = Some(next_value(&mut args, "--admin-url")?),
            "--admin-token" => token = Some(next_value(&mut args, "--admin-token")?),
            "--operation-id" => operation_id = Some(next_value(&mut args, "--operation-id")?),
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }

    let successor = load_optional_successor_bundle(&mut lookup)?;

    if offline {
        if url.is_some() || token.is_some() || operation_id.is_some() {
            return Err("admin URL, token, and operation id are irrelevant with --offline".into());
        }
        let serve = ServeConfig::from_lookup(&mut lookup)?;
        return Ok(AdminCommandConfig {
            target: AdminTarget::Offline,
            serve: Some(Box::new(serve)),
            operation_id: None,
            successor,
        });
    }

    let url = url
        .or_else(|| lookup("RHIZA_ADMIN_URL"))
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "missing admin URL: pass --admin-url or set RHIZA_ADMIN_URL".to_string())?;
    let url = validate_origin_url(url, "admin URL")?;
    let token = token
        .or_else(|| lookup("RHIZA_ADMIN_TOKEN"))
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            "missing admin token: pass --admin-token or set RHIZA_ADMIN_TOKEN".to_string()
        })?;
    AdminConfig::new(token.clone()).map_err(|error| format!("invalid admin token: {error}"))?;
    let operation_id = operation_id
        .or_else(|| lookup("RHIZA_ADMIN_OPERATION_ID"))
        .filter(|value| !value.trim().is_empty());
    if require_operation_id && operation_id.is_none() {
        return Err(
            "missing operation id: pass --operation-id or set RHIZA_ADMIN_OPERATION_ID".into(),
        );
    }
    let serve = need_serve
        .then(|| ServeConfig::from_lookup(&mut lookup))
        .transpose()?
        .map(Box::new);
    if let Some(serve) = &serve {
        if token == serve.client_token
            || serve.bundle.peers.iter().any(|peer| peer.token() == token)
        {
            return Err("admin token must be distinct from client and peer tokens".into());
        }
    }
    Ok(AdminCommandConfig {
        target: AdminTarget::Live(AdminClientConfig { url, token }),
        serve,
        operation_id,
        successor,
    })
}

fn load_optional_successor_bundle(
    lookup: &mut impl FnMut(&str) -> Option<String>,
) -> Result<Option<ConfigurationBundle>, String> {
    let inline = optional_env(&mut *lookup, "RHIZA_SUCCESSOR_CONFIG_BUNDLE")?;
    let file = optional_env(&mut *lookup, "RHIZA_SUCCESSOR_CONFIG_BUNDLE_FILE")?;
    match (inline, file) {
        (Some(_), Some(_)) => Err(
            "RHIZA_SUCCESSOR_CONFIG_BUNDLE and RHIZA_SUCCESSOR_CONFIG_BUNDLE_FILE are mutually exclusive"
                .into(),
        ),
        (Some(json), None) => parse_configuration_bundle(&json).map(Some),
        (None, Some(path)) => fs::read_to_string(path)
            .map_err(|error| format!("cannot read RHIZA_SUCCESSOR_CONFIG_BUNDLE_FILE: {error}"))
            .and_then(|json| parse_configuration_bundle(&json))
            .map(Some),
        (None, None) => Ok(None),
    }
}

fn parse_gc_command(mut args: impl Iterator<Item = String>) -> Result<Command, String> {
    let subcommand = args
        .next()
        .ok_or_else(|| "missing gc subcommand".to_string())?;
    match subcommand.as_str() {
        "plan" => parse_gc_plan(args, |name| env::var(name).ok()).map(Command::GcPlan),
        "inspect" | "evidence" => {
            let plan_hash = parse_plan_hash_flags(args, false)?;
            let base = CheckpointCommandConfig::from_lookup(|name| env::var(name).ok())?;
            Ok(Command::GcInspect(GcInspectConfig { base, plan_hash }))
        }
        "apply" => {
            let plan_hash = parse_gc_apply_flags(args)?;
            let base = CheckpointCommandConfig::from_lookup(|name| env::var(name).ok())?;
            Ok(Command::GcApply(GcInspectConfig { base, plan_hash }))
        }
        _ => Err(format!("unknown gc subcommand: {subcommand}")),
    }
}

fn parse_gc_plan(
    args: impl IntoIterator<Item = String>,
    mut lookup: impl FnMut(&str) -> Option<String>,
) -> Result<GcPlanConfig, String> {
    let mut operation_id = None;
    let mut retain_generations = None;
    let mut grace_ms = None;
    let mut min_age_ms = None;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--operation-id" => operation_id = Some(next_value(&mut args, "--operation-id")?),
            "--retain-generations" => {
                retain_generations = Some(next_value(&mut args, "--retain-generations")?)
            }
            "--grace-ms" => grace_ms = Some(next_value(&mut args, "--grace-ms")?),
            "--min-age-ms" => min_age_ms = Some(next_value(&mut args, "--min-age-ms")?),
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }
    let operation_id = required_arg(operation_id, "--operation-id")?;
    if operation_id.trim().is_empty() {
        return Err("--operation-id must not be empty".into());
    }
    let retain_generations =
        parse_nonnegative_usize(retain_generations, "--retain-generations")?.unwrap_or(1);
    let grace_ms = parse_nonnegative_u64(grace_ms, "--grace-ms")?.unwrap_or(60_000);
    let min_age_ms = parse_nonnegative_u64(min_age_ms, "--min-age-ms")?.unwrap_or(86_400_000);
    let base = CheckpointCommandConfig::from_lookup(&mut lookup)?;
    Ok(GcPlanConfig {
        base,
        operation_id,
        retain_generations,
        grace_ms,
        min_age_ms,
    })
}

fn parse_nonnegative_u64(value: Option<String>, name: &str) -> Result<Option<u64>, String> {
    value
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| format!("{name} must be a nonnegative integer"))
        })
        .transpose()
}

fn parse_nonnegative_usize(value: Option<String>, name: &str) -> Result<Option<usize>, String> {
    value
        .map(|value| {
            value
                .parse::<usize>()
                .map_err(|_| format!("{name} must be a nonnegative integer"))
        })
        .transpose()
}

fn parse_gc_apply_flags(args: impl IntoIterator<Item = String>) -> Result<String, String> {
    parse_plan_hash_flags(args, true)
}

fn parse_plan_hash_flags(
    args: impl IntoIterator<Item = String>,
    require_confirmation: bool,
) -> Result<String, String> {
    let mut plan_hash = None;
    let mut confirmed = false;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--plan-hash" => plan_hash = Some(next_value(&mut args, "--plan-hash")?),
            "--confirm" if require_confirmation => confirmed = true,
            _ => return Err(format!("unknown argument: {arg}")),
        }
    }
    let plan_hash = required_arg(plan_hash, "--plan-hash")?;
    if plan_hash.len() != 64
        || !plan_hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err("--plan-hash must be an exact 64-character lowercase SHA-256 hex value".into());
    }
    if require_confirmation && !confirmed {
        return Err("gc apply requires --confirm".into());
    }
    Ok(plan_hash)
}

fn parse_positive_value(value: Option<String>, name: &str) -> Result<u64, String> {
    value
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("{name} must be a positive integer"))
}

fn load_configuration_bundle(
    mut lookup: impl FnMut(&str) -> Option<String>,
    read_file: impl FnOnce(&str) -> std::io::Result<String>,
) -> Result<ConfigurationBundle, String> {
    let inline = optional_env(&mut lookup, "RHIZA_CONFIG_BUNDLE")?;
    let file = optional_env(&mut lookup, "RHIZA_CONFIG_BUNDLE_FILE")?;
    if inline.is_some() && file.is_some() {
        return Err(
            "RHIZA_CONFIG_BUNDLE and RHIZA_CONFIG_BUNDLE_FILE are mutually exclusive".into(),
        );
    }
    let json = match (inline, file) {
        (Some(json), None) => json,
        (None, Some(path)) => read_file(&path)
            .map_err(|error| format!("cannot read RHIZA_CONFIG_BUNDLE_FILE: {error}"))?,
        (None, None) => {
            return Err("RHIZA_CONFIG_BUNDLE or RHIZA_CONFIG_BUNDLE_FILE is required".into())
        }
        _ => unreachable!("bundle source exclusivity checked"),
    };
    parse_configuration_bundle(&json)
}

fn parse_configuration_bundle(json: &str) -> Result<ConfigurationBundle, String> {
    let document: ConfigurationBundleDocument = serde_json::from_str(json)
        .map_err(|error| format!("invalid configuration bundle JSON: {error}"))?;
    if document.config_id == 0 {
        return Err("configuration bundle config_id must be positive".into());
    }
    let mut members = document
        .members
        .into_iter()
        .map(|member| {
            let recorder_tcp = match member.recorder_tcp_addr {
                None => {
                    if member.recorder_tls_server_name.is_some() {
                        return Err(
                            "recorder_tls_server_name requires recorder_tcp_addr".to_string()
                        );
                    }
                    None
                }
                Some(address) => {
                    validate_recorder_tcp_endpoint(&address)?;
                    if member
                        .recorder_tls_server_name
                        .as_ref()
                        .is_some_and(|name| name.trim().is_empty())
                    {
                        return Err("recorder_tls_server_name must not be empty".into());
                    }
                    Some(RecorderTcpPeer {
                        address,
                        tls_server_name: member.recorder_tls_server_name,
                    })
                }
            };
            let log_url = member.log_url.unwrap_or_else(|| member.url.clone());
            let peer =
                PeerConfig::new_with_log_url(member.node_id, member.url, log_url, member.token)
                    .map_err(|error| error.to_string())?;
            Ok::<_, String>((peer, recorder_tcp))
        })
        .collect::<Result<Vec<_>, _>>()?;
    members.sort_by(|left, right| left.0.node_id().cmp(right.0.node_id()));
    let (peers, recorder_tcp_peers): (Vec<_>, Vec<_>) = members.into_iter().unzip();
    let membership = Membership::from_voters(
        peers
            .iter()
            .map(|peer| peer.node_id().to_string())
            .collect::<Vec<_>>(),
    )
    .map_err(|error| error.to_string())?;

    let predecessor = document
        .predecessor
        .map(|predecessor| parse_predecessor(document.config_id, &membership, predecessor))
        .transpose()?;
    let configuration_state = match predecessor.as_ref() {
        Some(predecessor) => ConfigurationState::active(
            predecessor.stop.entry.config_id,
            predecessor.membership.digest(),
        )
        .validate_entry(&predecessor.stop.entry)
        .map_err(|error| format!("invalid predecessor stop entry: {error}"))?,
        None => ConfigurationState::active(document.config_id, membership.digest()),
    };
    Ok(ConfigurationBundle {
        config_id: document.config_id,
        peers,
        recorder_tcp_peers,
        membership,
        configuration_state,
        predecessor,
    })
}

fn parse_predecessor(
    successor_config_id: u64,
    successor_membership: &Membership,
    predecessor: PredecessorDocument,
) -> Result<PredecessorConfiguration, String> {
    let membership = Membership::from_voters(predecessor.members)
        .map_err(|error| format!("invalid predecessor membership: {error}"))?;
    let entry = predecessor.stop_entry;
    let proof = predecessor.stop_proof;
    if entry.config_id.checked_add(1) != Some(successor_config_id) {
        return Err("successor config_id must equal predecessor config_id + 1".into());
    }
    proof
        .validate_for(entry.index, entry.epoch, entry.config_id, &membership)
        .map_err(|error| format!("invalid predecessor Stop proof: {error:?}"))?;
    let value = proof
        .proposal()
        .value
        .as_ref()
        .ok_or_else(|| "predecessor Stop proof has no value".to_string())?;
    let command = StoredCommand::new(entry.entry_type, entry.payload.clone());
    let change = ConfigChange::recognize(&command)
        .map_err(|_| "predecessor Stop must be bound to an exact successor".to_string())?;
    let bound_successor = change
        .successor()
        .ok_or_else(|| "predecessor Stop must be bound to an exact successor".to_string())?
        .clone();
    let transitioned = ConfigurationState::active(entry.config_id, membership.digest())
        .validate_entry(&entry)
        .map_err(|error| format!("invalid predecessor stop entry: {error}"))?;
    if entry.recompute_hash() != entry.hash
        || value.command_hash != command.hash()
        || value.prev_hash != entry.prev_hash
        || value.entry_hash != entry.hash
        || bound_successor.config_id() != successor_config_id
        || bound_successor.members() != successor_membership.members()
        || bound_successor.digest() != successor_membership.digest()
        || transitioned.stop().map(|stop| (stop.index(), stop.hash()))
            != Some((entry.index, entry.hash))
    {
        return Err("predecessor Stop entry and proof do not match".into());
    }
    Ok(PredecessorConfiguration {
        membership,
        stop: StopInformation { entry, proof },
    })
}

async fn serve(config: ServeConfig) -> Result<(), String> {
    serve_until(config, shutdown_signal()).await
}

async fn run_prestage_serve(config: PrestageServeConfig) -> Result<(), String> {
    let startup = prestage_startup_config(&config.target)?;
    let recorder_clients = build_recorder_clients(&config.target)?;
    let log_peers = build_log_peers(&config.target)?
        .into_iter()
        .map(|peer| Arc::new(peer) as Arc<dyn LogPeer>)
        .collect();
    let (recorder_listener, recorder_transport) = bind_ha_recorder_listener(&config.target).await?;
    let service_listener = bind_client_listener(&config.target).await?;
    let mut serve = HaServeConfig::new(
        recorder_listener,
        service_listener,
        recorder_transport,
        recorder_clients,
        log_peers,
    );
    if let Some(admin) = config.target.admin_config()? {
        serve = serve.with_admin(admin);
    }
    if let Some(tail_token) = config.target.tail_token.clone() {
        serve = serve.with_tail_token(tail_token);
    }
    let tail_source = Arc::new(HttpCertifiedTailSource::new(&config)?);
    let prestage = HaSuccessorPrestageConfig::new(
        config.source_archive.clone(),
        &config.prestage_dir,
        config.target.node_id.clone(),
        config.target.execution_profile,
        config.source_bundle.membership.clone(),
        config.target.bundle.membership.clone(),
        config.tail_token.clone(),
    );
    let node = prestage
        .start_live(startup, serve, tail_source)
        .map_err(|error| error.to_string())?;
    println!(
        "{{\"state\":\"prestage\",\"node_id\":{},\"source_config_id\":{},\"target_config_id\":{},\"same_process\":true}}",
        serde_json::to_string(&config.target.node_id).map_err(|error| error.to_string())?,
        config.source_bundle.config_id,
        config.target.bundle.config_id
    );

    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    let mut transition_poll = tokio::time::interval(Duration::from_millis(250));
    transition_poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut predecessor_bound = false;
    loop {
        tokio::select! {
            biased;
            () = &mut shutdown => return shutdown_ha_successor(node, Ok(())).await,
            result = node.ready() => {
                result.map_err(|error| error.to_string())?;
                break;
            }
            result = node.monitor() => {
                return shutdown_ha_successor(node, result).await;
            }
            _ = transition_poll.tick(), if !predecessor_bound => {
                if let Some(final_config) = transition_serve_config(&config)? {
                    let predecessor = final_config.bundle.require_predecessor()?;
                    node.bind_predecessor(HaPredecessor::new(
                        predecessor.membership.clone(),
                        predecessor.stop.clone(),
                    ))
                    .map_err(|error| error.to_string())?;
                    predecessor_bound = true;
                    println!(
                        "{{\"state\":\"transitioning\",\"node_id\":{},\"target_config_id\":{},\"stop_index\":{}}}",
                        serde_json::to_string(&config.target.node_id)
                            .map_err(|error| error.to_string())?,
                        final_config.bundle.config_id,
                        predecessor.stop.entry.index
                    );
                }
            }
        }
    }
    println!(
        "rhiza successor serving client={} recorder={} recovery_generation={}",
        config.target.client_listen,
        active_recorder_listen(&config.target)?,
        config.target.recovery_generation
    );
    tokio::select! {
        biased;
        () = &mut shutdown => shutdown_ha_successor(node, Ok(())).await,
        result = node.monitor() => shutdown_ha_successor(node, result).await,
    }
}

fn transition_serve_config(config: &PrestageServeConfig) -> Result<Option<ServeConfig>, String> {
    let transition = match fs::read_to_string(&config.transition_bundle_file) {
        Ok(json) => parse_configuration_bundle(&json)?,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("cannot read successor transition bundle: {error}")),
    };
    let predecessor = transition.require_predecessor()?;
    if transition.config_id != config.target.bundle.config_id
        || transition.membership != config.target.bundle.membership
        || predecessor.membership != config.source_bundle.membership
    {
        return Err("successor transition bundle differs from the prestaged identities".into());
    }
    let transition_path = config.transition_bundle_file.to_string_lossy().into_owned();
    let final_config = ServeConfig::from_lookup(|name| {
        if name == "RHIZA_CONFIG_BUNDLE_FILE" {
            Some(transition_path.clone())
        } else if name == "RHIZA_CONFIG_BUNDLE" {
            None
        } else {
            env::var(name).ok()
        }
    })?;
    Ok(Some(final_config))
}

fn prestage_startup_config(config: &ServeConfig) -> Result<HaStartupConfig, String> {
    let remote = config
        .remote
        .as_ref()
        .ok_or_else(|| "prestage finalization requires RHIZA_OBJECT_STORE".to_string())?;
    let store = open_object_store(&remote.object_store)?;
    let archive = ObjectArchiveStore::new_checkpoint(
        store,
        CheckpointIdentity::new(
            config.cluster_id.clone(),
            config.epoch,
            config.bundle.config_id,
            config.bundle.membership.digest(),
            config.recovery_generation,
        ),
    )
    .map_err(|error| error.to_string())?;
    Ok(HaStartupConfig::new(
        config.node_config()?,
        archive,
        remote.durability.clone(),
        remote.lease_duration_ms,
        HaStartupMode::Rejoin,
    ))
}

struct HttpCertifiedTailSource {
    client: reqwest::Client,
    peers: Vec<PeerConfig>,
    tail_token: String,
    cluster_id: String,
    epoch: u64,
    config_id: u64,
    membership_digest: String,
    recovery_generation: u64,
}

impl HttpCertifiedTailSource {
    fn new(config: &PrestageServeConfig) -> Result<Self, String> {
        Ok(Self {
            client: bounded_http_client(HEALTH_CONNECT_TIMEOUT, HEALTH_REQUEST_TIMEOUT)?,
            peers: config.source_bundle.peers.clone(),
            tail_token: config.tail_token.clone(),
            cluster_id: config.target.cluster_id.clone(),
            epoch: config.target.epoch,
            config_id: config.source_bundle.config_id,
            membership_digest: config.source_bundle.membership.digest().to_hex(),
            recovery_generation: config.target.recovery_generation,
        })
    }
}

impl HaCertifiedTailSource for HttpCertifiedTailSource {
    fn fetch<'a>(
        &'a self,
        request: &'a rhiza_node::CertifiedTailRequest,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<CertifiedTailResponse, HaCertifiedTailError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            let mut failures = Vec::new();
            for peer in &self.peers {
                let url = endpoint(peer.log_base_url(), CERTIFIED_TAIL_PATH);
                let response = self
                    .client
                    .post(&url)
                    .header(
                        header::AUTHORIZATION,
                        format!("Rhiza-Tail {}", self.tail_token),
                    )
                    .header(TAIL_VERSION_HEADER, TAIL_PROTOCOL_VERSION)
                    .header(TAIL_CLUSTER_ID_HEADER, &self.cluster_id)
                    .header(TAIL_EPOCH_HEADER, self.epoch)
                    .header(TAIL_CONFIG_ID_HEADER, self.config_id)
                    .header(TAIL_MEMBERSHIP_DIGEST_HEADER, &self.membership_digest)
                    .header(RECOVERY_GENERATION_HEADER, self.recovery_generation)
                    .json(request)
                    .send()
                    .await;
                match response {
                    Ok(response) if response.status().is_success() => {
                        return response.json().await.map_err(|error| {
                            HaCertifiedTailError::Rejected(format!(
                                "invalid response from {url}: {error}"
                            ))
                        });
                    }
                    Ok(response) if response.status().as_u16() == 409 => {
                        let error: CertifiedTailErrorResponse =
                            response.json().await.map_err(|error| {
                                HaCertifiedTailError::Rejected(format!(
                                    "invalid rebase response from {url}: {error}"
                                ))
                            })?;
                        let CertifiedTailErrorResponse::RebaseRequired { checkpoint } = error;
                        return Err(HaCertifiedTailError::RebaseRequired(checkpoint));
                    }
                    Ok(response) if matches!(response.status().as_u16(), 429 | 503 | 504) => {
                        failures.push(format!("{url}: HTTP {}", response.status()));
                    }
                    Ok(response) => {
                        return Err(HaCertifiedTailError::Rejected(format!(
                            "{url}: HTTP {}",
                            response.status()
                        )));
                    }
                    Err(error) => failures.push(format!("{url}: {}", request_error(error))),
                }
            }
            Err(HaCertifiedTailError::Unavailable(failures.join("; ")))
        })
    }
}

async fn serve_until<F>(config: ServeConfig, shutdown: F) -> Result<(), String>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    if config.remote.is_some() {
        serve_remote_until(config, shutdown).await
    } else {
        serve_local_until(config, shutdown).await
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = terminate.recv() => {}
                }
            }
            Err(error) => {
                eprintln!("cannot install SIGTERM handler: {error}");
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ServeExit {
    Shutdown,
    Client,
    Recorder,
}

async fn wait_for_shutdown(mut shutdown: tokio::sync::watch::Receiver<bool>) {
    while !*shutdown.borrow() {
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

fn stop_admin_admission(tasks: Option<&AdminTaskTracker>) {
    if let Some(tasks) = tasks {
        tasks.stop_admission();
    }
}

async fn wait_for_admin_tasks(tasks: Option<&AdminTaskTracker>) {
    if let Some(tasks) = tasks {
        tasks.wait_for_idle().await;
    }
}

fn shutdown_deadline_error(timeout: Duration) -> String {
    format!(
        "shutdown did not complete within {} seconds; final checkpoint durability is unconfirmed",
        timeout.as_secs_f64()
    )
}

async fn before_shutdown_deadline<T>(
    deadline: tokio::time::Instant,
    timeout: Duration,
    future: impl std::future::Future<Output = T>,
) -> Result<T, String> {
    if tokio::time::Instant::now() >= deadline {
        return Err(shutdown_deadline_error(timeout));
    }
    tokio::time::timeout_at(deadline, future)
        .await
        .map_err(|_| shutdown_deadline_error(timeout))
}

async fn serve_local_until<F>(config: ServeConfig, shutdown: F) -> Result<(), String>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    tokio::pin!(shutdown);
    let node_config = config.node_config()?;
    let recorder = open_recorder_from_current_state(&config)?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let recorder_server_startup =
        spawn_recorder_server(&config, recorder.clone(), shutdown_rx.clone());
    tokio::pin!(recorder_server_startup);
    let mut recorder_server = tokio::select! {
        biased;
        () = &mut shutdown => return Ok(()),
        result = &mut recorder_server_startup => result?,
    };
    tokio::select! {
        biased;
        () = &mut shutdown => {
            stop_recorder_during_startup(&shutdown_tx, &mut recorder_server).await?;
            return Ok(());
        }
        result = &mut recorder_server.0 => return Err(recorder_task_error(result)),
        () = tokio::task::yield_now() => {}
    }

    let consensus = build_consensus(&config, Some(&recorder))?;
    let runtime_startup = open_runtime_with_retry(node_config, consensus, Vec::new());
    tokio::pin!(runtime_startup);
    let runtime = tokio::select! {
        biased;
        () = &mut shutdown => {
            stop_recorder_during_startup(&shutdown_tx, &mut recorder_server).await?;
            return Ok(());
        }
        result = &mut runtime_startup => result?,
        result = &mut recorder_server.0 => return Err(recorder_task_error(result)),
    };
    let client_listener_startup = bind_client_listener(&config);
    tokio::pin!(client_listener_startup);
    let client_listener = tokio::select! {
        biased;
        () = &mut shutdown => {
            runtime.cancel_operations();
            stop_recorder_during_startup(&shutdown_tx, &mut recorder_server).await?;
            return Ok(());
        }
        result = &mut recorder_server.0 => {
            runtime.cancel_operations();
            return Err(recorder_task_error(result));
        }
        result = &mut client_listener_startup => result?,
    };
    println!(
        "rhiza serving client={} recorder={}",
        config.client_listen,
        active_recorder_listen(&config)?
    );
    let mut materializer = materializer_worker(runtime.clone(), shutdown_rx.clone());

    let (app, admin_tasks) = match config.admin_config()? {
        Some(admin) => {
            let (router, tasks) =
                node_router_with_admin_and_tasks(runtime.clone(), recorder, admin)
                    .map_err(|error| error.to_string())?;
            (router, Some(tasks))
        }
        None => (node_router(runtime.clone(), recorder), None),
    };
    let mut client_server = AbortOnDrop(tokio::spawn(async move {
        axum::serve(client_listener, app)
            .with_graceful_shutdown(wait_for_shutdown(shutdown_rx))
            .await
            .map_err(|error| format!("client server stopped: {error}"))
    }));
    let (exit, result) = tokio::select! {
        biased;
        () = &mut shutdown => (ServeExit::Shutdown, Ok(())),
        result = &mut client_server.0 =>
            (ServeExit::Client, server_task_result(result, "client server")),
        result = &mut recorder_server.0 =>
            (ServeExit::Recorder, Err(recorder_task_error(result))),
    };
    stop_admin_admission(admin_tasks.as_ref());
    shutdown_tx.send_replace(true);
    runtime.cancel_operations();
    let deadline = tokio::time::Instant::now() + SERVE_SHUTDOWN_TIMEOUT;
    let drained = before_shutdown_deadline(deadline, SERVE_SHUTDOWN_TIMEOUT, async {
        let mut drained = Ok(());
        if exit != ServeExit::Client {
            retain_first_error(
                &mut drained,
                server_task_result((&mut client_server.0).await, "client server"),
            );
        }
        if exit != ServeExit::Recorder {
            retain_first_error(
                &mut drained,
                server_task_result((&mut recorder_server.0).await, "recorder server"),
            );
        }
        retain_first_error(
            &mut drained,
            task_result((&mut materializer.0).await, "materializer worker"),
        );
        wait_for_admin_tasks(admin_tasks.as_ref()).await;
        drained
    })
    .await
    .unwrap_or_else(Err);
    let mut shutdown_result = result;
    retain_first_error(&mut shutdown_result, drained);
    shutdown_result
}

async fn serve_remote_until<F>(config: ServeConfig, shutdown: F) -> Result<(), String>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let remote = config
        .remote
        .clone()
        .ok_or_else(|| "remote checkpoint configuration is missing".to_string())?;
    let store = open_object_store(&remote.object_store)?;
    if !store.supports_strong_cross_process_cas() {
        return Err("serving checkpoints require strong cross-process compare-and-swap".into());
    }
    let archive = ObjectArchiveStore::new_checkpoint(
        store,
        CheckpointIdentity::new(
            config.cluster_id.clone(),
            config.epoch,
            config.bundle.config_id,
            config.bundle.membership.digest(),
            config.recovery_generation,
        ),
    )
    .map_err(|error| error.to_string())?;
    serve_remote_with_archive_until(config, remote, archive, shutdown).await
}

async fn serve_remote_with_archive_until<F>(
    config: ServeConfig,
    remote: RemoteCheckpointConfig,
    archive: ObjectArchiveStore,
    shutdown: F,
) -> Result<(), String>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    tokio::pin!(shutdown);
    let node_config = config.node_config()?;
    let startup_mode = match remote.startup {
        StartupMode::Bootstrap => HaStartupMode::Bootstrap,
        StartupMode::Rejoin => HaStartupMode::Rejoin,
        StartupMode::Disaster => HaStartupMode::Disaster,
    };
    let mut startup = HaStartupConfig::new(
        node_config,
        archive,
        remote.durability.clone(),
        remote.lease_duration_ms,
        startup_mode,
    );
    if let Some(predecessor) = &config.bundle.predecessor {
        startup = startup.with_predecessor(HaPredecessor::new(
            predecessor.membership.clone(),
            predecessor.stop.clone(),
        ));
    }
    let recorder_clients = build_recorder_clients(&config)?;
    let log_peers = build_log_peers(&config)?
        .into_iter()
        .map(|peer| Arc::new(peer) as Arc<dyn LogPeer>)
        .collect();
    let (recorder_listener, recorder_transport) = tokio::select! {
        biased;
        () = &mut shutdown => return Ok(()),
        result = bind_ha_recorder_listener(&config) => result?,
    };
    let service_listener = tokio::select! {
        biased;
        () = &mut shutdown => return Ok(()),
        result = bind_client_listener(&config) => result?,
    };
    let mut serve = HaServeConfig::new(
        recorder_listener,
        service_listener,
        recorder_transport,
        recorder_clients,
        log_peers,
    );
    if let Some(admin) = config.admin_config()? {
        serve = serve.with_admin(admin);
    }
    if let Some(tail_token) = config.tail_token.clone() {
        serve = serve.with_tail_token(tail_token);
    }
    let node = startup.start(serve);
    let ready = tokio::select! {
        biased;
        () = &mut shutdown => return shutdown_ha_node(node, Ok(())).await,
        result = node.ready() => result,
        result = node.monitor() => {
            return shutdown_ha_node(node, result).await;
        }
    };
    if let Err(error) = ready {
        return shutdown_ha_node(node, Err(error)).await;
    }
    println!(
        "rhiza serving client={} recorder={} recovery_generation={}",
        config.client_listen,
        active_recorder_listen(&config)?,
        config.recovery_generation
    );
    tokio::select! {
        biased;
        () = &mut shutdown => shutdown_ha_node(node, Ok(())).await,
        result = node.monitor() => shutdown_ha_node(node, result).await,
    }
}

async fn shutdown_ha_node(node: HaNode, observed: Result<(), HaNodeError>) -> Result<(), String> {
    match node.shutdown().await {
        Ok(()) => observed.map_err(|error| error.to_string()),
        Err(error) => Err(error.to_string()),
    }
}

async fn shutdown_ha_successor(
    node: HaSuccessorNode,
    observed: Result<(), HaNodeError>,
) -> Result<(), String> {
    match node.shutdown().await {
        Ok(()) => observed.map_err(|error| error.to_string()),
        Err(error) => Err(error.to_string()),
    }
}

fn open_object_store(config: &ObjStoreConfig) -> Result<ObjStore, String> {
    ObjStore::new(config.clone())
        .map_err(|error| redact_object_store_error(config, error.to_string()))
}

fn redact_object_store_error(config: &ObjStoreConfig, mut message: String) -> String {
    let secrets: Vec<&str> = match config {
        ObjStoreConfig::Local { .. } => Vec::new(),
        ObjStoreConfig::S3 {
            access_key,
            secret_key,
            ..
        } => access_key
            .iter()
            .chain(secret_key.iter())
            .map(String::as_str)
            .collect(),
        ObjStoreConfig::Gcs {
            service_account_key,
            ..
        } => service_account_key.iter().map(String::as_str).collect(),
        ObjStoreConfig::AzureBlob { access_key, .. } => {
            access_key.iter().map(String::as_str).collect()
        }
    };
    for secret in secrets.into_iter().filter(|secret| !secret.is_empty()) {
        message = message.replace(secret, "[redacted]");
    }
    message
}

fn open_recorder_from_current_state(config: &ServeConfig) -> Result<RecorderFileStore, String> {
    open_recorder_after_preflight(
        config.data_dir.join("recorder"),
        config.node_id.clone(),
        config.cluster_id.clone(),
        config.epoch,
        config.bundle.config_id,
        config.bundle.membership.clone(),
    )
}

fn open_recorder_after_preflight(
    root: PathBuf,
    recorder_id: impl Into<String>,
    cluster_id: impl Into<String>,
    epoch: u64,
    config_id: u64,
    membership: Membership,
) -> Result<RecorderFileStore, String> {
    let recorder_id = recorder_id.into();
    let cluster_id = cluster_id.into();
    let outcome = RecorderFileStore::preflight_existing_with_membership_outcome(
        &root,
        &cluster_id,
        epoch,
        config_id,
        &membership,
    )
    .map_err(|error| format!("local recorder is not trustworthy: {error}"))?;
    let opened = match outcome {
        RecorderPreflight::Missing => RecorderFileStore::new_with_membership(
            root,
            recorder_id,
            cluster_id,
            epoch,
            config_id,
            membership,
        ),
        RecorderPreflight::Valid | RecorderPreflight::Recoverable => {
            RecorderFileStore::open_existing_with_membership(
                root,
                recorder_id,
                cluster_id,
                epoch,
                config_id,
                membership,
            )
        }
    };
    opened.map_err(|error| error.to_string())
}

fn active_recorder_listen(config: &ServeConfig) -> Result<&str, String> {
    if config.recorder_transport.is_tcp() {
        config
            .recorder_tcp
            .as_ref()
            .map(|tcp| tcp.listen.as_str())
            .ok_or_else(|| "recorder TCP configuration is missing".to_string())
    } else {
        Ok(&config.recorder_listen)
    }
}

async fn bind_ha_recorder_listener(
    config: &ServeConfig,
) -> Result<(tokio::net::TcpListener, HaRecorderTransport), String> {
    match config.recorder_transport {
        RecorderTransport::Http => {
            let listener = tokio::net::TcpListener::bind(&config.recorder_listen)
                .await
                .map_err(|error| format!("cannot bind recorder listener: {error}"))?;
            Ok((listener, HaRecorderTransport::Http))
        }
        RecorderTransport::TcpPostcard => {
            let tcp = config
                .recorder_tcp
                .as_ref()
                .ok_or_else(|| "recorder TCP configuration is missing".to_string())?;
            let listener = tokio::net::TcpListener::bind(&tcp.listen)
                .await
                .map_err(|error| format!("cannot bind recorder TCP listener: {error}"))?;
            Ok((listener, HaRecorderTransport::TcpPostcard))
        }
        RecorderTransport::TcpTlsPostcard => {
            let tcp = config
                .recorder_tcp
                .as_ref()
                .ok_or_else(|| "recorder TCP configuration is missing".to_string())?;
            let tls = tcp
                .tls
                .as_ref()
                .ok_or_else(|| "recorder TLS configuration is missing".to_string())?;
            let certificate = fs::read(&tls.certificate)
                .map_err(|error| format!("cannot read recorder TLS certificate: {error}"))?;
            let private_key = fs::read(&tls.private_key)
                .map_err(|error| format!("cannot read recorder TLS private key: {error}"))?;
            let tls = RecorderTlsServerConfig::from_pem(&certificate, &private_key)?;
            let listener = tokio::net::TcpListener::bind(&tcp.listen)
                .await
                .map_err(|error| format!("cannot bind recorder TLS listener: {error}"))?;
            Ok((listener, HaRecorderTransport::TcpTlsPostcard(tls)))
        }
        #[cfg(feature = "recorder-postcard-rpc")]
        RecorderTransport::TcpPostcardRpc => {
            let tcp = config
                .recorder_tcp
                .as_ref()
                .ok_or_else(|| "recorder TCP configuration is missing".to_string())?;
            let listener = tokio::net::TcpListener::bind(&tcp.listen)
                .await
                .map_err(|error| format!("cannot bind recorder TCP listener: {error}"))?;
            Ok((listener, HaRecorderTransport::TcpPostcardRpc))
        }
        #[cfg(feature = "recorder-postcard-rpc")]
        RecorderTransport::TcpTlsPostcardRpc => {
            let tcp = config
                .recorder_tcp
                .as_ref()
                .ok_or_else(|| "recorder TCP configuration is missing".to_string())?;
            let tls = tcp
                .tls
                .as_ref()
                .ok_or_else(|| "recorder TLS configuration is missing".to_string())?;
            let certificate = fs::read(&tls.certificate)
                .map_err(|error| format!("cannot read recorder TLS certificate: {error}"))?;
            let private_key = fs::read(&tls.private_key)
                .map_err(|error| format!("cannot read recorder TLS private key: {error}"))?;
            let tls = RecorderPostcardRpcTlsServerConfig::from_pem(&certificate, &private_key)?;
            let listener = tokio::net::TcpListener::bind(&tcp.listen)
                .await
                .map_err(|error| format!("cannot bind recorder TLS listener: {error}"))?;
            Ok((listener, HaRecorderTransport::TcpTlsPostcardRpc(tls)))
        }
    }
}

async fn spawn_recorder_server<R>(
    config: &ServeConfig,
    recorder: R,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<AbortOnDrop<Result<(), String>>, String>
where
    R: RecorderRpc + Clone + Send + Sync + 'static,
{
    match config.recorder_transport {
        RecorderTransport::Http => {
            let listener = tokio::net::TcpListener::bind(&config.recorder_listen)
                .await
                .map_err(|error| format!("cannot bind recorder listener: {error}"))?;
            let app = recorder_router_for_generation(
                recorder,
                config.bundle.peers.clone(),
                config.recovery_generation,
            );
            Ok(AbortOnDrop(tokio::spawn(async move {
                axum::serve(listener, app)
                    .with_graceful_shutdown(wait_for_shutdown(shutdown))
                    .await
                    .map_err(|error| format!("recorder server stopped: {error}"))
            })))
        }
        RecorderTransport::TcpPostcard => {
            let tcp = config
                .recorder_tcp
                .as_ref()
                .ok_or_else(|| "recorder TCP configuration is missing".to_string())?;
            let listener = tokio::net::TcpListener::bind(&tcp.listen)
                .await
                .map_err(|error| format!("cannot bind recorder TCP listener: {error}"))?;
            let peers = config.bundle.peers.clone();
            let recovery_generation = config.recovery_generation;
            let (lifecycle, keepalive) = recorder_ingress_lifecycle(shutdown);
            Ok(AbortOnDrop(tokio::spawn(async move {
                let _keepalive = keepalive;
                serve_recorder_tcp(listener, recorder, peers, recovery_generation, lifecycle)
                    .await
                    .result
            })))
        }
        RecorderTransport::TcpTlsPostcard => {
            let tcp = config
                .recorder_tcp
                .as_ref()
                .ok_or_else(|| "recorder TCP configuration is missing".to_string())?;
            let tls = tcp
                .tls
                .as_ref()
                .ok_or_else(|| "recorder TLS configuration is missing".to_string())?;
            let certificate = fs::read(&tls.certificate)
                .map_err(|error| format!("cannot read recorder TLS certificate: {error}"))?;
            let private_key = fs::read(&tls.private_key)
                .map_err(|error| format!("cannot read recorder TLS private key: {error}"))?;
            let tls = RecorderTlsServerConfig::from_pem(&certificate, &private_key)?;
            let listener = tokio::net::TcpListener::bind(&tcp.listen)
                .await
                .map_err(|error| format!("cannot bind recorder TLS listener: {error}"))?;
            let peers = config.bundle.peers.clone();
            let recovery_generation = config.recovery_generation;
            let (lifecycle, keepalive) = recorder_ingress_lifecycle(shutdown);
            Ok(AbortOnDrop(tokio::spawn(async move {
                let _keepalive = keepalive;
                serve_recorder_tcp_tls(
                    listener,
                    recorder,
                    peers,
                    recovery_generation,
                    tls,
                    lifecycle,
                )
                .await
                .result
            })))
        }
        #[cfg(feature = "recorder-postcard-rpc")]
        RecorderTransport::TcpPostcardRpc => {
            let tcp = config
                .recorder_tcp
                .as_ref()
                .ok_or_else(|| "recorder TCP configuration is missing".to_string())?;
            let listener = tokio::net::TcpListener::bind(&tcp.listen)
                .await
                .map_err(|error| format!("cannot bind recorder TCP listener: {error}"))?;
            let peers = config.bundle.peers.clone();
            let recovery_generation = config.recovery_generation;
            let (lifecycle, keepalive) = recorder_ingress_lifecycle(shutdown);
            Ok(AbortOnDrop(tokio::spawn(async move {
                let _keepalive = keepalive;
                serve_recorder_postcard_rpc(
                    listener,
                    recorder,
                    peers,
                    recovery_generation,
                    lifecycle,
                )
                .await
                .result
            })))
        }
        #[cfg(feature = "recorder-postcard-rpc")]
        RecorderTransport::TcpTlsPostcardRpc => {
            let tcp = config
                .recorder_tcp
                .as_ref()
                .ok_or_else(|| "recorder TCP configuration is missing".to_string())?;
            let tls = tcp
                .tls
                .as_ref()
                .ok_or_else(|| "recorder TLS configuration is missing".to_string())?;
            let certificate = fs::read(&tls.certificate)
                .map_err(|error| format!("cannot read recorder TLS certificate: {error}"))?;
            let private_key = fs::read(&tls.private_key)
                .map_err(|error| format!("cannot read recorder TLS private key: {error}"))?;
            let tls = RecorderPostcardRpcTlsServerConfig::from_pem(&certificate, &private_key)?;
            let listener = tokio::net::TcpListener::bind(&tcp.listen)
                .await
                .map_err(|error| format!("cannot bind recorder TLS listener: {error}"))?;
            let peers = config.bundle.peers.clone();
            let recovery_generation = config.recovery_generation;
            let (lifecycle, keepalive) = recorder_ingress_lifecycle(shutdown);
            Ok(AbortOnDrop(tokio::spawn(async move {
                let _keepalive = keepalive;
                serve_recorder_postcard_rpc_tls(
                    listener,
                    recorder,
                    peers,
                    recovery_generation,
                    tls,
                    lifecycle,
                )
                .await
                .result
            })))
        }
    }
}

struct RecorderIngressKeepalive {
    _force: tokio::sync::watch::Sender<bool>,
    _started: tokio::sync::oneshot::Receiver<()>,
    _listener_dropped: tokio::sync::oneshot::Receiver<()>,
}

fn recorder_ingress_lifecycle(
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> (RecorderIngressLifecycle, RecorderIngressKeepalive) {
    let (force, force_rx) = tokio::sync::watch::channel(false);
    let (started, started_rx) = tokio::sync::oneshot::channel();
    let (listener_dropped, listener_dropped_rx) = tokio::sync::oneshot::channel();
    (
        RecorderIngressLifecycle::new(shutdown, force_rx, started, listener_dropped),
        RecorderIngressKeepalive {
            _force: force,
            _started: started_rx,
            _listener_dropped: listener_dropped_rx,
        },
    )
}

async fn bind_client_listener(config: &ServeConfig) -> Result<tokio::net::TcpListener, String> {
    tokio::net::TcpListener::bind(&config.client_listen)
        .await
        .map_err(|error| format!("cannot bind client listener: {error}"))
}

fn recorder_task_error(result: Result<Result<(), String>, tokio::task::JoinError>) -> String {
    match result {
        Ok(Ok(())) => "recorder server stopped".into(),
        Ok(Err(error)) => error,
        Err(error) => format!("recorder server task failed: {error}"),
    }
}

async fn stop_recorder_during_startup(
    shutdown: &tokio::sync::watch::Sender<bool>,
    recorder_server: &mut AbortOnDrop<Result<(), String>>,
) -> Result<(), String> {
    shutdown.send_replace(true);
    let joined = tokio::time::timeout(SERVE_SHUTDOWN_TIMEOUT, &mut recorder_server.0)
        .await
        .map_err(|_| "recorder server did not stop during startup shutdown".to_string())?;
    joined.map_err(|error| format!("recorder server task failed: {error}"))?
}

fn materializer_worker(
    runtime: Arc<NodeRuntime>,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> AbortOnDrop<()> {
    AbortOnDrop(tokio::spawn(async move {
        if let Err(error) = runtime
            .run_background_materializer(Duration::from_millis(100), wait_for_shutdown(shutdown))
            .await
        {
            eprintln!("background materializer stopped: {error}");
        }
    }))
}

fn task_result<T>(result: Result<T, tokio::task::JoinError>, name: &str) -> Result<T, String> {
    result.map_err(|error| format!("{name} task failed: {error}"))
}

fn retain_first_error(current: &mut Result<(), String>, next: Result<(), String>) {
    if current.is_ok() && next.is_err() {
        *current = next;
    }
}

fn server_task_result(
    result: Result<Result<(), String>, tokio::task::JoinError>,
    name: &str,
) -> Result<(), String> {
    task_result(result, name)?
}

fn build_recorder_clients(config: &ServeConfig) -> Result<RecorderClients, String> {
    let local_token = config.local_peer_token()?.to_owned();
    let tls_ca_bundle = if config.recorder_transport.is_tls() {
        let tls = config
            .recorder_tcp
            .as_ref()
            .and_then(|tcp| tcp.tls.as_ref())
            .ok_or_else(|| "recorder TLS configuration is missing".to_string())?;
        Some(
            fs::read(&tls.ca_bundle)
                .map_err(|error| format!("cannot read recorder TLS CA bundle: {error}"))?,
        )
    } else {
        None
    };
    config
        .bundle
        .peers
        .iter()
        .enumerate()
        .map(|(index, peer)| {
            let client: Box<dyn RecorderRpc> = match config.recorder_transport {
                RecorderTransport::Http => Box::new(
                    HttpRecorderClient::new_with_recovery_generation(
                        peer.base_url(),
                        config.node_id.clone(),
                        local_token.clone(),
                        config.recovery_generation,
                    )
                    .map_err(|error| error.to_string())?,
                ),
                RecorderTransport::TcpPostcard => {
                    let endpoint = config.bundle.recorder_tcp_peers[index]
                        .as_ref()
                        .ok_or_else(|| {
                            format!("recorder TCP endpoint is missing for {}", peer.node_id())
                        })?;
                    Box::new(TcpPostcardRecorderClient::new(
                        &endpoint.address,
                        peer.node_id(),
                        config.node_id.clone(),
                        local_token.clone(),
                        config.recovery_generation,
                    )?)
                }
                RecorderTransport::TcpTlsPostcard => {
                    let endpoint = config.bundle.recorder_tcp_peers[index]
                        .as_ref()
                        .ok_or_else(|| {
                            format!("recorder TCP endpoint is missing for {}", peer.node_id())
                        })?;
                    let server_name = endpoint.tls_server_name.as_deref().ok_or_else(|| {
                        format!("recorder TLS server name is missing for {}", peer.node_id())
                    })?;
                    let ca_bundle = tls_ca_bundle
                        .as_deref()
                        .ok_or_else(|| "recorder TLS CA bundle is missing".to_string())?;
                    let tls = RecorderTlsClientConfig::from_ca_pem(ca_bundle, server_name)?;
                    Box::new(TcpPostcardRecorderClient::new_tls(
                        &endpoint.address,
                        peer.node_id(),
                        config.node_id.clone(),
                        local_token.clone(),
                        config.recovery_generation,
                        tls,
                    )?)
                }
                #[cfg(feature = "recorder-postcard-rpc")]
                RecorderTransport::TcpPostcardRpc => {
                    let endpoint = config.bundle.recorder_tcp_peers[index]
                        .as_ref()
                        .ok_or_else(|| {
                            format!("recorder TCP endpoint is missing for {}", peer.node_id())
                        })?;
                    Box::new(TcpPostcardRpcRecorderClient::new(
                        &endpoint.address,
                        peer.node_id(),
                        config.node_id.clone(),
                        local_token.clone(),
                        config.recovery_generation,
                    )?)
                }
                #[cfg(feature = "recorder-postcard-rpc")]
                RecorderTransport::TcpTlsPostcardRpc => {
                    let endpoint = config.bundle.recorder_tcp_peers[index]
                        .as_ref()
                        .ok_or_else(|| {
                            format!("recorder TCP endpoint is missing for {}", peer.node_id())
                        })?;
                    let server_name = endpoint.tls_server_name.as_deref().ok_or_else(|| {
                        format!("recorder TLS server name is missing for {}", peer.node_id())
                    })?;
                    let ca_bundle = tls_ca_bundle
                        .as_deref()
                        .ok_or_else(|| "recorder TLS CA bundle is missing".to_string())?;
                    let tls =
                        RecorderPostcardRpcTlsClientConfig::from_ca_pem(ca_bundle, server_name)?;
                    Box::new(TcpPostcardRpcRecorderClient::new_tls(
                        &endpoint.address,
                        peer.node_id(),
                        config.node_id.clone(),
                        local_token.clone(),
                        config.recovery_generation,
                        tls,
                    )?)
                }
            };
            Ok((peer.node_id().to_owned(), client))
        })
        .collect()
}

fn build_consensus(
    config: &ServeConfig,
    local_recorder: Option<&RecorderFileStore>,
) -> Result<Arc<ThreeNodeConsensus>, String> {
    build_consensus_at_checkpoint(config, local_recorder, None)
}

fn build_consensus_at_checkpoint(
    config: &ServeConfig,
    local_recorder: Option<&RecorderFileStore>,
    recovered_checkpoint: Option<LogAnchor>,
) -> Result<Arc<ThreeNodeConsensus>, String> {
    if let Some(recorder) = local_recorder {
        let recorder_id = recorder.recorder_id().map_err(|error| error.to_string())?;
        if recorder_id != config.node_id {
            return Err(format!(
                "local recorder identity mismatch: expected {}, got {recorder_id}",
                config.node_id
            ));
        }
    }
    let local_token = config.local_peer_token()?.to_owned();
    let tls_ca_bundle = if config.recorder_transport.is_tls() {
        let tls = config
            .recorder_tcp
            .as_ref()
            .and_then(|tcp| tcp.tls.as_ref())
            .ok_or_else(|| "recorder TLS configuration is missing".to_string())?;
        Some(
            fs::read(&tls.ca_bundle)
                .map_err(|error| format!("cannot read recorder TLS CA bundle: {error}"))?,
        )
    } else {
        None
    };
    let recorders = config
        .bundle
        .peers
        .iter()
        .enumerate()
        .map(|(index, peer)| {
            let network_client: Box<dyn RecorderRpc> = match config.recorder_transport {
                RecorderTransport::Http => Box::new(
                    HttpRecorderClient::new_with_recovery_generation(
                        peer.base_url(),
                        config.node_id.clone(),
                        local_token.clone(),
                        config.recovery_generation,
                    )
                    .map_err(|error| error.to_string())?,
                ),
                RecorderTransport::TcpPostcard => {
                    let endpoint = config.bundle.recorder_tcp_peers[index]
                        .as_ref()
                        .ok_or_else(|| {
                            format!("recorder TCP endpoint is missing for {}", peer.node_id())
                        })?;
                    Box::new(TcpPostcardRecorderClient::new(
                        &endpoint.address,
                        peer.node_id(),
                        config.node_id.clone(),
                        local_token.clone(),
                        config.recovery_generation,
                    )?)
                }
                RecorderTransport::TcpTlsPostcard => {
                    let endpoint = config.bundle.recorder_tcp_peers[index]
                        .as_ref()
                        .ok_or_else(|| {
                            format!("recorder TCP endpoint is missing for {}", peer.node_id())
                        })?;
                    let server_name = endpoint.tls_server_name.as_deref().ok_or_else(|| {
                        format!("recorder TLS server name is missing for {}", peer.node_id())
                    })?;
                    let ca_bundle = tls_ca_bundle
                        .as_deref()
                        .ok_or_else(|| "recorder TLS CA bundle is missing".to_string())?;
                    let tls = RecorderTlsClientConfig::from_ca_pem(ca_bundle, server_name)?;
                    Box::new(TcpPostcardRecorderClient::new_tls(
                        &endpoint.address,
                        peer.node_id(),
                        config.node_id.clone(),
                        local_token.clone(),
                        config.recovery_generation,
                        tls,
                    )?)
                }
                #[cfg(feature = "recorder-postcard-rpc")]
                RecorderTransport::TcpPostcardRpc => {
                    let endpoint = config.bundle.recorder_tcp_peers[index]
                        .as_ref()
                        .ok_or_else(|| {
                            format!("recorder TCP endpoint is missing for {}", peer.node_id())
                        })?;
                    Box::new(TcpPostcardRpcRecorderClient::new(
                        &endpoint.address,
                        peer.node_id(),
                        config.node_id.clone(),
                        local_token.clone(),
                        config.recovery_generation,
                    )?)
                }
                #[cfg(feature = "recorder-postcard-rpc")]
                RecorderTransport::TcpTlsPostcardRpc => {
                    let endpoint = config.bundle.recorder_tcp_peers[index]
                        .as_ref()
                        .ok_or_else(|| {
                            format!("recorder TCP endpoint is missing for {}", peer.node_id())
                        })?;
                    let server_name = endpoint.tls_server_name.as_deref().ok_or_else(|| {
                        format!("recorder TLS server name is missing for {}", peer.node_id())
                    })?;
                    let ca_bundle = tls_ca_bundle
                        .as_deref()
                        .ok_or_else(|| "recorder TLS CA bundle is missing".to_string())?;
                    let tls =
                        RecorderPostcardRpcTlsClientConfig::from_ca_pem(ca_bundle, server_name)?;
                    Box::new(TcpPostcardRpcRecorderClient::new_tls(
                        &endpoint.address,
                        peer.node_id(),
                        config.node_id.clone(),
                        local_token.clone(),
                        config.recovery_generation,
                        tls,
                    )?)
                }
            };
            let recorder: Box<dyn RecorderRpc> = if peer.node_id() == config.node_id {
                local_recorder
                    .map(|recorder| Box::new(recorder.clone()) as Box<dyn RecorderRpc>)
                    .unwrap_or(network_client)
            } else {
                network_client
            };
            Ok((peer.node_id().to_owned(), recorder))
        })
        .collect::<Result<Vec<_>, String>>()?;
    let consensus = match recovered_checkpoint {
        Some(checkpoint_root) => {
            let next_index = checkpoint_root
                .index()
                .checked_add(1)
                .ok_or_else(|| "checkpoint root index cannot advance".to_string())?;
            ThreeNodeConsensus::from_recorders_with_ids_and_recovered_tip(
                config.cluster_id.clone(),
                config.node_id.clone(),
                config.epoch,
                config.bundle.config_id,
                recorders,
                next_index,
                checkpoint_root.hash(),
            )
        }
        None => ThreeNodeConsensus::from_recorders_with_ids(
            config.cluster_id.clone(),
            config.node_id.clone(),
            config.epoch,
            config.bundle.config_id,
            recorders,
        ),
    };
    consensus.map(Arc::new).map_err(|error| error.to_string())
}

fn build_log_peers(config: &ServeConfig) -> Result<Vec<HttpLogPeer>, String> {
    let local_token = config.local_peer_token()?.to_owned();
    config
        .bundle
        .peers
        .iter()
        .filter(|peer| peer.node_id() != config.node_id)
        .map(|peer| {
            HttpLogPeer::new_with_recovery_generation(
                peer.log_base_url(),
                config.node_id.clone(),
                local_token.clone(),
                config.recovery_generation,
            )
            .map_err(|error| error.to_string())
        })
        .collect()
}

async fn open_runtime_with_retry(
    node_config: NodeConfig,
    consensus: Arc<ThreeNodeConsensus>,
    peer_candidates: Vec<HttpLogPeer>,
) -> Result<Arc<NodeRuntime>, String> {
    const RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(100);
    let mut last_retry_error = None;

    loop {
        let attempt_config = node_config.clone();
        let attempt_consensus = Arc::clone(&consensus);
        let attempt_peers = peer_candidates.clone();
        let result = tokio::task::spawn_blocking(move || {
            let peer_refs = attempt_peers
                .iter()
                .map(|peer| peer as &dyn LogPeer)
                .collect::<Vec<_>>();
            NodeRuntime::open(attempt_config, attempt_consensus, &peer_refs)
        })
        .await
        .map_err(|error| format!("runtime startup task failed: {error}"))?;

        match result {
            Ok(runtime) => return Ok(Arc::new(runtime)),
            Err(error @ (NodeError::Unavailable(_) | NodeError::Contention(_))) => {
                let message = error.to_string();
                if last_retry_error.as_deref() != Some(message.as_str()) {
                    eprintln!("runtime startup waiting for recorder quorum: {message}");
                    last_retry_error = Some(message);
                }
                tokio::time::sleep(RETRY_DELAY).await;
            }
            Err(error) => return Err(error.to_string()),
        }
    }
}

async fn run_init_checkpoint(config: CheckpointCommandConfig) -> Result<CheckpointTip, String> {
    initialize_empty_checkpoint(&config.archive()?).await
}

async fn run_roll_checkpoint(
    config: RollCheckpointConfig,
) -> Result<(CheckpointTip, CheckpointTip), String> {
    if config.from_generation.checked_add(1) != Some(config.to_generation) {
        return Err("target recovery generation must equal source generation + 1".into());
    }
    let source = config.archive_for_generation(config.from_generation)?;
    let target = config.archive_for_generation(config.to_generation)?;
    roll_checkpoint(&source, &target).await
}

async fn run_checkpoint_inspect(config: CheckpointCommandConfig) -> Result<String, String> {
    let loaded = config
        .archive()?
        .load_checkpoint()
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "checkpoint is not initialized".to_string())?;
    serde_json::to_string(loaded.manifest()).map_err(|error| error.to_string())
}

async fn run_checkpoint_compact(config: AdminCommandConfig) -> Result<String, String> {
    match config.target {
        AdminTarget::Offline => {
            run_checkpoint_compact_offline(*required_serve_config(config.serve)?).await
        }
        AdminTarget::Live(client) => {
            let serve = required_serve_config(config.serve)?;
            let status: AdminStatusResponse = admin_get(&client, ADMIN_STATUS_PATH).await?;
            validate_live_admin_target(&status, &serve, &[serve.bundle.config_id])?;
            let response: AdminCompactResponse = admin_post(
                &client,
                ADMIN_COMPACT_PATH,
                &AdminCompactRequest {
                    operation_id: required_operation_id(config.operation_id)?,
                    expected_config_id: serve.bundle.config_id,
                    expected_recovery_generation: serve.recovery_generation,
                    expected_root: status.qlog_root,
                },
            )
            .await?;
            serde_json::to_string(&response).map_err(|error| error.to_string())
        }
    }
}

async fn run_checkpoint_compact_offline(config: ServeConfig) -> Result<String, String> {
    let remote = config
        .remote
        .as_ref()
        .ok_or_else(|| "checkpoint compact requires RHIZA_OBJECT_STORE".to_string())?;
    let store = open_object_store(&remote.object_store)?;
    let archive = ObjectArchiveStore::new_checkpoint(
        store,
        CheckpointIdentity::new(
            config.cluster_id.clone(),
            config.epoch,
            config.bundle.config_id,
            config.bundle.membership.digest(),
            config.recovery_generation,
        ),
    )
    .map_err(|error| error.to_string())?;
    let runtime = open_offline_runtime(&config)?;
    let coordinator =
        CheckpointCoordinator::open_with_holder(archive, DurabilityMode::Sync, &config.node_id)
            .await
            .map_err(|error| error.to_string())?;
    let anchor = runtime
        .checkpoint_compact(&coordinator)
        .await
        .map_err(|error| error.to_string())?;
    serde_json::to_string(&anchor).map_err(|error| error.to_string())
}

async fn run_gc_plan(config: GcPlanConfig) -> Result<String, String> {
    let archive = config.base.archive()?;
    let now = unix_time_ms()?;
    let identity = config.base.identity();
    archive
        .set_gc_root(identity.clone(), now)
        .await
        .map_err(|error| error.to_string())?;
    let plan = archive
        .plan_gc(
            GcPolicy::new(
                config.operation_id,
                identity,
                config.retain_generations,
                config.grace_ms,
                config.min_age_ms,
            ),
            now,
        )
        .await
        .map_err(|error| error.to_string())?;
    serde_json::to_string(&plan).map_err(|error| error.to_string())
}

async fn run_gc_inspect(config: GcInspectConfig) -> Result<String, String> {
    let store = open_object_store(&config.base.object_store)?;
    let archive = ObjectArchiveStore::new_checkpoint(store.clone(), config.base.identity())
        .map_err(|error| error.to_string())?;
    let plan_bytes = store
        .get(&archive.gc_plan_key(&config.plan_hash))
        .await
        .map_err(|error| redact_object_store_error(&config.base.object_store, error.to_string()))?;
    let plan: GcPlan =
        serde_json::from_slice(&plan_bytes).map_err(|error| format!("invalid GC plan: {error}"))?;
    validate_gc_plan_identity(&plan, &config.base.identity(), &config.plan_hash)?;
    let mut evidence = Vec::new();
    for key in store
        .list(&archive.gc_evidence_prefix(&config.plan_hash))
        .await
        .map_err(|error| redact_object_store_error(&config.base.object_store, error.to_string()))?
    {
        let bytes = store.get(&key).await.map_err(|error| {
            redact_object_store_error(&config.base.object_store, error.to_string())
        })?;
        evidence.push(
            serde_json::from_slice::<serde_json::Value>(&bytes)
                .map_err(|error| format!("invalid GC evidence: {error}"))?,
        );
    }
    serde_json::to_string(&serde_json::json!({"plan": plan, "evidence": evidence}))
        .map_err(|error| error.to_string())
}

async fn run_gc_apply(config: GcInspectConfig) -> Result<String, String> {
    let store = open_object_store(&config.base.object_store)?;
    let archive = ObjectArchiveStore::new_checkpoint(store.clone(), config.base.identity())
        .map_err(|error| error.to_string())?;
    let plan_bytes = store
        .get(&archive.gc_plan_key(&config.plan_hash))
        .await
        .map_err(|error| redact_object_store_error(&config.base.object_store, error.to_string()))?;
    let plan: GcPlan =
        serde_json::from_slice(&plan_bytes).map_err(|error| format!("invalid GC plan: {error}"))?;
    validate_gc_plan_identity(&plan, &config.base.identity(), &config.plan_hash)?;
    let report = archive
        .execute_gc(&config.plan_hash, unix_time_ms()?)
        .await
        .map_err(|error| error.to_string())?;
    serde_json::to_string(&report).map_err(|error| error.to_string())
}

fn validate_gc_plan_identity(
    plan: &GcPlan,
    expected: &CheckpointIdentity,
    plan_hash: &str,
) -> Result<(), String> {
    if plan.plan_hash() != plan_hash {
        return Err("stored GC plan hash does not match --plan-hash".into());
    }
    if plan.root() != expected {
        return Err(
            "GC plan generation/config identity does not match the command environment".into(),
        );
    }
    Ok(())
}

fn unix_time_ms() -> Result<u64, String> {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| format!("system clock is before Unix epoch: {error}"))?
        .as_millis();
    u64::try_from(millis).map_err(|_| "system time exceeds supported range".into())
}

fn open_offline_runtime(config: &ServeConfig) -> Result<NodeRuntime, String> {
    let node_config = config.node_config()?;
    let consensus = build_consensus(config, None)?;
    NodeRuntime::open(node_config, consensus, &[]).map_err(|error| error.to_string())
}

async fn run_membership_status(config: AdminCommandConfig) -> Result<String, String> {
    match config.target {
        AdminTarget::Live(client) => {
            let status: AdminStatusResponse = admin_get(&client, ADMIN_STATUS_PATH).await?;
            serde_json::to_string(&status).map_err(|error| error.to_string())
        }
        AdminTarget::Offline => {
            let serve = required_serve_config(config.serve)?;
            let runtime = open_offline_runtime(&serve)?;
            let status = runtime.status().map_err(|error| error.to_string())?;
            serde_json::to_string(&status).map_err(|error| error.to_string())
        }
    }
}

async fn run_membership_stop(config: AdminCommandConfig) -> Result<String, String> {
    let successor = config.successor.clone().ok_or_else(|| {
        "membership stop requires RHIZA_SUCCESSOR_CONFIG_BUNDLE or RHIZA_SUCCESSOR_CONFIG_BUNDLE_FILE"
            .to_string()
    })?;
    match config.target {
        AdminTarget::Live(client) => {
            let serve = required_serve_config(config.serve)?;
            if serve.bundle.predecessor.is_some() {
                return Err(
                    "cannot stop a successor bundle; use the active configuration bundle".into(),
                );
            }
            let status: AdminStatusResponse = admin_get(&client, ADMIN_STATUS_PATH).await?;
            validate_live_admin_target(&status, &serve, &[serve.bundle.config_id])?;
            let successor = successor_descriptor(&serve, &successor)?;
            let response: AdminStopResponse = admin_post(
                &client,
                ADMIN_STOP_PATH,
                &AdminStopRequest {
                    operation_id: required_operation_id(config.operation_id)?,
                    expected_config_id: serve.bundle.config_id,
                    successor,
                },
            )
            .await?;
            let material = PredecessorDocument {
                members: serve.bundle.membership.members().to_vec(),
                stop_entry: response.stop.entry,
                stop_proof: response.stop.proof,
            };
            serde_json::to_string(&material).map_err(|error| error.to_string())
        }
        AdminTarget::Offline => {
            run_membership_stop_offline(*required_serve_config(config.serve)?, successor)
        }
    }
}

fn run_membership_stop_offline(
    config: ServeConfig,
    successor: ConfigurationBundle,
) -> Result<String, String> {
    if config.bundle.predecessor.is_some() {
        return Err("cannot stop a successor bundle; use the active configuration bundle".into());
    }
    successor_descriptor(&config, &successor)?;
    let runtime = open_offline_runtime(&config)?;
    let stop = runtime
        .stop_current_configuration_for_successor(&successor.membership)
        .map_err(|error| error.to_string())?;
    let material = PredecessorDocument {
        members: config.bundle.membership.members().to_vec(),
        stop_entry: stop.entry,
        stop_proof: stop.proof,
    };
    serde_json::to_string(&material).map_err(|error| error.to_string())
}

fn successor_descriptor(
    predecessor: &ServeConfig,
    successor: &ConfigurationBundle,
) -> Result<AdminSuccessorBundle, String> {
    if successor.predecessor.is_some()
        || predecessor.bundle.config_id.checked_add(1) != Some(successor.config_id)
    {
        return Err(
            "successor bundle must be an unactivated config with config_id predecessor + 1".into(),
        );
    }
    Ok(AdminSuccessorBundle {
        config_id: successor.config_id,
        members: successor.membership.members().to_vec(),
        digest: successor.membership.digest(),
    })
}

async fn run_membership_install_successor(config: AdminCommandConfig) -> Result<String, String> {
    match config.target {
        AdminTarget::Live(client) => {
            let serve = required_serve_config(config.serve)?;
            serve.node_config()?;
            let predecessor = serve.bundle.require_predecessor()?;
            let status: AdminStatusResponse = admin_get(&client, ADMIN_STATUS_PATH).await?;
            validate_live_admin_target(&status, &serve, &[predecessor.stop.entry.config_id])?;
            let response: AdminInstallSuccessorResponse = admin_post(
                &client,
                ADMIN_INSTALL_SUCCESSOR_PATH,
                &AdminInstallSuccessorRequest {
                    operation_id: required_operation_id(config.operation_id)?,
                    expected_config_id: predecessor.stop.entry.config_id,
                    expected_stopped_anchor: LogAnchor::new(
                        predecessor.stop.entry.index,
                        predecessor.stop.entry.hash,
                    ),
                    old_members: predecessor.membership.members().to_vec(),
                    stop: predecessor.stop.clone(),
                    successor: AdminSuccessorBundle {
                        config_id: serve.bundle.config_id,
                        members: serve.bundle.membership.members().to_vec(),
                        digest: serve.bundle.membership.digest(),
                    },
                },
            )
            .await?;
            serde_json::to_string(&response).map_err(|error| error.to_string())
        }
        AdminTarget::Offline => {
            run_membership_install_successor_offline(*required_serve_config(config.serve)?)
        }
    }
}

fn run_membership_install_successor_offline(config: ServeConfig) -> Result<String, String> {
    config.node_config()?;
    let predecessor = config.bundle.require_predecessor()?;
    if predecessor.stop.entry.cluster_id != config.cluster_id
        || predecessor.stop.entry.epoch != config.epoch
        || predecessor.stop.entry.config_id.checked_add(1) != Some(config.bundle.config_id)
    {
        return Err(
            "predecessor certificate cluster/epoch/config does not match the successor bundle"
                .into(),
        );
    }
    let recorder = RecorderFileStore::new_with_membership(
        config.data_dir.join("recorder"),
        config.node_id.clone(),
        config.cluster_id,
        config.epoch,
        predecessor.stop.entry.config_id,
        predecessor.membership.clone(),
    )
    .map_err(|error| error.to_string())?;
    let state = install_successor_recorder(
        &recorder,
        config.bundle.config_id,
        config.bundle.membership.clone(),
        &predecessor.stop,
    )
    .map_err(|error| error.to_string())?;
    serde_json::to_string(&serde_json::json!({
        "config_id": state.config_id(),
        "config_digest": state.config_digest(),
        "activated": state.is_activated(),
        "predecessor_installed": state.predecessor().is_some(),
    }))
    .map_err(|error| error.to_string())
}

async fn run_membership_activate(config: AdminCommandConfig) -> Result<String, String> {
    match config.target {
        AdminTarget::Live(client) => {
            let serve = required_serve_config(config.serve)?;
            let predecessor_config_id = serve.bundle.require_predecessor()?.stop.entry.config_id;
            let status: AdminStatusResponse = admin_get(&client, ADMIN_STATUS_PATH).await?;
            validate_live_admin_target(
                &status,
                &serve,
                &[predecessor_config_id, serve.bundle.config_id],
            )?;
            let response: AdminActivateResponse = admin_post(
                &client,
                ADMIN_ACTIVATE_PATH,
                &AdminActivateRequest {
                    operation_id: required_operation_id(config.operation_id)?,
                    expected_config_id: serve.bundle.config_id,
                },
            )
            .await?;
            serde_json::to_string(&response).map_err(|error| error.to_string())
        }
        AdminTarget::Offline => {
            run_membership_activate_offline(*required_serve_config(config.serve)?)
        }
    }
}

fn run_membership_activate_offline(config: ServeConfig) -> Result<String, String> {
    config.bundle.require_predecessor()?;
    let runtime = open_offline_runtime(&config)?;
    let entry = runtime
        .activate_successor()
        .map_err(|error| error.to_string())?;
    serde_json::to_string(&entry).map_err(|error| error.to_string())
}

fn required_serve_config(config: Option<Box<ServeConfig>>) -> Result<Box<ServeConfig>, String> {
    config.ok_or_else(|| "local serve configuration is required".into())
}

fn required_operation_id(operation_id: Option<String>) -> Result<String, String> {
    operation_id.ok_or_else(|| "admin operation id is required".into())
}

fn validate_live_admin_target(
    status: &AdminStatusResponse,
    serve: &ServeConfig,
    allowed_state_config_ids: &[u64],
) -> Result<(), String> {
    let expected_members = serve.bundle.membership.members();
    let actual_state_config_id = status.node.configuration_state.config_id();
    let mismatch = if status.cluster_id != serve.cluster_id {
        Some("cluster_id")
    } else if status.execution_profile != serve.execution_profile {
        Some("execution_profile")
    } else if status.epoch != serve.epoch {
        Some("epoch")
    } else if status.recovery_generation != serve.recovery_generation {
        Some("recovery_generation")
    } else if status.node.active_config_id != serve.bundle.config_id
        || !allowed_state_config_ids.contains(&actual_state_config_id)
    {
        Some("config_id")
    } else if status.members.as_slice() != expected_members
        || status.node.active_membership_digest != serve.bundle.membership.digest()
    {
        Some("membership")
    } else {
        None
    };
    match mismatch {
        Some(field) => Err(format!(
            "admin target fence mismatch for {field}; refusing mutating request"
        )),
        None => Ok(()),
    }
}

async fn admin_get<T: DeserializeOwned>(
    config: &AdminClientConfig,
    path: &str,
) -> Result<T, String> {
    let client = bounded_http_client(ADMIN_CONNECT_TIMEOUT, ADMIN_REQUEST_TIMEOUT)?;
    admin_get_with_client(config, path, &client).await
}

async fn admin_get_with_client<T: DeserializeOwned>(
    config: &AdminClientConfig,
    path: &str,
    client: &reqwest::Client,
) -> Result<T, String> {
    let response = client
        .get(admin_url(config, path))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth(&config.token)
        .send()
        .await
        .map_err(admin_request_error)?;
    decode_admin_response(response).await
}

async fn admin_post<T: Serialize, R: DeserializeOwned>(
    config: &AdminClientConfig,
    path: &str,
    body: &T,
) -> Result<R, String> {
    let response = bounded_http_client(ADMIN_CONNECT_TIMEOUT, ADMIN_REQUEST_TIMEOUT)?
        .post(admin_url(config, path))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .bearer_auth(&config.token)
        .json(body)
        .send()
        .await
        .map_err(admin_request_error)?;
    decode_admin_response(response).await
}

fn admin_url(config: &AdminClientConfig, path: &str) -> String {
    format!("{}{path}", config.url.trim_end_matches('/'))
}

fn admin_request_error(error: reqwest::Error) -> String {
    format!("admin request failed: {}", error.without_url())
}

fn bounded_http_client(
    connect_timeout: Duration,
    timeout: Duration,
) -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .connect_timeout(connect_timeout)
        .timeout(timeout)
        .build()
        .map_err(|error| format!("cannot build HTTP client: {error}"))
}

async fn decode_admin_response<T: DeserializeOwned>(response: Response) -> Result<T, String> {
    let status = response.status();
    if status.is_success() {
        return response
            .json::<T>()
            .await
            .map_err(|error| format!("invalid admin response: {error}"));
    }
    let code = response
        .json::<AdminErrorResponse>()
        .await
        .map(|error| format!("{:?}", error.code))
        .unwrap_or_else(|_| "unknown".into());
    Err(format!("admin request failed: status={status} code={code}"))
}

impl RollCheckpointConfig {
    fn archive_for_generation(&self, generation: u64) -> Result<ObjectArchiveStore, String> {
        let mut config = self.base.clone();
        config.recovery_generation = generation;
        config.archive()
    }
}

async fn initialize_empty_checkpoint(
    archive: &ObjectArchiveStore,
) -> Result<CheckpointTip, String> {
    let loaded = archive
        .initialize_checkpoint()
        .await
        .map_err(|error| error.to_string())?;
    if loaded.manifest().tip().index() != 0 || !loaded.manifest().segments().is_empty() {
        return Err("refusing to initialize a nonempty checkpoint".into());
    }
    Ok(*loaded.manifest().tip())
}

async fn roll_checkpoint(
    source: &ObjectArchiveStore,
    target: &ObjectArchiveStore,
) -> Result<(CheckpointTip, CheckpointTip), String> {
    let source_before = source
        .load_checkpoint()
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "source checkpoint is not initialized".to_string())?;
    if source_before.manifest().base().snapshot().is_some() {
        let published = source
            .roll_recovery_generation(target)
            .await
            .map_err(|error| error.to_string())?;
        let source_tip = *source_before.manifest().tip();
        if published.manifest().tip() != &source_tip {
            return Err("target checkpoint does not exactly match source after roll".into());
        }
        return Ok((source_tip, *published.manifest().tip()));
    }
    let entries = source
        .load_checkpoint_restore()
        .await
        .map_err(|error| format!("source checkpoint verification failed: {error}"))?
        .ok_or_else(|| "source checkpoint disappeared during roll".to_string())?
        .restored()
        .suffix()
        .to_vec();
    let source_after = source
        .load_checkpoint()
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "source checkpoint disappeared during roll".to_string())?;
    if source_before != source_after {
        return Err("source checkpoint changed during roll; old pods must be stopped".into());
    }
    target
        .initialize_checkpoint()
        .await
        .map_err(|error| error.to_string())?;
    let published = target
        .publish_committed(&entries)
        .await
        .map_err(|error| error.to_string())?;
    let target_entries = target
        .load_checkpoint_restore()
        .await
        .map_err(|error| format!("target checkpoint verification failed: {error}"))?
        .ok_or_else(|| "target checkpoint disappeared during roll".to_string())?
        .restored()
        .suffix()
        .to_vec();
    let source_tip = *source_after.manifest().tip();
    let target_tip = target_entries
        .last()
        .map(|entry| CheckpointTip::new(entry.index, entry.hash))
        .unwrap_or(source_tip);
    if target_entries != entries
        || target_tip != source_tip
        || *published.manifest().tip() != source_tip
    {
        return Err("target checkpoint does not exactly match source after roll".into());
    }
    Ok((source_tip, target_tip))
}

#[cfg(feature = "sql")]
async fn request_write(args: &WriteArgs) -> Result<WriteResponse, String> {
    let request = WriteRequest {
        request_id: args.request_id.clone(),
        key: args.key.clone(),
        value: args.value.clone(),
    };
    remote_client(&args.urls, &args.token)?
        .write(request)
        .await
        .map_err(|error| error.to_string())
}

#[cfg(feature = "sql")]
async fn request_read(args: &ReadArgs) -> Result<ReadResponse, String> {
    let request = ReadRequest {
        key: args.key.clone(),
        consistency: args.consistency,
    };
    remote_client(&args.urls, &args.token)?
        .read(request)
        .await
        .map_err(|error| error.to_string())
}

#[cfg(feature = "sql")]
async fn request_sql_execute(args: &SqlExecuteArgs) -> Result<SqlExecuteResponse, String> {
    let request = SqlExecuteRequest {
        request_id: args.request_id.clone(),
        statements: vec![args.statement.clone()],
    };
    remote_client(&args.urls, &args.token)?
        .sql_execute(request)
        .await
        .map_err(|error| error.to_string())
}

#[cfg(feature = "sql")]
async fn request_sql_query(args: &SqlQueryArgs) -> Result<SqlQueryResponse, String> {
    let request = SqlQueryRequest {
        statement: args.statement.clone(),
        consistency: args.consistency,
        max_rows: args.max_rows,
    };
    remote_client(&args.urls, &args.token)?
        .sql_query(request)
        .await
        .map_err(|error| error.to_string())
}

#[cfg(feature = "graph")]
async fn request_graph_query(args: &GraphQueryArgs) -> Result<GraphQueryResponse, String> {
    let request = GraphQueryRequest {
        statement: args.statement.clone(),
        consistency: args.consistency,
        max_rows: args.max_rows,
    };
    remote_client(&args.urls, &args.token)?
        .graph_query(request)
        .await
        .map_err(|error| error.to_string())
}

#[cfg(feature = "graph")]
async fn request_graph_put_document(
    args: &GraphPutDocumentArgs,
) -> Result<GraphMutationResponse, String> {
    remote_client(&args.urls, &args.token)?
        .graph_put_document(args.request.clone())
        .await
        .map_err(|error| error.to_string())
}

#[cfg(feature = "graph")]
async fn request_graph_delete_document(
    args: &GraphDeleteDocumentArgs,
) -> Result<GraphMutationResponse, String> {
    remote_client(&args.urls, &args.token)?
        .graph_delete_document(args.request.clone())
        .await
        .map_err(|error| error.to_string())
}

#[cfg(feature = "graph")]
async fn request_graph_get_document(
    args: &GraphGetDocumentArgs,
) -> Result<GraphGetDocumentResponse, String> {
    remote_client(&args.urls, &args.token)?
        .graph_get_document(args.request.clone())
        .await
        .map_err(|error| error.to_string())
}

#[cfg(feature = "kv")]
async fn request_kv_get(args: &KvGetArgs) -> Result<KvGetResponse, String> {
    remote_client(&args.urls, &args.token)?
        .kv_get(args.request.clone())
        .await
        .map_err(|error| error.to_string())
}

#[cfg(feature = "kv")]
async fn request_kv_scan(args: &KvScanArgs) -> Result<KvScanResponse, String> {
    remote_client(&args.urls, &args.token)?
        .kv_scan(args.request.clone())
        .await
        .map_err(|error| error.to_string())
}

#[cfg(feature = "kv")]
async fn request_kv_put(args: &KvPutArgs) -> Result<KvMutationResponse, String> {
    remote_client(&args.urls, &args.token)?
        .kv_put(args.request.clone())
        .await
        .map_err(|error| error.to_string())
}

#[cfg(feature = "kv")]
async fn request_kv_delete(args: &KvDeleteArgs) -> Result<KvMutationResponse, String> {
    remote_client(&args.urls, &args.token)?
        .kv_delete(args.request.clone())
        .await
        .map_err(|error| error.to_string())
}

#[cfg(feature = "kv")]
async fn request_kv_batch(args: &KvBatchArgs) -> Result<KvBatchResponse, String> {
    remote_client(&args.urls, &args.token)?
        .kv_batch(args.request.clone())
        .await
        .map_err(|error| error.to_string())
}

fn remote_client(urls: &[String], token: &str) -> Result<RhizaClient, String> {
    RhizaClient::new(urls.iter().cloned(), token).map_err(|error| error.to_string())
}

async fn request_health(args: &HealthArgs) -> Result<(), String> {
    let client = bounded_http_client(HEALTH_CONNECT_TIMEOUT, HEALTH_REQUEST_TIMEOUT)?;
    request_health_with_client(args, &client).await
}

async fn request_health_with_client(
    args: &HealthArgs,
    client: &reqwest::Client,
) -> Result<(), String> {
    let path = if args.ready { READYZ_PATH } else { LIVEZ_PATH };
    let response = protocol_request(client, Method::GET, &args.url, path)
        .send()
        .await
        .map_err(request_error)?;
    success_response(response).await.map(|_| ())
}

fn protocol_request(
    client: &reqwest::Client,
    method: Method,
    url: &str,
    path: &str,
) -> RequestBuilder {
    client
        .request(method, endpoint(url, path))
        .header(VERSION_HEADER, PROTOCOL_VERSION)
        .header(header::ACCEPT, "application/json")
        .header(header::CONTENT_TYPE, "application/json")
}

fn endpoint(url: &str, path: &str) -> String {
    format!("{}{path}", url.trim_end_matches('/'))
}

fn request_error(error: reqwest::Error) -> String {
    format!("request failed: {}", error.without_url())
}

#[derive(Deserialize)]
struct ServerErrorResponse {
    code: String,
    #[serde(default)]
    message: Option<String>,
}

async fn success_response(response: Response) -> Result<Response, String> {
    if response.status().is_success() {
        return Ok(response);
    }

    let status = response.status();
    let Ok(body) = response.bytes().await else {
        return Err(format!("HTTP {status}"));
    };
    let Ok(error) = serde_json::from_slice::<ServerErrorResponse>(&body) else {
        return Err(format!("HTTP {status}"));
    };
    Err(server_error_detail(status, error))
}

fn server_error_detail(status: reqwest::StatusCode, error: ServerErrorResponse) -> String {
    let mut detail = format!("HTTP {status} code={}", error.code);
    if let Some(message) = error.message.filter(|message| !message.is_empty()) {
        detail.push_str(" message=");
        detail.push_str(&message);
    }
    detail
}

#[cfg(feature = "sql")]
fn finish_read(args: &ReadArgs, response: ReadResponse) -> i32 {
    if let Some(expected) = &args.expect {
        if response.value.as_deref() != Some(expected) {
            let actual = response.value.as_deref().unwrap_or("<missing>");
            eprintln!("read failed: expected {expected:?}, got {actual:?}");
            return 1;
        }
    }
    let value = response
        .value
        .as_ref()
        .map(|value| serde_json::to_string(value).expect("string serialization cannot fail"))
        .unwrap_or_else(|| "null".into());
    println!(
        "value={value} applied_index={} hash={}",
        response.applied_index,
        response.hash.to_hex()
    );
    0
}

fn fail(context: &str, error: impl std::fmt::Display) -> i32 {
    eprintln!("{context} failed: {error}");
    1
}

#[cfg(feature = "sql")]
fn parse_object_store(value: &str) -> Result<ObjStoreConfig, String> {
    parse_object_store_with_lookup(value, true, &mut |name| env::var(name).ok())
}

fn parse_object_store_with_lookup(
    value: &str,
    allow_local: bool,
    lookup: &mut impl FnMut(&str) -> Option<String>,
) -> Result<ObjStoreConfig, String> {
    if let Some(root) = value.strip_prefix("local:") {
        if !allow_local {
            return Err("local:<path> object stores are only supported by e2e".into());
        }
        if root.is_empty() {
            return Err("local object store path must not be empty".into());
        }
        return Ok(ObjStoreConfig::Local {
            root: PathBuf::from(root),
        });
    }
    match value {
        "s3" => {
            let endpoint = optional_env(lookup, "RHIZA_S3_ENDPOINT")?;
            let access_key = optional_env(lookup, "RHIZA_S3_ACCESS_KEY")?;
            let secret_key = optional_env(lookup, "RHIZA_S3_SECRET_KEY")?;
            if access_key.is_some() != secret_key.is_some() {
                return Err(
                    "RHIZA_S3_ACCESS_KEY and RHIZA_S3_SECRET_KEY must be set together".into(),
                );
            }
            Ok(ObjStoreConfig::S3 {
                endpoint,
                bucket: required_env(lookup, "RHIZA_S3_BUCKET")?,
                access_key,
                secret_key,
                region: lookup("RHIZA_S3_REGION").unwrap_or_else(|| "us-east-1".into()),
                allow_http: parse_optional_bool(lookup, "RHIZA_S3_ALLOW_HTTP")?.unwrap_or(false),
            })
        }
        "gcs" => {
            let service_account_path = optional_env(lookup, "RHIZA_GCS_SERVICE_ACCOUNT_PATH")?;
            let service_account_key = optional_env(lookup, "RHIZA_GCS_SERVICE_ACCOUNT_KEY")?;
            if service_account_path.is_some() && service_account_key.is_some() {
                return Err(
                    "RHIZA_GCS_SERVICE_ACCOUNT_PATH and RHIZA_GCS_SERVICE_ACCOUNT_KEY are mutually exclusive"
                        .into(),
                );
            }
            Ok(ObjStoreConfig::Gcs {
                bucket: required_env(lookup, "RHIZA_GCS_BUCKET")?,
                service_account_path,
                service_account_key,
            })
        }
        "azure" => Ok(ObjStoreConfig::AzureBlob {
            account: required_env(lookup, "RHIZA_AZURE_ACCOUNT")?,
            container: required_env(lookup, "RHIZA_AZURE_CONTAINER")?,
            access_key: optional_env(lookup, "RHIZA_AZURE_ACCESS_KEY")?,
        }),
        _ => Err("RHIZA_OBJECT_STORE must be s3|gcs|azure (local:<path> is e2e-only)".into()),
    }
}

fn parse_optional_bool(
    lookup: &mut impl FnMut(&str) -> Option<String>,
    name: &str,
) -> Result<Option<bool>, String> {
    match lookup(name).as_deref() {
        None => Ok(None),
        Some("true" | "1") => Ok(Some(true)),
        Some("false" | "0") => Ok(Some(false)),
        Some(_) => Err(format!("{name} must be true|false|1|0")),
    }
}

const USAGE: &str = "usage:\n  rhiza status --url <url>\n  rhiza e2e [options]\n  rhiza serve\n  rhiza prestage serve\n  rhiza validate-config-bundle [--stdin]\n  rhiza init-checkpoint\n  rhiza roll-checkpoint [--from-generation N --to-generation N+1]\n  rhiza checkpoint inspect\n  rhiza checkpoint compact\n  rhiza gc plan --operation-id <id> [--retain-generations N --grace-ms N --min-age-ms N]\n  rhiza gc inspect|evidence --plan-hash <sha256>\n  rhiza gc apply --plan-hash <sha256> --confirm\n  rhiza membership status|stop|install-successor|activate [--offline]\n  rhiza write --url <preferred> [--url <fallback> ...] [--token <token>] --request-id <id> --key <key> --value <value>\n  rhiza read --url <preferred> [--url <fallback> ...] [--token <token>] --key <key> [--consistency local|read_barrier|applied_index:N] [--expect <value>]\n  rhiza sql execute --url <preferred> [--url <fallback> ...] [--token <token>] --request-id <id> --sql <sql> [--params-json <json>]\n  rhiza sql query --url <preferred> [--url <fallback> ...] [--token <token>] --sql <sql> [--params-json <json>] [--consistency local|read_barrier|applied_index:N] [--max-rows N]\n  rhiza graph query --url <preferred> [--url <fallback> ...] [--token <token>] --cypher <cypher> [--params-json <typed-json-object>] [--consistency local|read_barrier|applied_index:N] [--max-rows N]\n  rhiza kv get|scan|put|delete --url <preferred> [--url <fallback> ...] [--token <token>] [options]\n  rhiza health --url <url> [--ready]\n\nServe, prestage, checkpoint, recovery, GC, and offline membership commands require RHIZA_EXECUTION_PROFILE=sql|graph|kv and RHIZA_CONFIG_BUNDLE or RHIZA_CONFIG_BUNDLE_FILE. prestage serve additionally requires RHIZA_PRESTAGE_SOURCE_BUNDLE_FILE, RHIZA_PRESTAGE_TRANSITION_BUNDLE_FILE, and a distinct RHIZA_TAIL_TOKEN. Repeat --url in preferred order. Idempotent operations hedge later endpoints after 100 ms; read_barrier operations retry sequentially. Every attempt reuses the exact request body, including write request IDs and read consistency. Client requests use a 2 s connect deadline, 5 s per-attempt deadline, and 15 s total operation deadline. Membership and checkpoint compact commands use the live admin API by default; pass --offline only as an explicit local fallback while the data root is not serving. gc plan is dry-run only; deletion requires gc apply with the exact plan hash and --confirm. roll-checkpoint performs explicit full-cluster disaster-recovery fencing; stop all old-generation pods before running it.";

fn usage() {
    eprintln!("{USAGE}");
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    #[cfg(all(feature = "graph", feature = "kv"))]
    use std::sync::{Arc, Mutex};

    #[cfg(all(feature = "graph", feature = "kv"))]
    use axum::routing::post;
    use axum::{http::StatusCode, routing::get, Json, Router};
    use rhiza_archive::{CheckpointIdentity, ObjectArchiveStore};
    use rhiza_core::{ConfigChange, EntryType, LogAnchor, LogEntry, LogHash};
    #[cfg(feature = "sql")]
    use rhiza_core::{RecoveryAnchor, SnapshotIdentity};
    #[cfg(feature = "graph")]
    use rhiza_node::GraphQueryParameterDto;
    use rhiza_node::{NodeStatus, RuntimeConfigurationStatus};
    #[cfg(all(feature = "graph", feature = "kv"))]
    use rhiza_node::{GRAPH_QUERY_PATH, KV_SCAN_PATH};
    use rhiza_obj_store::{ObjStore, ObjStoreConfig};
    use rhiza_quepaxa::AcceptedValue;
    #[cfg(feature = "sql")]
    use rhiza_sql::{
        encode_sql_command, sql_executor_fingerprint, SqlBatchMember, SqlCommand,
        SqliteStateMachine,
    };

    use super::*;

    #[test]
    #[cfg(feature = "sql")]
    fn e2e_parser_rejects_the_removed_verify_restore_alias() {
        assert!(matches!(parse_command(["e2e"]), Ok(Command::E2e(_))));
        assert_eq!(
            parse_command(["verify-restore"])
                .err()
                .expect("removed alias must be rejected"),
            "unknown command: verify-restore"
        );
        assert!(USAGE.contains("rhiza e2e [options]"));
        assert!(!USAGE.contains("verify-restore"));
        assert!(USAGE.contains("rhiza graph query"));
        assert!(USAGE.contains("rhiza kv get|scan|put|delete"));
        assert!(USAGE.contains("RHIZA_EXECUTION_PROFILE=sql|graph|kv"));
    }

    #[test]
    #[cfg(feature = "sql")]
    fn write_parser_builds_request_when_all_flags_are_present() {
        let command = parse_command([
            "write",
            "--url",
            "http://127.0.0.1:8080",
            "--token",
            "client-secret",
            "--request-id",
            "request-1",
            "--key",
            "alpha",
            "--value",
            "one",
        ])
        .unwrap();

        let Command::Write(args) = command else {
            panic!("expected write command");
        };
        assert_eq!(args.urls, ["http://127.0.0.1:8080"]);
        assert_eq!(args.token, "client-secret");
        assert_eq!(args.request_id, "request-1");
        assert_eq!(args.key, "alpha");
        assert_eq!(args.value, "one");
    }

    #[test]
    #[cfg(feature = "sql")]
    fn client_parsers_preserve_ordered_repeated_endpoints() {
        let command = parse_command([
            "write",
            "--url",
            "http://preferred:8080",
            "--url",
            "http://fallback:8080",
            "--token",
            "client-secret",
            "--request-id",
            "request-1",
            "--key",
            "alpha",
            "--value",
            "one",
        ])
        .unwrap();

        let Command::Write(args) = command else {
            panic!("expected write command");
        };
        assert_eq!(args.urls, ["http://preferred:8080", "http://fallback:8080"]);

        let command = parse_command([
            "sql",
            "query",
            "--url",
            "http://preferred:8080",
            "--url",
            "http://fallback:8080",
            "--token",
            "client-secret",
            "--sql",
            "SELECT 1",
            "--consistency",
            "read_barrier",
        ])
        .unwrap();

        let Command::SqlQuery(args) = command else {
            panic!("expected sql query command");
        };
        assert_eq!(args.urls, ["http://preferred:8080", "http://fallback:8080"]);
        assert_eq!(args.consistency, Some(ReadConsistency::ReadBarrier));
    }

    #[test]
    #[cfg(feature = "sql")]
    fn write_uses_environment_client_token_when_flag_is_absent() {
        let args = parse_write_with_lookup(
            [
                "--url",
                "http://127.0.0.1:8080",
                "--request-id",
                "request-1",
                "--key",
                "alpha",
                "--value",
                "one",
            ]
            .map(String::from),
            |name| (name == "RHIZA_CLIENT_TOKEN").then(|| "environment-secret".into()),
        )
        .unwrap();

        assert_eq!(args.token, "environment-secret");
    }

    #[test]
    #[cfg(feature = "sql")]
    fn read_uses_environment_client_token_when_flag_is_absent() {
        let args = parse_read_with_lookup(
            ["--url", "http://127.0.0.1:8080", "--key", "alpha"].map(String::from),
            |name| (name == "RHIZA_CLIENT_TOKEN").then(|| "environment-secret".into()),
        )
        .unwrap();

        assert_eq!(args.token, "environment-secret");
    }

    #[test]
    #[cfg(feature = "sql")]
    fn read_parser_rejects_unknown_consistency() {
        let error = parse_command([
            "read",
            "--url",
            "http://127.0.0.1:8080",
            "--token",
            "client-secret",
            "--key",
            "alpha",
            "--consistency",
            "eventual",
        ])
        .err()
        .expect("invalid consistency should fail");

        assert_eq!(
            error,
            "consistency must be `local`, `read_barrier`, or `applied_index:N`"
        );
    }

    #[test]
    #[cfg(feature = "sql")]
    fn read_parsers_accept_only_canonical_read_barrier_consistency() {
        let consistency = "read_barrier";
        let read = parse_command([
            "read",
            "--url",
            "http://127.0.0.1:8080",
            "--token",
            "client-secret",
            "--key",
            "alpha",
            "--consistency",
            consistency,
        ])
        .unwrap();
        let Command::Read(read) = read else {
            panic!("expected read command");
        };
        assert_eq!(read.consistency, Some(ReadConsistency::ReadBarrier));

        let query = parse_command([
            "sql",
            "query",
            "--url",
            "http://127.0.0.1:8080",
            "--token",
            "client-secret",
            "--sql",
            "SELECT 1",
            "--consistency",
            consistency,
        ])
        .unwrap();
        let Command::SqlQuery(query) = query else {
            panic!("expected sql query command");
        };
        assert_eq!(query.consistency, Some(ReadConsistency::ReadBarrier));
        assert!(parse_read_consistency("barrier").is_err());
    }

    #[test]
    #[cfg(feature = "sql")]
    fn sql_read_parsers_accept_the_full_applied_index_domain_without_aliases() {
        let read = parse_command([
            "read",
            "--url",
            "http://127.0.0.1:8080",
            "--token",
            "client-secret",
            "--key",
            "alpha",
            "--consistency",
            "applied_index:0",
        ])
        .unwrap();
        let Command::Read(read) = read else {
            panic!("expected read command");
        };
        assert_eq!(read.consistency, Some(ReadConsistency::AppliedIndex(0)));

        let query = parse_command([
            "sql",
            "query",
            "--url",
            "http://127.0.0.1:8080",
            "--token",
            "client-secret",
            "--sql",
            "SELECT 1",
            "--consistency",
            "applied_index:18446744073709551615",
        ])
        .unwrap();
        let Command::SqlQuery(query) = query else {
            panic!("expected sql query command");
        };
        assert_eq!(
            query.consistency,
            Some(ReadConsistency::AppliedIndex(u64::MAX))
        );

        for invalid in [
            "applied_index:",
            "applied_index:18446744073709551616",
            "applied-index:1",
            "barrier",
        ] {
            assert!(
                parse_read_consistency(invalid).is_err(),
                "{invalid} must not be accepted"
            );
        }

        for command in ["rhiza read ", "rhiza sql query "] {
            let usage = USAGE
                .lines()
                .find(|line| line.trim_start().starts_with(command))
                .expect("SQL read command must be documented");
            assert!(usage.contains("local|read_barrier|applied_index:N"));
        }
    }

    #[test]
    #[cfg(feature = "sql")]
    fn sql_parsers_preserve_typed_parameters_and_query_controls() {
        let execute = parse_command([
            "sql",
            "execute",
            "--url",
            "http://127.0.0.1:8080",
            "--token",
            "client-secret",
            "--request-id",
            "sql-1",
            "--sql",
            "INSERT INTO users(id, name) VALUES (?1, ?2)",
            "--params-json",
            r#"[{"type":"integer","value":1},{"type":"text","value":"Ada"}]"#,
        ])
        .unwrap();
        let Command::SqlExecute(execute) = execute else {
            panic!("expected sql execute command");
        };
        assert_eq!(execute.request_id, "sql-1");
        assert_eq!(
            execute.statement.parameters,
            [SqlValue::Integer(1), SqlValue::Text("Ada".into())]
        );

        let query = parse_command([
            "sql",
            "query",
            "--url",
            "http://127.0.0.1:8080",
            "--token",
            "client-secret",
            "--sql",
            "SELECT name FROM users",
            "--consistency",
            "read_barrier",
            "--max-rows",
            "25",
        ])
        .unwrap();
        let Command::SqlQuery(query) = query else {
            panic!("expected sql query command");
        };
        assert_eq!(query.consistency, Some(ReadConsistency::ReadBarrier));
        assert_eq!(query.max_rows, Some(25));
    }

    #[cfg(feature = "graph")]
    #[test]
    fn graph_query_parser_preserves_full_cypher_typed_parameters_and_controls() {
        let command = parse_command([
            "graph",
            "query",
            "--url",
            "http://127.0.0.1:8080",
            "--token",
            "client-secret",
            "--cypher",
            "MATCH (u:User)-[:FOLLOWS]->(v) WHERE u.id = $id RETURN v.name ORDER BY v.name",
            "--params-json",
            r#"{"id":{"type":"u64","value":7},"tags":{"type":"list","value":[{"type":"string","value":"rust"}]}}"#,
            "--consistency",
            "applied_index:42",
            "--max-rows",
            "25",
        ])
        .unwrap();
        let Command::GraphQuery(args) = command else {
            panic!("expected graph query command");
        };

        assert!(args
            .statement
            .cypher
            .contains("MATCH (u:User)-[:FOLLOWS]->(v)"));
        assert_eq!(
            args.statement.parameters["id"],
            GraphQueryParameterDto::U64(7)
        );
        assert_eq!(args.consistency, Some(ReadConsistency::AppliedIndex(42)));
        assert_eq!(args.max_rows, Some(25));
    }

    #[cfg(feature = "kv")]
    #[test]
    fn kv_parsers_preserve_base64_and_validate_scan_shape() {
        let put = parse_command([
            "kv",
            "put",
            "--url",
            "http://127.0.0.1:8080",
            "--token",
            "client-secret",
            "--request-id",
            "put-1",
            "--key-base64",
            "/wA=",
            "--value-base64",
            "AAEC",
        ])
        .unwrap();
        let Command::KvPut(put) = put else {
            panic!("expected KV put command");
        };
        assert_eq!(put.request.key, "/wA=");
        assert_eq!(put.request.value, "AAEC");

        let scan = parse_command([
            "kv",
            "scan",
            "--url",
            "http://127.0.0.1:8080",
            "--token",
            "client-secret",
            "--prefix-base64",
            "/w==",
            "--cursor-base64",
            "/wA=",
            "--limit",
            "10",
            "--consistency",
            "read_barrier",
        ])
        .unwrap();
        let Command::KvScan(scan) = scan else {
            panic!("expected KV scan command");
        };
        assert_eq!(scan.request.prefix.as_deref(), Some("/w=="));
        assert_eq!(scan.request.cursor.as_deref(), Some("/wA="));
        assert_eq!(scan.request.limit, Some(10));

        assert!(parse_command([
            "kv",
            "scan",
            "--url",
            "http://127.0.0.1:8080",
            "--token",
            "client-secret",
            "--prefix-base64",
            "YQ==",
            "--start-base64",
            "YQ==",
        ])
        .is_err());
    }

    #[cfg(all(feature = "graph", feature = "kv"))]
    #[tokio::test]
    async fn graph_and_kv_query_clients_send_the_public_route_schemas_unchanged() {
        let captured = Arc::new(Mutex::new(Vec::<serde_json::Value>::new()));
        let graph_captured = Arc::clone(&captured);
        let kv_captured = Arc::clone(&captured);
        let app = Router::new()
            .route(
                GRAPH_QUERY_PATH,
                post(move |Json(request): Json<GraphQueryRequest>| {
                    let captured = Arc::clone(&graph_captured);
                    async move {
                        captured
                            .lock()
                            .unwrap()
                            .push(serde_json::to_value(request).unwrap());
                        Json(GraphQueryResponse {
                            columns: Vec::new(),
                            rows: Vec::new(),
                            applied_index: 7,
                            hash: LogHash::ZERO,
                        })
                    }
                }),
            )
            .route(
                KV_SCAN_PATH,
                post(move |Json(request): Json<KvScanRequest>| {
                    let captured = Arc::clone(&kv_captured);
                    async move {
                        captured
                            .lock()
                            .unwrap()
                            .push(serde_json::to_value(request).unwrap());
                        Json(KvScanResponse {
                            entries: Vec::new(),
                            next_cursor: None,
                            applied_index: 8,
                            hash: LogHash::ZERO,
                        })
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let graph = parse_graph_query(
            [
                "--url",
                &url,
                "--token",
                "client-secret",
                "--cypher",
                "MATCH (n) WHERE n.id = $id RETURN n",
                "--params-json",
                r#"{"id":{"type":"u64","value":7}}"#,
                "--consistency",
                "applied_index:7",
                "--max-rows",
                "20",
            ]
            .map(String::from),
            |_| None,
        )
        .unwrap();
        assert_eq!(request_graph_query(&graph).await.unwrap().applied_index, 7);

        let kv = parse_kv_scan(
            [
                "--url",
                &url,
                "--token",
                "client-secret",
                "--prefix-base64",
                "/w==",
                "--cursor-base64",
                "/wA=",
                "--limit",
                "10",
                "--consistency",
                "local",
            ]
            .map(String::from),
            |_| None,
        )
        .unwrap();
        assert_eq!(request_kv_scan(&kv).await.unwrap().applied_index, 8);

        let captured = captured.lock().unwrap();
        assert_eq!(
            captured[0]["statement"]["cypher"],
            "MATCH (n) WHERE n.id = $id RETURN n"
        );
        assert_eq!(captured[0]["statement"]["parameters"]["id"]["type"], "u64");
        assert_eq!(captured[0]["consistency"]["applied_index"], 7);
        assert_eq!(captured[1]["prefix"], "/w==");
        assert_eq!(captured[1]["cursor"], "/wA=");
        assert_eq!(captured[1]["limit"], 10);
        server.abort();
    }

    #[test]
    fn health_parser_selects_readiness_endpoint() {
        let command =
            parse_command(["health", "--url", "http://127.0.0.1:8080/", "--ready"]).unwrap();

        let Command::Health(args) = command else {
            panic!("expected health command");
        };
        assert_eq!(args.url, "http://127.0.0.1:8080/");
        assert!(args.ready);
    }

    #[test]
    fn status_parser_requires_url_and_selects_readiness_endpoint() {
        let error = parse_command(["status"])
            .err()
            .expect("status without a URL should fail");
        assert_eq!(error, "missing required argument: --url");

        let command = parse_command(["status", "--url", "http://127.0.0.1:8080/"]).unwrap();
        let Command::Status(args) = command else {
            panic!("expected status command");
        };
        assert_eq!(args.url, "http://127.0.0.1:8080/");
        assert!(args.ready);
    }

    #[tokio::test]
    async fn status_fails_when_readiness_endpoint_is_not_ready() {
        let app = Router::new().route(
            READYZ_PATH,
            get(|| async { StatusCode::SERVICE_UNAVAILABLE }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await });

        let exit_code = run(["status", "--url", &format!("http://{address}")]
            .map(String::from)
            .into_iter())
        .await;

        server.abort();
        assert_eq!(exit_code, 1);
    }

    #[tokio::test]
    async fn certified_tail_reports_obsolete_prestage_when_rebase_is_required() {
        let checkpoint = LogAnchor::new(41, LogHash::from_bytes([7; 32]));
        let body = CertifiedTailErrorResponse::RebaseRequired { checkpoint };
        let app = Router::new().route(
            CERTIFIED_TAIL_PATH,
            axum::routing::post(move || {
                let body = body.clone();
                async move { (StatusCode::CONFLICT, Json(body)) }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await });
        let source = HttpCertifiedTailSource {
            client: bounded_http_client(Duration::from_millis(20), Duration::from_millis(100))
                .unwrap(),
            peers: vec![
                PeerConfig::new("node-1", format!("http://{address}"), "peer-secret").unwrap(),
            ],
            tail_token: "tail-secret".into(),
            cluster_id: "cluster-a".into(),
            epoch: 1,
            config_id: 1,
            membership_digest: Membership::from_voters(
                ["node-1", "node-2", "node-3"].map(String::from),
            )
            .unwrap()
            .digest()
            .to_hex(),
            recovery_generation: 0,
        };
        let request = rhiza_node::CertifiedTailRequest {
            from: LogAnchor::new(0, LogHash::ZERO),
            max_entries: 8,
        };

        assert_eq!(
            source.fetch(&request).await.unwrap_err(),
            HaCertifiedTailError::RebaseRequired(checkpoint)
        );
        server.abort();
    }

    #[test]
    fn serve_config_uses_default_listeners_and_local_peer_token() {
        let (profile_name, profile, canonical_cluster_id) = compiled_profile_fixture();
        let values = HashMap::from([
            ("RHIZA_EXECUTION_PROFILE", profile_name),
            ("RHIZA_CLUSTER_ID", "cluster-a"),
            ("RHIZA_NODE_ID", "node-2"),
            ("RHIZA_DATA_DIR", "/tmp/node-2"),
            ("RHIZA_EPOCH", "1"),
            ("RHIZA_CONFIG_BUNDLE", base_bundle_json()),
            ("RHIZA_CLIENT_TOKEN", "client-secret"),
        ]);

        let config =
            ServeConfig::from_lookup(|name| values.get(name).map(ToString::to_string)).unwrap();

        assert_eq!(config.client_listen, "0.0.0.0:8080");
        assert_eq!(config.execution_profile, profile);
        assert_eq!(config.cluster_id, canonical_cluster_id);
        assert_eq!(config.recorder_listen, "0.0.0.0:8081");
        assert_eq!(config.local_peer_token().unwrap(), "peer-2-secret");
        assert_eq!(config.bundle.peers[0].base_url(), "http://node-1:8081");
        assert_eq!(config.bundle.peers[0].log_base_url(), "http://node-1:8080");
        assert_eq!(config.bundle.peers[1].log_base_url(), "http://node-2:8081");
        assert_eq!(config.recovery_generation, 1);
        assert!(config.remote.is_none());
        assert_eq!(config.recorder_transport, RecorderTransport::Http);
        assert!(config.recorder_tcp.is_none());
    }

    #[test]
    fn serve_derives_node_id_from_statefulset_hostname_when_unset() {
        let mut values = base_serve_env();
        values.remove("RHIZA_NODE_ID");
        values.insert("HOSTNAME", "rhiza-sql-c7-1");

        let config = parse_serve_env(&values).unwrap();

        assert_eq!(config.node_id, "node-2");
    }

    #[test]
    fn serve_prefers_explicit_node_id_over_hostname() {
        let mut values = base_serve_env();
        values.insert("HOSTNAME", "not-a-statefulset-pod");

        let config = parse_serve_env(&values).unwrap();

        assert_eq!(config.node_id, "node-2");
    }

    #[test]
    fn serve_rejects_missing_or_invalid_hostname_when_node_id_is_unset() {
        let mut values = base_serve_env();
        values.remove("RHIZA_NODE_ID");
        assert!(parse_serve_env(&values)
            .unwrap_err()
            .contains("HOSTNAME is required"));

        for hostname in ["rhiza", "rhiza-sql-c7-x", "rhiza-sql-c7-"] {
            values.insert("HOSTNAME", hostname);
            let error = parse_serve_env(&values).unwrap_err();
            assert!(error.contains("StatefulSet ordinal"), "{error}");
        }
    }

    #[test]
    fn serve_rejects_an_explicitly_empty_node_id() {
        let mut values = base_serve_env();
        values.insert("RHIZA_NODE_ID", "");
        values.insert("HOSTNAME", "rhiza-sql-c7-1");

        assert_eq!(
            parse_serve_env(&values).unwrap_err(),
            "RHIZA_NODE_ID must not be empty"
        );
    }

    #[test]
    fn tcp_postcard_transport_requires_listener_and_all_peer_addresses_without_tls() {
        let mut values = base_serve_env();
        values.insert("RHIZA_RECORDER_TRANSPORT", "tcp-postcard");
        let error = parse_serve_env(&values).unwrap_err();
        assert!(error.contains("RHIZA_RECORDER_TCP_LISTEN"), "{error}");
        assert!(!error.contains("peer-1-secret"), "{error}");

        values.insert("RHIZA_RECORDER_TCP_LISTEN", "0.0.0.0:8082");
        let error = parse_serve_env(&values).unwrap_err();
        assert!(error.contains("recorder_tcp_addr"), "{error}");

        let bundle = serde_json::json!({
            "config_id": 7,
            "members": [
                {"node_id":"node-1", "url":"http://node-1:8081", "log_url":"http://node-1:8080", "recorder_tcp_addr":"node-1:8082", "token":"peer-1-secret"},
                {"node_id":"node-2", "url":"http://node-2:8081", "recorder_tcp_addr":"node-2:8082", "token":"peer-2-secret"},
                {"node_id":"node-3", "url":"http://node-3:8081", "recorder_tcp_addr":"node-3:8082", "token":"peer-3-secret"}
            ]
        })
        .to_string();
        values.insert("RHIZA_CONFIG_BUNDLE", &bundle);
        let config = parse_serve_env(&values).unwrap();
        assert_eq!(config.recorder_transport, RecorderTransport::TcpPostcard);
        assert_eq!(config.recorder_tcp.as_ref().unwrap().listen, "0.0.0.0:8082");

        values.insert("RHIZA_RECORDER_TLS_CA_FILE", "/irrelevant/ca.pem");
        let error = parse_serve_env(&values).unwrap_err();
        assert!(error.contains("irrelevant"), "{error}");
        values.remove("RHIZA_RECORDER_TLS_CA_FILE");
    }

    #[cfg(not(feature = "recorder-postcard-rpc"))]
    #[test]
    fn postcard_rpc_transport_reports_when_candidate_feature_is_absent() {
        let mut values = base_serve_env();
        values.insert("RHIZA_RECORDER_TRANSPORT", "tcp-postcard-rpc");

        let error = parse_serve_env(&values).unwrap_err();

        assert!(error.contains("tcp-postcard-rpc"), "{error}");
        assert!(error.contains("recorder-postcard-rpc"), "{error}");
        assert!(error.contains("not compiled"), "{error}");
    }

    #[cfg(feature = "recorder-postcard-rpc")]
    #[test]
    fn postcard_rpc_transport_uses_tcp_listener_and_tls_configuration() {
        let bundle = serde_json::json!({
            "config_id": 7,
            "members": [
                {"node_id":"node-1", "url":"http://node-1:8081", "log_url":"http://node-1:8080", "recorder_tcp_addr":"node-1:8082", "recorder_tls_server_name":"node-1", "token":"peer-1-secret"},
                {"node_id":"node-2", "url":"http://node-2:8081", "recorder_tcp_addr":"node-2:8082", "recorder_tls_server_name":"node-2", "token":"peer-2-secret"},
                {"node_id":"node-3", "url":"http://node-3:8081", "recorder_tcp_addr":"node-3:8082", "recorder_tls_server_name":"node-3", "token":"peer-3-secret"}
            ]
        })
        .to_string();
        let mut values = base_serve_env();
        values.insert("RHIZA_CONFIG_BUNDLE", &bundle);
        values.insert("RHIZA_RECORDER_TRANSPORT", "tcp-postcard-rpc");
        values.insert("RHIZA_RECORDER_TLS", "on");
        values.insert("RHIZA_RECORDER_TCP_LISTEN", "0.0.0.0:8082");
        values.insert("RHIZA_RECORDER_TLS_CERT_FILE", "/missing/tls.crt");
        values.insert("RHIZA_RECORDER_TLS_KEY_FILE", "/missing/tls.key");
        values.insert("RHIZA_RECORDER_TLS_CA_FILE", "/missing/ca-bundle.pem");

        let config = parse_serve_env(&values).unwrap();

        assert_eq!(
            config.recorder_transport,
            RecorderTransport::TcpTlsPostcardRpc
        );
        assert_eq!(active_recorder_listen(&config).unwrap(), "0.0.0.0:8082");
        let tls = config.recorder_tcp.unwrap().tls.unwrap();
        assert_eq!(tls.certificate, Path::new("/missing/tls.crt"));
        assert_eq!(tls.private_key, Path::new("/missing/tls.key"));
        assert_eq!(tls.ca_bundle, Path::new("/missing/ca-bundle.pem"));
    }

    #[test]
    fn non_tls_transports_reject_bundle_tls_server_names() {
        let bundle = serde_json::json!({
            "config_id": 7,
            "members": [
                {"node_id":"node-1", "url":"http://node-1:8081", "recorder_tcp_addr":"node-1:8082", "recorder_tls_server_name":"node-1", "token":"peer-1-secret"},
                {"node_id":"node-2", "url":"http://node-2:8081", "recorder_tcp_addr":"node-2:8082", "recorder_tls_server_name":"node-2", "token":"peer-2-secret"},
                {"node_id":"node-3", "url":"http://node-3:8081", "recorder_tcp_addr":"node-3:8082", "recorder_tls_server_name":"node-3", "token":"peer-3-secret"}
            ]
        })
        .to_string();
        let mut values = base_serve_env();
        values.insert("RHIZA_CONFIG_BUNDLE", &bundle);

        let error = parse_serve_env(&values).unwrap_err();
        assert!(error.contains("recorder_tls_server_name"), "{error}");
        assert!(error.contains("irrelevant"), "{error}");

        values.insert("RHIZA_RECORDER_TRANSPORT", "tcp-postcard");
        values.insert("RHIZA_RECORDER_TCP_LISTEN", "0.0.0.0:8082");
        let error = parse_serve_env(&values).unwrap_err();
        assert!(error.contains("recorder_tls_server_name"), "{error}");
        assert!(error.contains("irrelevant"), "{error}");
    }

    #[test]
    fn tcp_postcard_tls_on_requires_all_tls_files_and_server_names() {
        let mut values = base_serve_env();
        values.insert("RHIZA_RECORDER_TRANSPORT", "tcp-postcard");
        values.insert("RHIZA_RECORDER_TLS", "on");
        let error = parse_serve_env(&values).unwrap_err();
        assert!(error.contains("RHIZA_RECORDER_TCP_LISTEN"), "{error}");

        values.insert("RHIZA_RECORDER_TCP_LISTEN", "0.0.0.0:8082");
        let error = parse_serve_env(&values).unwrap_err();
        assert!(error.contains("RHIZA_RECORDER_TLS_CERT_FILE"), "{error}");

        values.insert("RHIZA_RECORDER_TLS_CERT_FILE", "/missing/tls.crt");
        values.insert("RHIZA_RECORDER_TLS_KEY_FILE", "/missing/tls.key");
        values.insert("RHIZA_RECORDER_TLS_CA_FILE", "/missing/ca-bundle.pem");
        let error = parse_serve_env(&values).unwrap_err();
        assert!(error.contains("recorder_tcp_addr"), "{error}");

        let bundle = serde_json::json!({
            "config_id": 7,
            "members": [
                {"node_id":"node-1", "url":"http://node-1:8081", "recorder_tcp_addr":"node-1:8082", "recorder_tls_server_name":"node-1", "token":"peer-1-secret"},
                {"node_id":"node-2", "url":"http://node-2:8081", "recorder_tcp_addr":"node-2:8082", "recorder_tls_server_name":"node-2", "token":"peer-2-secret"},
                {"node_id":"node-3", "url":"http://node-3:8081", "recorder_tcp_addr":"node-3:8082", "recorder_tls_server_name":"node-3", "token":"peer-3-secret"}
            ]
        })
        .to_string();
        values.insert("RHIZA_CONFIG_BUNDLE", &bundle);
        let config = parse_serve_env(&values).unwrap();
        assert_eq!(config.recorder_transport, RecorderTransport::TcpTlsPostcard);
        let tls = config.recorder_tcp.unwrap().tls.unwrap();
        assert_eq!(tls.certificate, Path::new("/missing/tls.crt"));
        assert_eq!(tls.private_key, Path::new("/missing/tls.key"));
        assert_eq!(tls.ca_bundle, Path::new("/missing/ca-bundle.pem"));
    }

    #[test]
    fn recorder_tls_switch_rejects_invalid_or_conflicting_configuration() {
        let mut values = base_serve_env();
        values.insert("RHIZA_RECORDER_TLS", "sometimes");
        let error = parse_serve_env(&values).unwrap_err();
        assert!(
            error.contains("RHIZA_RECORDER_TLS must be on|off"),
            "{error}"
        );

        values.insert("RHIZA_RECORDER_TLS", "on");
        let error = parse_serve_env(&values).unwrap_err();
        assert!(
            error.contains("requires RHIZA_RECORDER_TRANSPORT=tcp-postcard"),
            "{error}"
        );

        values.insert("RHIZA_RECORDER_TRANSPORT", "tcp-tls-postcard");
        let error = parse_serve_env(&values).unwrap_err();
        assert!(
            error.contains("RHIZA_RECORDER_TRANSPORT must be http|tcp-postcard"),
            "{error}"
        );
    }

    #[test]
    fn configuration_bundle_accepts_tls_server_name_for_each_member() {
        let json = serde_json::json!({
            "config_id": 7,
            "members": [
                {"node_id":"node-1", "url":"http://node-1:8081", "recorder_tcp_addr":"node-1:8082", "recorder_tls_server_name":"node-1", "token":"t1"},
                {"node_id":"node-2", "url":"http://node-2:8081", "recorder_tcp_addr":"node-2:8082", "recorder_tls_server_name":"node-2", "token":"t2"},
                {"node_id":"node-3", "url":"http://node-3:8081", "recorder_tcp_addr":"node-3:8082", "recorder_tls_server_name":"node-3", "token":"t3"}
            ]
        });

        let bundle = parse_configuration_bundle(&json.to_string()).unwrap();
        assert_eq!(
            bundle.recorder_tcp_peers[1]
                .as_ref()
                .unwrap()
                .tls_server_name
                .as_deref(),
            Some("node-2")
        );
    }

    #[test]
    fn configuration_bundle_canonicalizes_three_through_seven_members() {
        for count in 3..=7 {
            let members = (1..=count)
                .rev()
                .map(|index| {
                    serde_json::json!({
                        "node_id": format!("node-{index}"),
                        "url": format!("http://node-{index}:8081"),
                        "token": format!("peer-{index}-secret"),
                    })
                })
                .collect::<Vec<_>>();
            let bundle = parse_configuration_bundle(
                &serde_json::json!({
                    "config_id": 7,
                    "members": members,
                })
                .to_string(),
            )
            .unwrap();

            assert_eq!(bundle.peers.len(), count);
            assert_eq!(bundle.membership.members()[0], "node-1");
            assert_eq!(
                bundle.membership.members()[count - 1],
                format!("node-{count}")
            );
            assert_eq!(bundle.configuration_state.config_id(), 7);
            assert_eq!(
                bundle.configuration_state.digest(),
                bundle.membership.digest()
            );
        }
    }

    #[test]
    fn configuration_bundle_rejects_invalid_counts_and_duplicate_members() {
        for json in [
            serde_json::json!({"config_id": 1, "members": []}),
            serde_json::json!({
                "config_id": 1,
                "members": [
                    {"node_id": "n1", "url": "http://n1", "token": "t1"},
                    {"node_id": "n2", "url": "http://n2", "token": "t2"},
                ],
            }),
            serde_json::json!({
                "config_id": 1,
                "members": [
                    {"node_id": "n1", "url": "http://n1", "token": "t1"},
                    {"node_id": "n1", "url": "http://n1-copy", "token": "t2"},
                    {"node_id": "n3", "url": "http://n3", "token": "t3"},
                ],
            }),
        ] {
            assert!(parse_configuration_bundle(&json.to_string()).is_err());
        }
    }

    #[test]
    fn configuration_bundle_rejects_obsolete_top_level_version_as_unknown_field() {
        let mut json: serde_json::Value = serde_json::from_str(base_bundle_json()).unwrap();
        json["version"] = serde_json::json!(1);

        let error = parse_configuration_bundle(&json.to_string()).unwrap_err();

        assert!(error.contains("unknown field `version`"), "{error}");
    }

    #[test]
    fn bundle_source_is_exclusive_and_never_exposes_tokens() {
        let token = "bundle-token-that-must-stay-private";
        let json = serde_json::json!({
            "config_id": 7,
            "members": [
                {"node_id": "n1", "url": "http://n1", "token": token},
                {"node_id": "n2", "url": "http://n2", "token": "t2"},
                {"node_id": "n3", "url": "http://n3", "token": "t3"},
            ],
        })
        .to_string();
        let values = HashMap::from([
            ("RHIZA_CONFIG_BUNDLE", json.as_str()),
            ("RHIZA_CONFIG_BUNDLE_FILE", "/tmp/config.json"),
        ]);
        let error = load_configuration_bundle(
            |name| values.get(name).map(ToString::to_string),
            |_| unreachable!("exclusive sources fail before file access"),
        )
        .unwrap_err();
        assert!(!error.contains(token));
        assert!(error.contains("mutually exclusive"));

        let parsed = parse_configuration_bundle(&json).unwrap();
        assert!(!format!("{parsed:?}").contains(token));
    }

    #[test]
    fn configuration_bundle_loads_from_file() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("configuration.json");
        std::fs::write(
            &path,
            serde_json::json!({
                "config_id": 9,
                "members": [
                    {"node_id": "n3", "url": "http://n3", "token": "t3"},
                    {"node_id": "n1", "url": "http://n1", "token": "t1"},
                    {"node_id": "n2", "url": "http://n2", "token": "t2"},
                ],
            })
            .to_string(),
        )
        .unwrap();
        let path = path.to_string_lossy().into_owned();

        let bundle = load_configuration_bundle(
            |name| (name == "RHIZA_CONFIG_BUNDLE_FILE").then(|| path.clone()),
            |path| std::fs::read_to_string(path),
        )
        .unwrap();

        assert_eq!(bundle.config_id, 9);
        assert_eq!(bundle.membership.members(), ["n1", "n2", "n3"]);
    }

    #[test]
    fn serve_rejects_a_local_node_outside_the_bundle() {
        let mut values = base_serve_env();
        values.insert("RHIZA_NODE_ID", "node-9");

        assert!(parse_serve_env(&values)
            .unwrap_err()
            .contains("local node_id"));
    }

    #[test]
    fn checkpoint_identity_uses_bundle_config_id() {
        let json = serde_json::json!({
            "config_id": 11,
            "members": [
                {"node_id": "n1", "url": "http://n1", "token": "t1"},
                {"node_id": "n2", "url": "http://n2", "token": "t2"},
                {"node_id": "n3", "url": "http://n3", "token": "t3"},
            ],
        })
        .to_string();

        let config_id =
            configuration_id(&mut |name| (name == "RHIZA_CONFIG_BUNDLE").then(|| json.clone()))
                .unwrap();

        assert_eq!(config_id, 11);
    }

    #[test]
    fn checkpoint_identity_uses_the_required_profile_and_canonical_cluster_id() {
        let (profile_name, _, canonical_cluster_id) = compiled_profile_fixture();
        let values = HashMap::from([
            ("RHIZA_EXECUTION_PROFILE", profile_name),
            ("RHIZA_CLUSTER_ID", "cluster-a"),
            ("RHIZA_EPOCH", "2"),
            ("RHIZA_CONFIG_BUNDLE", checkpoint_bundle_json()),
            ("RHIZA_RECOVERY_GENERATION", "3"),
            ("RHIZA_OBJECT_STORE", "s3"),
            ("RHIZA_S3_BUCKET", "checkpoints"),
        ]);

        let config =
            CheckpointCommandConfig::from_lookup(|name| values.get(name).map(ToString::to_string))
                .unwrap();

        assert_eq!(config.identity().cluster_id(), canonical_cluster_id);

        let error = match CheckpointCommandConfig::from_lookup(|name| {
            (name != "RHIZA_EXECUTION_PROFILE")
                .then(|| values.get(name))
                .flatten()
                .map(ToString::to_string)
        }) {
            Err(error) => error,
            Ok(_) => panic!("missing execution profile must be rejected"),
        };
        assert_eq!(error, "RHIZA_EXECUTION_PROFILE is required");
    }

    #[test]
    fn gc_apply_requires_an_exact_hash_and_explicit_confirmation() {
        assert!(parse_gc_apply_flags(["--plan-hash", "abc"].map(String::from)).is_err());
        assert!(parse_gc_apply_flags(["--confirm"].map(String::from)).is_err());
        assert!(parse_gc_apply_flags(
            ["--plan-hash", &"a".repeat(64), "--confirm"].map(String::from)
        )
        .is_ok());
        assert!(parse_gc_apply_flags(
            ["--plan-hash", &"z".repeat(64), "--confirm"].map(String::from)
        )
        .is_err());
    }

    #[test]
    fn membership_install_requires_predecessor_transition_material() {
        let json = serde_json::json!({
            "config_id": 2,
            "members": [
                {"node_id": "n1", "url": "http://n1", "token": "t1"},
                {"node_id": "n2", "url": "http://n2", "token": "t2"},
                {"node_id": "n4", "url": "http://n4", "token": "t4"},
            ],
        });
        let bundle = parse_configuration_bundle(&json.to_string()).unwrap();
        assert!(bundle.predecessor.is_none());
        assert!(bundle.require_predecessor().is_err());
    }

    fn exact_predecessor_bundle() -> serde_json::Value {
        predecessor_bundle_bound_to(["node-1", "node-2", "node-3"])
    }

    fn predecessor_bundle_bound_to<const N: usize>(
        successor_members: [&str; N],
    ) -> serde_json::Value {
        let predecessor = Membership::new(["node-1", "node-2", "node-3"]).unwrap();
        let command = ConfigChange::bound_stop(
            "rhiza-vind",
            3,
            predecessor.digest(),
            4,
            successor_members.into_iter().map(String::from).collect(),
        )
        .unwrap()
        .to_stored_command();
        let entry = LogEntry {
            cluster_id: "rhiza-vind".into(),
            epoch: 1,
            config_id: 3,
            index: 10,
            entry_type: command.entry_type,
            payload: command.payload.clone(),
            prev_hash: LogHash::ZERO,
            hash: LogEntry::calculate_hash(
                "rhiza-vind",
                10,
                1,
                3,
                command.entry_type,
                LogHash::ZERO,
                &command.payload,
            ),
        };
        let proposal = rhiza_quepaxa::Proposal::new(
            rhiza_quepaxa::ProposalPriority::MAX,
            "n1",
            1,
            AcceptedValue::from_command("rhiza-vind", 10, 1, 3, LogHash::ZERO, &command),
        );
        let proof = DecisionProof::Phase2 {
            cluster_id: "rhiza-vind".into(),
            slot: 10,
            epoch: 1,
            config_id: 3,
            config_digest: predecessor.digest(),
            step: 6,
            proposal: proposal.clone(),
            summaries: ["node-1", "node-2"]
                .into_iter()
                .map(|id| rhiza_quepaxa::RecorderSummary {
                    recorder_id: id.into(),
                    slot: 10,
                    step: 6,
                    first_current: None,
                    aggregate_prior: Some(proposal.clone()),
                })
                .collect(),
        };
        serde_json::json!({
            "config_id": 4,
            "members": [
                {"node_id": "node-1", "url": "http://rhiza-sql-c4-0.rhiza-sql-c4:8081", "log_url": "http://rhiza-sql-c4-0.rhiza-sql-c4:8080", "token": "peer-1-secret"},
                {"node_id": "node-2", "url": "http://rhiza-sql-c4-1.rhiza-sql-c4:8081", "log_url": "http://rhiza-sql-c4-1.rhiza-sql-c4:8080", "token": "peer-2-secret"},
                {"node_id": "node-3", "url": "http://rhiza-sql-c4-2.rhiza-sql-c4:8081", "log_url": "http://rhiza-sql-c4-2.rhiza-sql-c4:8080", "token": "peer-3-secret"},
            ],
            "predecessor": {
                "members": ["node-1", "node-2", "node-3"],
                "stop_entry": entry,
                "stop_proof": proof,
            },
        })
    }

    #[test]
    fn validate_config_bundle_uses_the_production_parser() {
        let json = exact_predecessor_bundle().to_string();
        let values = HashMap::from([("RHIZA_CONFIG_BUNDLE", json.as_str())]);

        let command = parse_validate_config_bundle(
            |name| values.get(name).map(ToString::to_string),
            |_| unreachable!("inline bundle must not read a file"),
        )
        .unwrap();

        assert!(matches!(command, Command::ValidateConfigBundle(Some(4))));
        assert!(matches!(
            parse_command(["validate-config-bundle", "--stdin"]).unwrap(),
            Command::ValidateConfigBundle(None)
        ));
    }

    #[test]
    fn configuration_bundle_accepts_exact_predecessor_stop_material() {
        let json = exact_predecessor_bundle();

        let bundle = parse_configuration_bundle(&json.to_string()).unwrap();

        assert_eq!(bundle.config_id, 4);
        assert_eq!(bundle.configuration_state.config_id(), 3);
        assert_eq!(
            bundle.configuration_state.digest(),
            Membership::new(["node-1", "node-2", "node-3"])
                .unwrap()
                .digest()
        );
        assert_eq!(bundle.configuration_state.stop().unwrap().index(), 10);
        assert!(bundle.require_predecessor().is_ok());
    }

    #[test]
    fn configuration_bundle_rejects_obsolete_predecessor_version_as_unknown_field() {
        let mut json = exact_predecessor_bundle();
        json["predecessor"]["version"] = serde_json::json!(2);

        let error = parse_configuration_bundle(&json.to_string()).unwrap_err();

        assert!(error.contains("unknown field `version`"), "{error}");
    }

    #[test]
    fn predecessor_fixture_is_generated_by_and_accepted_by_production_types() {
        let fixture = include_str!("../../../scripts/test-fixtures/config-4-predecessor.json");

        assert_eq!(
            serde_json::from_str::<serde_json::Value>(fixture).unwrap(),
            exact_predecessor_bundle()
        );
        assert!(parse_configuration_bundle(fixture).is_ok());
        let wrong_successor =
            include_str!("../../../scripts/test-fixtures/config-4-wrong-successor.json");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(wrong_successor).unwrap(),
            predecessor_bundle_bound_to(["node-1", "node-2", "node-4"])
        );
        assert!(parse_configuration_bundle(wrong_successor).is_err());
    }

    #[test]
    fn configuration_bundle_rejects_semantically_invalid_predecessor_material() {
        let valid = exact_predecessor_bundle();
        let mut mutations = Vec::new();

        let mut digest = valid.clone();
        digest["predecessor"]["stop_proof"]["Phase2"]["config_digest"][0] = serde_json::json!(1);
        mutations.push(digest);

        let mut entry_hash = valid.clone();
        entry_hash["predecessor"]["stop_entry"]["hash"][0] = serde_json::json!(1);
        mutations.push(entry_hash);

        let mut command_binding = valid.clone();
        command_binding["predecessor"]["stop_proof"]["Phase2"]["proposal"]["value"]
            ["command_hash"][0] = serde_json::json!(1);
        mutations.push(command_binding);

        mutations.push(predecessor_bundle_bound_to(["node-1", "node-2", "node-4"]));

        let mut phase2_maximum = valid;
        let low_priority = serde_json::json!([
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 1
        ]);
        let high_priority = serde_json::json!([
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 2
        ]);
        phase2_maximum["predecessor"]["stop_proof"]["Phase2"]["proposal"]["priority"] =
            low_priority.clone();
        for summary in phase2_maximum["predecessor"]["stop_proof"]["Phase2"]["summaries"]
            .as_array_mut()
            .unwrap()
        {
            summary["aggregate_prior"]["priority"] = low_priority.clone();
        }
        phase2_maximum["predecessor"]["stop_proof"]["Phase2"]["summaries"][0]["aggregate_prior"]
            ["priority"] = high_priority;
        mutations.push(phase2_maximum);

        for mutation in mutations {
            assert!(parse_configuration_bundle(&mutation.to_string()).is_err());
        }
    }

    fn base_serve_env() -> HashMap<&'static str, &'static str> {
        let (profile_name, _, _) = compiled_profile_fixture();
        HashMap::from([
            ("RHIZA_EXECUTION_PROFILE", profile_name),
            ("RHIZA_CLUSTER_ID", "cluster-a"),
            ("RHIZA_NODE_ID", "node-2"),
            ("RHIZA_DATA_DIR", "/tmp/node-2"),
            ("RHIZA_EPOCH", "1"),
            ("RHIZA_CONFIG_BUNDLE", base_bundle_json()),
            ("RHIZA_CLIENT_TOKEN", "client-secret"),
        ])
    }

    fn compiled_profile_fixture() -> (&'static str, ExecutionProfile, &'static str) {
        [
            ("sql", ExecutionProfile::Sqlite, "rhiza:sql:cluster-a"),
            ("graph", ExecutionProfile::Graph, "rhiza:graph:cluster-a"),
            ("kv", ExecutionProfile::Kv, "rhiza:kv:cluster-a"),
        ]
        .into_iter()
        .find(|(_, profile, _)| execution_profile_compiled(*profile))
        .expect("test builds enable at least one execution profile")
    }

    fn base_bundle_json() -> &'static str {
        r#"{"config_id":7,"members":[{"node_id":"node-1","url":"http://node-1:8081","log_url":"http://node-1:8080","token":"peer-1-secret"},{"node_id":"node-2","url":"http://node-2:8081","token":"peer-2-secret"},{"node_id":"node-3","url":"http://node-3:8081","token":"peer-3-secret"}]}"#
    }

    fn checkpoint_bundle_json() -> &'static str {
        r#"{"config_id":11,"members":[{"node_id":"n1","url":"http://n1","token":"t1"},{"node_id":"n2","url":"http://n2","token":"t2"},{"node_id":"n3","url":"http://n3","token":"t3"}]}"#
    }

    fn parse_serve_env(values: &HashMap<&str, &str>) -> Result<ServeConfig, String> {
        ServeConfig::from_lookup(|name| values.get(name).map(ToString::to_string))
    }

    #[test]
    fn execution_profile_is_required_and_canonicalizes_the_cluster_id() {
        let mut values = base_serve_env();
        values.remove("RHIZA_EXECUTION_PROFILE");
        assert_eq!(
            parse_serve_env(&values).unwrap_err(),
            "RHIZA_EXECUTION_PROFILE is required"
        );

        for (value, profile, expected_cluster_id) in [
            ("sql", ExecutionProfile::Sqlite, "rhiza:sql:cluster-a"),
            ("graph", ExecutionProfile::Graph, "rhiza:graph:cluster-a"),
            ("kv", ExecutionProfile::Kv, "rhiza:kv:cluster-a"),
        ] {
            values.insert("RHIZA_EXECUTION_PROFILE", value);
            if execution_profile_compiled(profile) {
                let config = parse_serve_env(&values).unwrap();
                assert_eq!(config.execution_profile, profile);
                assert_eq!(config.cluster_id, expected_cluster_id);
                assert_eq!(config.node_config().unwrap().execution_profile(), profile);
            } else {
                assert_eq!(
                    parse_serve_env(&values).unwrap_err(),
                    format!("RHIZA_EXECUTION_PROFILE={value} is not compiled into this binary")
                );
            }
        }

        values.insert("RHIZA_EXECUTION_PROFILE", "sqlite");
        assert_eq!(
            parse_serve_env(&values).unwrap_err(),
            "RHIZA_EXECUTION_PROFILE must be sql|graph|kv"
        );

        let (profile_name, profile, canonical_cluster_id) = compiled_profile_fixture();
        values.insert("RHIZA_EXECUTION_PROFILE", profile_name);
        values.insert("RHIZA_CLUSTER_ID", canonical_cluster_id);
        let canonical = parse_serve_env(&values).unwrap();
        assert_eq!(canonical.logical_cluster_id, "cluster-a");
        assert_eq!(canonical.cluster_id, canonical_cluster_id);
        assert_eq!(
            canonical.node_config().unwrap().execution_profile(),
            profile
        );
        let foreign_cluster_id = match profile {
            ExecutionProfile::Sqlite => "rhiza:graph:cluster-a",
            ExecutionProfile::Graph => "rhiza:sql:cluster-a",
            ExecutionProfile::Kv => "rhiza:sql:cluster-a",
        };
        values.insert("RHIZA_CLUSTER_ID", foreign_cluster_id);
        assert!(parse_serve_env(&values)
            .unwrap_err()
            .contains(&format!("not {profile_name}")));
    }

    #[test]
    fn serve_admin_and_tail_tokens_are_optional_distinct_and_redacted() {
        let values = base_serve_env();
        assert!(parse_serve_env(&values).unwrap().admin_token.is_none());

        let mut values = base_serve_env();
        values.insert("RHIZA_ADMIN_TOKEN", "admin-secret");
        let config = parse_serve_env(&values).unwrap();
        assert_eq!(config.admin_token.as_deref(), Some("admin-secret"));
        assert!(!format!("{config:?}").contains("admin-secret"));

        values.insert("RHIZA_ADMIN_TOKEN", "");
        assert!(parse_serve_env(&values).is_err());
        values.insert("RHIZA_ADMIN_TOKEN", "client-secret");
        assert!(parse_serve_env(&values).unwrap_err().contains("distinct"));
        values.insert("RHIZA_ADMIN_TOKEN", "peer-2-secret");
        assert!(parse_serve_env(&values).unwrap_err().contains("distinct"));
        for invalid in [" admin ", "admin secret", "admin\tsecret", "café"] {
            values.insert("RHIZA_ADMIN_TOKEN", invalid);
            assert!(parse_serve_env(&values).is_err(), "accepted {invalid:?}");
        }

        let mut values = base_serve_env();
        values.insert("RHIZA_ADMIN_TOKEN", "admin-secret");
        values.insert("RHIZA_TAIL_TOKEN", "tail-secret");
        let config = parse_serve_env(&values).unwrap();
        assert_eq!(config.tail_token.as_deref(), Some("tail-secret"));
        assert!(!format!("{config:?}").contains("tail-secret"));
        for collision in ["client-secret", "admin-secret", "peer-2-secret"] {
            values.insert("RHIZA_TAIL_TOKEN", collision);
            assert!(parse_serve_env(&values).unwrap_err().contains("distinct"));
        }
    }

    #[test]
    #[cfg(feature = "sql")]
    fn client_and_admin_commands_reject_unsafe_auth_and_non_origin_admin_urls() {
        for invalid in [" secret ", "a b", "a\tb", "café"] {
            assert!(parse_write_with_lookup(
                [
                    "--url",
                    "http://127.0.0.1:8080",
                    "--token",
                    invalid,
                    "--request-id",
                    "r1",
                    "--key",
                    "k",
                    "--value",
                    "v",
                ]
                .map(String::from),
                |_| None,
            )
            .is_err());
            assert!(parse_admin_command_config(
                [
                    "--admin-url",
                    "http://127.0.0.1:8080",
                    "--admin-token",
                    invalid,
                ]
                .map(String::from)
                .into_iter(),
                false,
                false,
                |_| None,
            )
            .is_err());
        }

        for invalid in [
            "http://user@127.0.0.1:8080",
            "http://user:password@127.0.0.1:8080",
            "http://127.0.0.1:8080/prefix",
            "http://127.0.0.1:8080?query=1",
            "http://127.0.0.1:8080#fragment",
        ] {
            assert!(
                parse_admin_command_config(
                    ["--admin-url", invalid, "--admin-token", "admin-secret"]
                        .map(String::from)
                        .into_iter(),
                    false,
                    false,
                    |_| None,
                )
                .is_err(),
                "accepted {invalid:?}"
            );
        }
    }

    #[test]
    fn membership_commands_default_to_live_admin_and_require_stable_operation_ids() {
        let admin_only = HashMap::from([
            ("RHIZA_ADMIN_URL", "http://127.0.0.1:8080"),
            ("RHIZA_ADMIN_TOKEN", "admin-secret"),
        ]);
        let status = parse_membership_command_with_lookup(
            ["status"].map(String::from).into_iter(),
            |name| admin_only.get(name).map(ToString::to_string),
        )
        .unwrap();
        let Command::MembershipStatus(status) = status else {
            panic!("expected membership status");
        };
        assert!(matches!(status.target, AdminTarget::Live(_)));
        assert!(status.serve.is_none());

        let mut live = base_serve_env();
        live.extend([
            ("RHIZA_ADMIN_URL", "http://127.0.0.1:8080"),
            ("RHIZA_ADMIN_TOKEN", "admin-secret"),
        ]);
        let missing_id =
            parse_membership_command_with_lookup(["stop"].map(String::from).into_iter(), |name| {
                live.get(name).map(ToString::to_string)
            })
            .err()
            .expect("missing operation id must fail");
        assert!(missing_id.contains("operation id"));
        live.insert("RHIZA_ADMIN_OPERATION_ID", "stop-001");
        let stop =
            parse_membership_command_with_lookup(["stop"].map(String::from).into_iter(), |name| {
                live.get(name).map(ToString::to_string)
            })
            .unwrap();
        let Command::MembershipStop(stop) = stop else {
            panic!("expected membership stop");
        };
        assert!(matches!(stop.target, AdminTarget::Live(_)));
        assert_eq!(stop.operation_id.as_deref(), Some("stop-001"));
        assert!(stop.serve.is_some());
    }

    #[test]
    fn live_admin_target_fence_requires_exact_profile_identity_and_configuration() {
        let serve = parse_serve_env(&base_serve_env()).unwrap();
        let state = serve.bundle.configuration_state.clone();
        let mut status = AdminStatusResponse {
            cluster_id: serve.cluster_id.clone(),
            execution_profile: serve.execution_profile,
            epoch: serve.epoch,
            node: NodeStatus {
                ready: true,
                configuration_status: RuntimeConfigurationStatus::Active,
                configuration_state: state,
                stop_anchor: None,
                active_config_id: serve.bundle.config_id,
                active_membership_digest: serve.bundle.membership.digest(),
            },
            members: serve.bundle.membership.members().to_vec(),
            recovery_generation: serve.recovery_generation,
            qlog_root: LogAnchor::new(0, LogHash::ZERO),
            checkpoint_root: None,
            stopped_transition: None,
        };
        assert!(validate_live_admin_target(&status, &serve, &[serve.bundle.config_id]).is_ok());

        status.execution_profile = match serve.execution_profile {
            ExecutionProfile::Sqlite => ExecutionProfile::Graph,
            ExecutionProfile::Graph => ExecutionProfile::Kv,
            ExecutionProfile::Kv => ExecutionProfile::Sqlite,
        };
        assert_eq!(
            validate_live_admin_target(&status, &serve, &[serve.bundle.config_id]).unwrap_err(),
            "admin target fence mismatch for execution_profile; refusing mutating request"
        );
        status.execution_profile = serve.execution_profile;
        status.node.active_config_id += 1;
        assert!(
            validate_live_admin_target(&status, &serve, &[serve.bundle.config_id])
                .unwrap_err()
                .contains("config_id")
        );
    }

    #[test]
    fn offline_membership_requires_explicit_flag_and_local_serve_config() {
        let values = base_serve_env();
        let command = parse_membership_command_with_lookup(
            ["status", "--offline"].map(String::from).into_iter(),
            |name| values.get(name).map(ToString::to_string),
        )
        .unwrap();
        let Command::MembershipStatus(config) = command else {
            panic!("expected membership status");
        };
        assert!(matches!(config.target, AdminTarget::Offline));
        assert!(config.serve.is_some());
    }

    #[test]
    fn remote_serve_parses_all_provider_modes_without_exposing_secrets() {
        let mut s3 = base_serve_env();
        s3.extend([
            ("RHIZA_OBJECT_STORE", "s3"),
            ("RHIZA_S3_ENDPOINT", "https://s3.example.test"),
            ("RHIZA_S3_BUCKET", "checkpoints"),
            ("RHIZA_S3_ACCESS_KEY", "s3-access-secret"),
            ("RHIZA_S3_SECRET_KEY", "s3-key-secret"),
            ("RHIZA_DURABILITY_MODE", "sync"),
            ("RHIZA_RECOVERY_GENERATION", "4"),
            ("RHIZA_STARTUP_MODE", "bootstrap"),
        ]);
        let config = parse_serve_env(&s3).unwrap();
        assert_eq!(config.recovery_generation, 4);
        assert_eq!(config.node_config().unwrap().recovery_generation(), 4);
        let debug = format!("{config:?}");
        assert!(!debug.contains("s3-access-secret"));
        assert!(!debug.contains("s3-key-secret"));
        assert!(debug.contains("[redacted]"));
        assert!(matches!(
            config.remote.as_ref().unwrap().object_store,
            ObjStoreConfig::S3 { .. }
        ));
        s3.insert("RHIZA_CHECKPOINT_LEASE_MS", "5000");
        assert_eq!(
            parse_serve_env(&s3)
                .unwrap()
                .remote
                .unwrap()
                .lease_duration_ms,
            5_000
        );

        let mut gcs = base_serve_env();
        gcs.extend([
            ("RHIZA_OBJECT_STORE", "gcs"),
            ("RHIZA_GCS_BUCKET", "checkpoints"),
            ("RHIZA_GCS_SERVICE_ACCOUNT_PATH", "/var/run/gcs.json"),
            ("RHIZA_DURABILITY_MODE", "bounded"),
            ("RHIZA_DURABILITY_MAX_LAG", "250ms"),
            ("RHIZA_RECOVERY_GENERATION", "2"),
            ("RHIZA_STARTUP_MODE", "rejoin"),
        ]);
        assert!(matches!(
            parse_serve_env(&gcs).unwrap().remote.unwrap().object_store,
            ObjStoreConfig::Gcs { .. }
        ));

        gcs.insert("RHIZA_CHECKPOINT_LEASE_MS", "59999");
        assert_eq!(
            parse_serve_env(&gcs).unwrap_err(),
            "RHIZA_CHECKPOINT_LEASE_MS must be at least 60000 for gcs"
        );
        gcs.insert("RHIZA_CHECKPOINT_LEASE_MS", "60000");
        assert_eq!(
            parse_serve_env(&gcs)
                .unwrap()
                .remote
                .unwrap()
                .lease_duration_ms,
            60_000
        );

        let mut azure = base_serve_env();
        azure.extend([
            ("RHIZA_OBJECT_STORE", "azure"),
            ("RHIZA_AZURE_ACCOUNT", "account"),
            ("RHIZA_AZURE_CONTAINER", "checkpoints"),
            ("RHIZA_AZURE_ACCESS_KEY", "azure-key-secret"),
            ("RHIZA_DURABILITY_MODE", "periodic"),
            ("RHIZA_DURABILITY_INTERVAL", "2m"),
            ("RHIZA_RECOVERY_GENERATION", "9"),
            ("RHIZA_STARTUP_MODE", "disaster"),
        ]);
        assert!(matches!(
            parse_serve_env(&azure)
                .unwrap()
                .remote
                .unwrap()
                .object_store,
            ObjStoreConfig::AzureBlob { .. }
        ));

        for secret in ["s3-access-secret", "s3-key-secret", "azure-key-secret"] {
            assert!(!parse_serve_env(&HashMap::from([
                ("RHIZA_CLUSTER_ID", "cluster-a"),
                ("RHIZA_OBJECT_STORE", "invalid"),
            ]))
            .unwrap_err()
            .contains(secret));
        }
    }

    #[test]
    fn duration_parser_accepts_only_strict_positive_supported_units() {
        assert_eq!(
            parse_positive_duration("1ms").unwrap(),
            std::time::Duration::from_millis(1)
        );
        assert_eq!(
            parse_positive_duration("2s").unwrap(),
            std::time::Duration::from_secs(2)
        );
        assert_eq!(
            parse_positive_duration("3m").unwrap(),
            std::time::Duration::from_secs(180)
        );
        assert_eq!(
            parse_positive_duration("4h").unwrap(),
            std::time::Duration::from_secs(14_400)
        );
        for invalid in ["", "0ms", "1", "1.5s", " 1s", "1d", "-1s", "1 s"] {
            assert!(
                parse_positive_duration(invalid).is_err(),
                "accepted {invalid:?}"
            );
        }
    }

    #[test]
    fn object_store_errors_redact_provider_credentials() {
        let config = ObjStoreConfig::S3 {
            endpoint: Some("https://s3.example.test".into()),
            bucket: "checkpoints".into(),
            access_key: Some("access-secret".into()),
            secret_key: Some("private-secret".into()),
            region: "us-east-1".into(),
            allow_http: false,
        };
        let message = redact_object_store_error(
            &config,
            "failed with access-secret and private-secret".into(),
        );
        assert_eq!(message, "failed with [redacted] and [redacted]");
    }

    #[test]
    fn s3_configuration_supports_aws_discovery_and_rejects_partial_credentials() {
        let base = HashMap::from([
            ("RHIZA_S3_BUCKET", "checkpoints"),
            ("RHIZA_S3_REGION", "ap-northeast-2"),
        ]);
        let discovered = parse_object_store_with_lookup("s3", false, &mut |name| {
            base.get(name).map(ToString::to_string)
        })
        .unwrap();
        assert!(matches!(
            discovered,
            ObjStoreConfig::S3 {
                endpoint: None,
                access_key: None,
                secret_key: None,
                ..
            }
        ));

        let full = HashMap::from([
            ("RHIZA_S3_ENDPOINT", "http://rustfs:9000"),
            ("RHIZA_S3_BUCKET", "checkpoints"),
            ("RHIZA_S3_ACCESS_KEY", "access"),
            ("RHIZA_S3_SECRET_KEY", "secret"),
        ]);
        assert!(matches!(
            parse_object_store_with_lookup("s3", false, &mut |name| {
                full.get(name).map(ToString::to_string)
            })
            .unwrap(),
            ObjStoreConfig::S3 {
                endpoint: Some(_),
                access_key: Some(_),
                secret_key: Some(_),
                ..
            }
        ));

        for values in [
            HashMap::from([
                ("RHIZA_S3_BUCKET", "checkpoints"),
                ("RHIZA_S3_ACCESS_KEY", "access"),
            ]),
            HashMap::from([
                ("RHIZA_S3_BUCKET", "checkpoints"),
                ("RHIZA_S3_SECRET_KEY", "secret"),
            ]),
            HashMap::from([
                ("RHIZA_S3_BUCKET", "checkpoints"),
                ("RHIZA_S3_ACCESS_KEY", ""),
                ("RHIZA_S3_SECRET_KEY", "secret"),
            ]),
        ] {
            assert!(parse_object_store_with_lookup("s3", false, &mut |name| {
                values.get(name).map(ToString::to_string)
            })
            .is_err());
        }
    }

    #[test]
    fn remote_serve_rejects_missing_or_irrelevant_checkpoint_settings() {
        let mut values = base_serve_env();
        values.extend([
            ("RHIZA_OBJECT_STORE", "s3"),
            ("RHIZA_S3_ENDPOINT", "https://s3.example.test"),
            ("RHIZA_S3_BUCKET", "checkpoints"),
            ("RHIZA_S3_ACCESS_KEY", "access"),
            ("RHIZA_S3_SECRET_KEY", "secret"),
            ("RHIZA_RECOVERY_GENERATION", "2"),
            ("RHIZA_STARTUP_MODE", "bootstrap"),
        ]);
        assert!(parse_serve_env(&values)
            .unwrap_err()
            .contains("RHIZA_DURABILITY_MODE"));

        values.insert("RHIZA_DURABILITY_MODE", "sync");
        values.insert("RHIZA_DURABILITY_MAX_LAG", "1s");
        assert!(parse_serve_env(&values).unwrap_err().contains("irrelevant"));

        values.insert("RHIZA_DURABILITY_MODE", "bounded");
        values.remove("RHIZA_DURABILITY_MAX_LAG");
        assert!(parse_serve_env(&values)
            .unwrap_err()
            .contains("RHIZA_DURABILITY_MAX_LAG"));

        values.insert("RHIZA_DURABILITY_MAX_LAG", "1s");
        values.insert("RHIZA_DURABILITY_INTERVAL", "1m");
        assert!(parse_serve_env(&values).unwrap_err().contains("irrelevant"));

        values.insert("RHIZA_DURABILITY_MODE", "periodic");
        values.remove("RHIZA_DURABILITY_MAX_LAG");
        values.remove("RHIZA_DURABILITY_INTERVAL");
        assert!(parse_serve_env(&values)
            .unwrap_err()
            .contains("RHIZA_DURABILITY_INTERVAL"));
    }

    #[test]
    fn remote_serve_requires_generation_and_startup_mode_and_rejects_local_store() {
        let mut values = base_serve_env();
        values.extend([
            ("RHIZA_OBJECT_STORE", "gcs"),
            ("RHIZA_GCS_BUCKET", "checkpoints"),
            ("RHIZA_DURABILITY_MODE", "sync"),
        ]);
        assert!(parse_serve_env(&values)
            .unwrap_err()
            .contains("RHIZA_RECOVERY_GENERATION"));
        values.insert("RHIZA_RECOVERY_GENERATION", "2");
        assert!(parse_serve_env(&values)
            .unwrap_err()
            .contains("RHIZA_STARTUP_MODE"));
        values.insert("RHIZA_STARTUP_MODE", "resume");
        assert!(parse_serve_env(&values)
            .unwrap_err()
            .contains("bootstrap|rejoin|disaster"));
        values.insert("RHIZA_OBJECT_STORE", "local:/tmp/checkpoints");
        values.insert("RHIZA_STARTUP_MODE", "rejoin");
        assert!(parse_serve_env(&values)
            .unwrap_err()
            .contains("only supported by e2e"));
    }

    #[test]
    fn roll_parser_accepts_flags_or_env_and_requires_consecutive_generations() {
        let (profile_name, _, _) = compiled_profile_fixture();
        let mut values = HashMap::from([
            ("RHIZA_EXECUTION_PROFILE", profile_name),
            ("RHIZA_CLUSTER_ID", "cluster-a"),
            ("RHIZA_EPOCH", "1"),
            (
                "RHIZA_CONFIG_BUNDLE",
                r#"{"config_id":1,"members":[{"node_id":"n1","url":"http://n1","token":"t1"},{"node_id":"n2","url":"http://n2","token":"t2"},{"node_id":"n3","url":"http://n3","token":"t3"}]}"#,
            ),
            ("RHIZA_OBJECT_STORE", "gcs"),
            ("RHIZA_GCS_BUCKET", "checkpoints"),
            ("RHIZA_FROM_GENERATION", "4"),
            ("RHIZA_TO_GENERATION", "5"),
        ]);
        let parsed = parse_roll_checkpoint_with_lookup(Vec::new(), |name| {
            values.get(name).map(ToString::to_string)
        })
        .unwrap();
        assert_eq!(parsed.from_generation, 4);
        assert_eq!(parsed.to_generation, 5);

        values.insert("RHIZA_TO_GENERATION", "6");
        assert!(parse_roll_checkpoint_with_lookup(Vec::new(), |name| {
            values.get(name).map(ToString::to_string)
        })
        .err()
        .expect("non-consecutive generations must fail")
        .contains("source generation + 1"));

        let parsed = parse_roll_checkpoint_with_lookup(
            ["--from-generation", "7", "--to-generation", "8"].map(String::from),
            |name| values.get(name).map(ToString::to_string),
        )
        .unwrap();
        assert_eq!((parsed.from_generation, parsed.to_generation), (7, 8));
    }

    fn local_checkpoint(root: &std::path::Path, generation: u64) -> ObjectArchiveStore {
        let store = ObjStore::new(ObjStoreConfig::Local {
            root: root.to_path_buf(),
        })
        .unwrap();
        ObjectArchiveStore::new_checkpoint_for_single_process(
            store,
            CheckpointIdentity::new(
                "rhiza:sql:cluster-a",
                1,
                1,
                LogHash::digest(&[b"cli-test-config"]),
                generation,
            ),
        )
    }

    #[cfg(feature = "sql")]
    fn entries(end: u64) -> Vec<LogEntry> {
        let root = tempfile::tempdir().unwrap();
        let db = SqliteStateMachine::open(
            root.path().join("state.sqlite"),
            "rhiza:sql:cluster-a",
            "node-1",
            1,
            1,
        )
        .unwrap();
        let mut previous = LogHash::ZERO;
        (1..=end)
            .map(|index| {
                let command = SqlCommand {
                    request_id: format!("checkpoint-entry-{index}"),
                    statements: vec![
                        SqlStatement {
                            sql: "CREATE TABLE IF NOT EXISTS checkpoint_fixture (id INTEGER PRIMARY KEY, value TEXT NOT NULL)".into(),
                            parameters: vec![],
                        },
                        SqlStatement {
                            sql: "INSERT INTO checkpoint_fixture(id, value) VALUES (?1, ?2)"
                                .into(),
                            parameters: vec![
                                SqlValue::Integer(index as i64),
                                SqlValue::Text(format!("entry-{index}")),
                            ],
                        },
                    ],
                };
                let request = encode_sql_command(&command).unwrap();
                let preparation = db
                    .prepare_sql_batch_effect(
                        &[SqlBatchMember {
                            command: &command,
                            request_payload: &request,
                        }],
                        index - 1,
                        previous,
                    )
                    .unwrap();
                preparation
                    .results
                    .into_iter()
                    .next()
                    .expect("one-member checkpoint fixture batch returns one result")
                    .unwrap();
                let payload = preparation
                    .effect
                    .expect("successful checkpoint fixture batch produces one QWAL v3 effect");
                let hash = LogEntry::calculate_hash(
                    "rhiza:sql:cluster-a",
                    index,
                    1,
                    1,
                    EntryType::Command,
                    previous,
                    &payload,
                );
                let entry = LogEntry {
                    cluster_id: "rhiza:sql:cluster-a".into(),
                    epoch: 1,
                    config_id: 1,
                    index,
                    entry_type: EntryType::Command,
                    payload,
                    prev_hash: previous,
                    hash,
                };
                db.apply_entry(&entry).unwrap();
                previous = hash;
                entry
            })
            .collect()
    }

    #[cfg(not(feature = "sql"))]
    fn entries(end: u64) -> Vec<LogEntry> {
        let mut previous = LogHash::ZERO;
        (1..=end)
            .map(|index| {
                let hash = LogEntry::calculate_hash(
                    "rhiza:sql:cluster-a",
                    index,
                    1,
                    1,
                    EntryType::Noop,
                    previous,
                    &[],
                );
                let entry = LogEntry {
                    cluster_id: "rhiza:sql:cluster-a".into(),
                    epoch: 1,
                    config_id: 1,
                    index,
                    entry_type: EntryType::Noop,
                    payload: Vec::new(),
                    prev_hash: previous,
                    hash,
                };
                previous = hash;
                entry
            })
            .collect()
    }

    #[test]
    #[cfg(feature = "sql")]
    fn checkpoint_fixture_entries_use_one_receipt_qwal_v3_effects_and_reject_v2() {
        let entries = entries(3);

        for (offset, entry) in entries.iter().enumerate() {
            let effect = rhiza_sql::decode_qwal_v3(&entry.payload).unwrap();
            assert_eq!(effect.base_index, offset as u64);
            assert_eq!(effect.base_hash, entry.prev_hash);
            assert_eq!(effect.receipts.len(), 1);
            assert_eq!(
                effect.receipts[0].request_id,
                format!("checkpoint-entry-{}", offset + 1)
            );

            let mut v2 = b"QWAL\0\x03".to_vec();
            v2.extend_from_slice(&entry.payload[rhiza_sql::QWAL_V3_MAGIC.len()..]);
            assert!(rhiza_sql::decode_qwal_v3(&v2).is_err());
        }
    }

    #[test]
    fn usage_describes_live_admin_as_the_default_and_offline_as_explicit() {
        assert!(USAGE.contains("live admin API by default"));
        assert!(USAGE.contains("pass --offline only as an explicit local fallback"));
    }

    #[tokio::test]
    async fn init_checkpoint_is_idempotent_only_for_an_empty_matching_identity() {
        let root = tempfile::tempdir().unwrap();
        let archive = local_checkpoint(root.path(), 1);
        assert_eq!(
            initialize_empty_checkpoint(&archive).await.unwrap().index(),
            0
        );
        assert_eq!(
            initialize_empty_checkpoint(&archive).await.unwrap().index(),
            0
        );
        archive.publish_committed(&entries(1)).await.unwrap();
        assert!(initialize_empty_checkpoint(&archive)
            .await
            .unwrap_err()
            .contains("nonempty"));
    }

    #[tokio::test]
    async fn roll_checkpoint_resumes_a_prefix_and_retries_a_complete_target() {
        let root = tempfile::tempdir().unwrap();
        let source = local_checkpoint(root.path(), 1);
        source.initialize_checkpoint().await.unwrap();
        source.publish_committed(&entries(3)).await.unwrap();
        let target = local_checkpoint(root.path(), 2);
        target.initialize_checkpoint().await.unwrap();
        target.publish_committed(&entries(1)).await.unwrap();

        let (old_tip, new_tip) = roll_checkpoint(&source, &target).await.unwrap();
        assert_eq!(old_tip.index(), 3);
        assert_eq!(new_tip, old_tip);
        assert_eq!(
            target
                .load_checkpoint_restore()
                .await
                .unwrap()
                .unwrap()
                .restored()
                .suffix(),
            entries(3)
        );
        let retry = roll_checkpoint(&source, &target).await.unwrap();
        assert_eq!(retry, (old_tip, new_tip));
        assert_eq!(
            target
                .load_checkpoint_restore()
                .await
                .unwrap()
                .unwrap()
                .restored()
                .suffix(),
            entries(3)
        );
    }

    #[tokio::test]
    async fn roll_checkpoint_rejects_a_divergent_target() {
        let root = tempfile::tempdir().unwrap();
        let source = local_checkpoint(root.path(), 1);
        source.initialize_checkpoint().await.unwrap();
        source.publish_committed(&entries(3)).await.unwrap();
        let target = local_checkpoint(root.path(), 2);
        target.initialize_checkpoint().await.unwrap();
        let mut divergent = entries(1);
        divergent[0].entry_type = EntryType::Command;
        divergent[0].payload = b"divergent-entry".to_vec();
        divergent[0].hash = LogEntry::calculate_hash(
            "rhiza:sql:cluster-a",
            1,
            1,
            1,
            EntryType::Command,
            LogHash::ZERO,
            &divergent[0].payload,
        );
        target.publish_committed(&divergent).await.unwrap();

        assert!(roll_checkpoint(&source, &target)
            .await
            .unwrap_err()
            .contains("publication conflicts at index 1"));
    }

    #[cfg(feature = "sql")]
    #[tokio::test]
    async fn roll_checkpoint_preserves_compacted_v2_snapshot_and_suffix() {
        let root = tempfile::tempdir().unwrap();
        let source = local_checkpoint(root.path(), 1);
        let committed = entries(4);
        source.publish_committed(&committed[..2]).await.unwrap();
        source.publish_committed(&committed[2..]).await.unwrap();
        let bytes = b"cli-roll-snapshot";
        source
            .publish_checkpoint_snapshot(
                RecoveryAnchor::new(
                    "rhiza:sql:cluster-a",
                    1,
                    ConfigurationState::active(
                        1,
                        source.checkpoint_identity().unwrap().config_digest(),
                    ),
                    1,
                    LogAnchor::new(2, committed[1].hash),
                    SnapshotIdentity::new(
                        "cli-roll-snapshot",
                        LogHash::digest(&[bytes]),
                        bytes.len() as u64,
                        sql_executor_fingerprint().unwrap(),
                    ),
                ),
                bytes,
            )
            .await
            .unwrap();
        let target = local_checkpoint(root.path(), 2);

        let (source_tip, target_tip) = roll_checkpoint(&source, &target).await.unwrap();

        assert_eq!(source_tip, target_tip);
        let restored = target.load_checkpoint_restore().await.unwrap().unwrap();
        assert_eq!(restored.restored().snapshot().unwrap().bytes(), bytes);
        assert_eq!(
            restored
                .restored()
                .snapshot()
                .unwrap()
                .anchor()
                .recovery_generation(),
            2
        );
        assert_eq!(restored.restored().suffix(), &committed[2..]);
    }

    fn unused_local_address() -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().to_string()
    }
    #[test]
    fn build_consensus_rejects_a_mismatched_direct_recorder_identity() {
        let root = tempfile::tempdir().unwrap();
        let mut config = parse_serve_env(&base_serve_env()).unwrap();
        config.data_dir = root.path().join("node-2");
        let recorder = RecorderFileStore::new_with_membership(
            root.path().join("wrong-recorder"),
            "node-9",
            config.cluster_id.clone(),
            config.epoch,
            config.bundle.config_id,
            config.bundle.membership.clone(),
        )
        .unwrap();

        let error = build_consensus(&config, Some(&recorder)).unwrap_err();

        assert!(error.contains("local recorder identity"));
        assert!(error.contains("node-2"));
        assert!(error.contains("node-9"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn build_consensus_combines_direct_self_with_one_remote_recorder() {
        let root = tempfile::tempdir().unwrap();
        let self_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let self_address = self_listener.local_addr().unwrap();
        let remote_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let remote_address = remote_listener.local_addr().unwrap();
        let unavailable_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let unavailable_address = unavailable_listener.local_addr().unwrap();
        let peers = vec![
            PeerConfig::new("node-1", format!("http://{self_address}"), "peer-1-secret").unwrap(),
            PeerConfig::new(
                "node-2",
                format!("http://{remote_address}"),
                "peer-2-secret",
            )
            .unwrap(),
            PeerConfig::new(
                "node-3",
                format!("http://{unavailable_address}"),
                "peer-3-secret",
            )
            .unwrap(),
        ];
        let membership =
            Membership::from_voters(peers.iter().map(|peer| peer.node_id().to_string())).unwrap();
        let cluster_id = "rhiza:sql:direct-recorder-cluster";
        let local_recorder = RecorderFileStore::new_with_membership(
            root.path().join("local-recorder"),
            "node-1",
            cluster_id,
            1,
            1,
            membership.clone(),
        )
        .unwrap();
        let remote_recorder = RecorderFileStore::new_with_membership(
            root.path().join("remote-recorder"),
            "node-2",
            cluster_id,
            1,
            1,
            membership.clone(),
        )
        .unwrap();
        let remote_app = recorder_router_for_generation(remote_recorder, peers.clone(), 1);
        let unavailable_app = Router::new().fallback(|| async { StatusCode::SERVICE_UNAVAILABLE });
        let self_app = unavailable_app.clone();
        let self_server = tokio::spawn(async move {
            axum::serve(self_listener, self_app).await.unwrap();
        });
        let remote_server = tokio::spawn(async move {
            axum::serve(remote_listener, remote_app).await.unwrap();
        });
        let unavailable_server = tokio::spawn(async move {
            axum::serve(unavailable_listener, unavailable_app)
                .await
                .unwrap();
        });
        let (_, execution_profile, _) = compiled_profile_fixture();
        let config = ServeConfig {
            execution_profile,
            logical_cluster_id: "direct-recorder-cluster".into(),
            cluster_id: cluster_id.into(),
            node_id: "node-1".into(),
            data_dir: root.path().join("node-1"),
            epoch: 1,
            bundle: ConfigurationBundle {
                config_id: 1,
                peers,
                recorder_tcp_peers: vec![None, None, None],
                configuration_state: ConfigurationState::active(1, membership.digest()),
                membership,
                predecessor: None,
            },
            client_token: "client-secret".into(),
            admin_token: None,
            tail_token: None,
            client_listen: unused_local_address(),
            recorder_listen: unused_local_address(),
            recorder_transport: RecorderTransport::Http,
            recorder_tcp: None,
            recovery_generation: 1,
            remote: None,
        };
        let consensus = build_consensus(&config, Some(&local_recorder)).unwrap();

        let entry = tokio::task::spawn_blocking(move || {
            let entry = consensus.propose_at(
                rhiza_quepaxa::RecorderRpcContext::default_timeout(),
                1,
                LogHash::ZERO,
                rhiza_core::Command::new(
                    rhiza_core::CommandKind::Deterministic,
                    b"direct-self".to_vec(),
                ),
            );
            assert!(consensus.finish_pending_rpcs(Duration::from_secs(2)));
            entry
        })
        .await
        .unwrap();

        match entry {
            Ok(entry) => {
                assert_eq!(entry.payload, b"direct-self");
                assert!(local_recorder.inspect_record_summary(1).unwrap().is_some());
            }
            Err(rhiza_quepaxa::Error::UnknownOutcome) => {
                assert!(local_recorder.inspect_record_summary(1).unwrap().is_some());
            }
            Err(error) => panic!("direct recorder proposal failed: {error}"),
        }
        self_server.abort();
        remote_server.abort();
        unavailable_server.abort();
    }

    #[cfg(feature = "sql")]
    async fn wait_for_tcp(address: &str) {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if tokio::net::TcpStream::connect(address).await.is_ok() {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for {address}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }

    #[cfg(feature = "sql")]
    async fn wait_until_ready(address: &str) {
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        let args = HealthArgs {
            url: format!("http://{address}"),
            ready: true,
        };
        loop {
            if request_health(&args).await.is_ok() {
                return;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for readiness at {address}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[cfg(feature = "sql")]
    async fn staggered_http_cluster_reaches_readiness_after_all_recorders_start() {
        staggered_cluster_start(RecorderTransport::Http).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[cfg(feature = "sql")]
    async fn staggered_tcp_postcard_cluster_commits_after_all_recorders_start() {
        staggered_cluster_start(RecorderTransport::TcpPostcard).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[cfg(all(feature = "sql", feature = "recorder-postcard-rpc"))]
    async fn staggered_postcard_rpc_cluster_commits_after_all_recorders_start() {
        staggered_cluster_start(RecorderTransport::TcpPostcardRpc).await;
    }

    #[cfg(feature = "sql")]
    async fn staggered_cluster_start(recorder_transport: RecorderTransport) {
        let _cluster_test_guard = sequential_cluster_test_lock().lock().await;
        let temp = tempfile::tempdir().unwrap();
        let recorder_addresses = [
            unused_local_address(),
            unused_local_address(),
            unused_local_address(),
        ];
        let client_addresses = [
            unused_local_address(),
            unused_local_address(),
            unused_local_address(),
        ];
        let tcp_addresses = [
            unused_local_address(),
            unused_local_address(),
            unused_local_address(),
        ];
        let peers = [
            PeerConfig::new(
                "node-1",
                format!("http://{}", recorder_addresses[0]),
                "peer-1-secret",
            )
            .unwrap(),
            PeerConfig::new(
                "node-2",
                format!("http://{}", recorder_addresses[1]),
                "peer-2-secret",
            )
            .unwrap(),
            PeerConfig::new(
                "node-3",
                format!("http://{}", recorder_addresses[2]),
                "peer-3-secret",
            )
            .unwrap(),
        ];
        let configs: [ServeConfig; 3] = std::array::from_fn(|index| ServeConfig {
            execution_profile: ExecutionProfile::Sqlite,
            logical_cluster_id: "staggered-cluster".into(),
            cluster_id: "rhiza:sql:staggered-cluster".into(),
            node_id: format!("node-{}", index + 1),
            data_dir: temp.path().join(format!("node-{}", index + 1)),
            epoch: 1,
            bundle: ConfigurationBundle {
                config_id: 1,
                configuration_state: ConfigurationState::active(1, {
                    Membership::from_voters(peers.iter().map(|peer| peer.node_id().to_string()))
                        .unwrap()
                        .digest()
                }),
                membership: Membership::from_voters(
                    peers.iter().map(|peer| peer.node_id().to_string()),
                )
                .unwrap(),
                peers: peers.to_vec(),
                recorder_tcp_peers: tcp_addresses
                    .iter()
                    .map(|address| {
                        Some(RecorderTcpPeer {
                            address: address.clone(),
                            tls_server_name: None,
                        })
                    })
                    .collect(),
                predecessor: None,
            },
            client_token: "client-secret".into(),
            admin_token: None,
            tail_token: None,
            client_listen: client_addresses[index].clone(),
            recorder_listen: recorder_addresses[index].clone(),
            recorder_transport,
            recorder_tcp: recorder_transport.is_tcp().then(|| RecorderTcpConfig {
                listen: tcp_addresses[index].clone(),
                tls: None,
            }),
            recovery_generation: 1,
            remote: None,
        });

        let (first_shutdown, first_wait) = tokio::sync::oneshot::channel();
        let first = tokio::spawn(serve_until(configs[0].clone(), async move {
            let _ = first_wait.await;
        }));
        let first_recorder_address = match recorder_transport {
            RecorderTransport::Http => &recorder_addresses[0],
            RecorderTransport::TcpPostcard | RecorderTransport::TcpTlsPostcard => &tcp_addresses[0],
            #[cfg(feature = "recorder-postcard-rpc")]
            RecorderTransport::TcpPostcardRpc | RecorderTransport::TcpTlsPostcardRpc => {
                &tcp_addresses[0]
            }
        };
        wait_for_tcp(first_recorder_address).await;
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        assert!(!first.is_finished());
        if recorder_transport.is_tcp() {
            assert!(tokio::net::TcpStream::connect(&recorder_addresses[0])
                .await
                .is_err());
        }
        assert!(tokio::net::TcpStream::connect(&client_addresses[0])
            .await
            .is_err());

        let (second_shutdown, second_wait) = tokio::sync::oneshot::channel();
        let second = tokio::spawn(serve_until(configs[1].clone(), async move {
            let _ = second_wait.await;
        }));

        let (third_shutdown, third_wait) = tokio::sync::oneshot::channel();
        let third = tokio::spawn(serve_until(configs[2].clone(), async move {
            let _ = third_wait.await;
        }));
        for index in 0..recorder_addresses.len() {
            let recorder_address = match recorder_transport {
                RecorderTransport::Http => &recorder_addresses[index],
                RecorderTransport::TcpPostcard | RecorderTransport::TcpTlsPostcard => {
                    &tcp_addresses[index]
                }
                #[cfg(feature = "recorder-postcard-rpc")]
                RecorderTransport::TcpPostcardRpc | RecorderTransport::TcpTlsPostcardRpc => {
                    &tcp_addresses[index]
                }
            };
            wait_for_tcp(recorder_address).await;
        }
        for address in &client_addresses[..2] {
            wait_until_ready(address).await;
        }
        wait_until_ready(&client_addresses[2]).await;
        let write = WriteArgs {
            urls: client_addresses
                .iter()
                .map(|address| format!("http://{address}"))
                .collect(),
            token: "client-secret".into(),
            request_id: format!("integration-{recorder_transport:?}"),
            key: "transport".into(),
            value: "committed".into(),
        };
        let write_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let committed = loop {
            match request_write(&write).await {
                Ok(committed) => break committed,
                Err(error)
                    if (error.contains("code=write_outcome_unknown")
                        || error.contains("code=ambiguous_mutation"))
                        && tokio::time::Instant::now() < write_deadline =>
                {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                Err(error) => panic!("same-request write retry did not converge: {error}"),
            }
        };
        assert!(committed.applied_index > 0);

        let _ = first_shutdown.send(());
        let _ = second_shutdown.send(());
        let _ = third_shutdown.send(());
        let joined = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(first, second, third)
        })
        .await
        .expect("servers must stop within the graceful shutdown bound");
        assert!(joined.0.unwrap().is_ok());
        assert!(joined.1.unwrap().is_ok());
        assert!(joined.2.unwrap().is_ok());
        for address in &recorder_addresses {
            assert!(tokio::net::TcpStream::connect(address).await.is_err());
        }
        for address in &tcp_addresses {
            assert!(tokio::net::TcpStream::connect(address).await.is_err());
        }
    }

    #[cfg(feature = "sql")]
    fn sequential_cluster_test_lock() -> &'static tokio::sync::Mutex<()> {
        static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
    }

    #[tokio::test]
    async fn admin_transport_errors_do_not_expose_url_credentials() {
        let address = unused_local_address();
        let username = "url-user-that-must-stay-private";
        let password = "url-password-that-must-stay-private";
        let config = AdminClientConfig {
            url: format!("http://{username}:{password}@{address}"),
            token: "admin-secret".into(),
        };
        let client =
            bounded_http_client(Duration::from_millis(20), Duration::from_millis(40)).unwrap();

        let error = admin_get_with_client::<serde_json::Value>(&config, ADMIN_STATUS_PATH, &client)
            .await
            .unwrap_err();

        assert!(!error.contains(username));
        assert!(!error.contains(password));
        assert!(!error.contains(&address));
    }

    #[tokio::test]
    async fn health_request_respects_the_configured_deadline() {
        let app = Router::new().route(
            LIVEZ_PATH,
            get(|| async {
                tokio::time::sleep(Duration::from_secs(1)).await;
                StatusCode::OK
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await });
        let client =
            bounded_http_client(Duration::from_millis(20), Duration::from_millis(40)).unwrap();
        let started = tokio::time::Instant::now();

        let error = request_health_with_client(
            &HealthArgs {
                url: format!("http://{address}"),
                ready: false,
            },
            &client,
        )
        .await
        .unwrap_err();

        server.abort();
        assert!(started.elapsed() < Duration::from_millis(500));
        assert!(error.starts_with("request failed:"));
    }

    #[tokio::test]
    async fn admin_request_respects_the_configured_deadline() {
        let app = Router::new().route(
            ADMIN_STATUS_PATH,
            get(|| async {
                tokio::time::sleep(Duration::from_secs(1)).await;
                Json(serde_json::json!({}))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await });
        let client =
            bounded_http_client(Duration::from_millis(20), Duration::from_millis(40)).unwrap();
        let started = tokio::time::Instant::now();

        let error = admin_get_with_client::<serde_json::Value>(
            &AdminClientConfig {
                url: format!("http://{address}"),
                token: "admin-secret".into(),
            },
            ADMIN_STATUS_PATH,
            &client,
        )
        .await
        .unwrap_err();

        server.abort();
        assert!(started.elapsed() < Duration::from_millis(500));
        assert!(error.starts_with("admin request failed:"));
    }
}
