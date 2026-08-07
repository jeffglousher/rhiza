use rhizadb::ExecutionProfile;

#[test]
fn sql_profile_is_always_available() {
    assert_eq!(ExecutionProfile::Sqlite.as_str(), "sql");
}

#[cfg(feature = "graph")]
#[test]
fn graph_feature_exports_the_graph_surface() {
    let _ = std::mem::size_of::<rhizadb::GraphCommandV1>();
    assert_eq!(ExecutionProfile::Graph.as_str(), "graph");
}

#[cfg(feature = "kv")]
#[test]
fn kv_feature_exports_the_kv_surface() {
    let _ = std::mem::size_of::<rhizadb::KvCommandV1>();
    assert_eq!(ExecutionProfile::Kv.as_str(), "kv");
}
