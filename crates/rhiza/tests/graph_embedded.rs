use std::collections::BTreeMap;

use rhizadb::{
    EmbeddedConfig, ExecutionProfile, GraphCommandResultV1, GraphCommandV1, GraphParameterValue,
    GraphResultValue, GraphValueV1, ReadConsistency, Rhiza,
};

#[tokio::test(flavor = "multi_thread")]
async fn graph_profile_executes_semantic_writes_and_queries_in_process() {
    let root = tempfile::tempdir().unwrap();
    let config =
        EmbeddedConfig::local_file_backed("cluster-a", root.path(), ExecutionProfile::Graph)
            .unwrap();
    let rhiza = Rhiza::open(config).await.unwrap();
    let handle = rhiza.handle();

    let write = handle
        .mutate_graph(
            GraphCommandV1::put_document(
                "graph-put",
                "document-1",
                GraphValueV1::String("Ada".into()),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        write.result(),
        &GraphCommandResultV1::PutDocument { created: true }
    );

    let result = handle
        .query_graph(
            "MATCH (d:RhizaDocument) WHERE d.id = $id RETURN d.string_value AS value",
            BTreeMap::from([(
                "id".into(),
                GraphParameterValue::String("document-1".into()),
            )]),
            ReadConsistency::Local,
            10,
        )
        .await
        .unwrap();
    assert_eq!(result.rows, [vec![GraphResultValue::String("Ada".into())]]);

    rhiza.shutdown().await.unwrap();
}
