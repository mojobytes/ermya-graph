mod common;

use ermya_graph::{Properties, Property};
use ermya_graph_protocol::PackStreamValue;
use ermya_graph_protocol::bolt_frame::{BoltChunkedReader, BoltChunkedWriter};
use ermya_graph_protocol::bolt_message::{BoltRequest, BoltResponse};
use ermya_graph_server::auth::NoAuthProvider;
use std::sync::Arc;
use tokio::io::{DuplexStream, ReadHalf, WriteHalf};

struct Client {
    writer: BoltChunkedWriter<WriteHalf<DuplexStream>>,
    reader: BoltChunkedReader<ReadHalf<DuplexStream>>,
}

impl Client {
    async fn send(&mut self, request: BoltRequest) -> BoltResponse {
        common::bolt_send(&mut self.writer, &request).await;
        common::bolt_recv(&mut self.reader).await
    }

    async fn run(&mut self, query: &str) {
        let response = self
            .send(BoltRequest::Run {
                query: query.into(),
                params: vec![],
                extra: vec![(
                    "db".into(),
                    PackStreamValue::String(common::DEFAULT_TEST_DB.into()),
                )],
            })
            .await;
        assert!(
            matches!(response, BoltResponse::Success { .. }),
            "{response:?}"
        );
        let mut response = self.send(BoltRequest::Pull { extra: vec![] }).await;
        while matches!(response, BoltResponse::Record { .. }) {
            response = common::bolt_recv(&mut self.reader).await;
        }
        assert!(
            matches!(response, BoltResponse::Success { .. }),
            "{response:?}"
        );
    }
}

fn property(value: &str) -> Properties {
    let mut p = Properties::new();
    p.insert("key".into(), Property::String(value.into()));
    p
}

async fn rejected_commit(unique: bool, check_code: bool) {
    let (registry, _dir) = common::single_db_registry().await;
    let db = registry
        .acquire(common::DEFAULT_TEST_DB, "test")
        .await
        .unwrap();
    let graph = db.graph();
    let id = {
        let mut g = graph.write().unwrap();
        g.schema_catalog_mut().add_unique_constraint("Item", "key");
        g.add_node("Version", property("original")).unwrap()
    };
    let (writer, reader, _shutdown) =
        common::spawn_single_db_handler(Arc::new(NoAuthProvider), registry).await;
    let mut client = Client { writer, reader };
    assert!(matches!(
        client.send(BoltRequest::Hello { extra: vec![] }).await,
        BoltResponse::Success { .. }
    ));
    client.run("RETURN 1 AS selected").await;
    assert!(matches!(
        client.send(BoltRequest::Begin { extra: vec![] }).await,
        BoltResponse::Success { .. }
    ));
    client
        .run(if unique {
            "CREATE (n:Item {key:'same'})"
        } else {
            "MATCH (n:Version) SET n.key = 'loser'"
        })
        .await;
    client.run("CREATE (n:LoserOnly)").await;
    {
        let mut g = graph.write().unwrap();
        if unique {
            g.add_node("Item", property("same")).unwrap();
        } else {
            let mut node = g.node(id).unwrap();
            *node.properties_mut() = property("winner");
            g.update_node(id, &node).unwrap();
        }
    }
    let response = client.send(BoltRequest::Commit).await;
    let BoltResponse::Failure { metadata } = response else {
        panic!("expected failure: {response:?}")
    };
    if check_code {
        let expected = if unique {
            "Neo.ClientError.Schema.ConstraintValidationFailed"
        } else {
            "Neo.TransientError.Transaction.Outdated"
        };
        assert!(
            metadata.contains(&("code".into(), PackStreamValue::String(expected.into()))),
            "{metadata:?}"
        );
        assert!(!format!("{metadata:?}").contains("__TG_"));
    }
    assert!(matches!(
        client.send(BoltRequest::Reset).await,
        BoltResponse::Success { .. }
    ));
    {
        let mut g = graph.write().unwrap();
        g.vacuum_once().unwrap();
        assert_eq!(
            g.pending_version_chains(),
            0,
            "failed transaction leaked deltas or held the vacuum watermark"
        );
        assert!(g.nodes_by_label("LoserOnly").is_empty());
    }
    assert!(matches!(
        client.send(BoltRequest::Begin { extra: vec![] }).await,
        BoltResponse::Success { .. }
    ));
    client.run("CREATE (n:AfterFailure)").await;
    assert!(matches!(
        client.send(BoltRequest::Commit).await,
        BoltResponse::Success { .. }
    ));
    assert_eq!(
        graph.read().unwrap().nodes_by_label("AfterFailure").len(),
        1
    );
}

#[tokio::test]
async fn unique_commit_has_schema_code() {
    rejected_commit(true, true).await;
}

#[tokio::test]
async fn conflict_commit_has_transient_code() {
    rejected_commit(false, true).await;
}

#[tokio::test]
async fn rejected_commit_reset_releases_engine_transaction() {
    rejected_commit(true, false).await;
}

#[tokio::test]
async fn disconnect_releases_open_transaction() {
    let (registry, _dir) = common::single_db_registry().await;
    let graph = registry
        .acquire(common::DEFAULT_TEST_DB, "test")
        .await
        .unwrap()
        .graph();
    let (writer, reader, _shutdown) =
        common::spawn_single_db_handler(Arc::new(NoAuthProvider), registry).await;
    let mut client = Client { writer, reader };
    client.send(BoltRequest::Hello { extra: vec![] }).await;
    client.run("RETURN 1 AS selected").await;
    client.send(BoltRequest::Begin { extra: vec![] }).await;
    client.run("CREATE (n:Abandoned)").await;
    drop(client);
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            if graph.read().unwrap().pending_version_chains() == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("disconnect must roll back pending deltas");
    assert!(graph.read().unwrap().nodes_by_label("Abandoned").is_empty());
}
