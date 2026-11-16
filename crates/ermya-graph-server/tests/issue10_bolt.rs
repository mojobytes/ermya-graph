mod common;

use ermya_graph_protocol::{
    PackStreamValue,
    bolt_message::{BoltRequest, BoltResponse},
};
use ermya_graph_server::auth::NoAuthProvider;
use std::sync::Arc;

#[tokio::test]
async fn begin_selects_database_without_prior_run_and_supports_commit_rollback_reset() {
    for explicit in [true, false] {
        let (registry, _dir) = common::single_db_registry().await;
        let db = registry
            .acquire(common::DEFAULT_TEST_DB, "test")
            .await
            .unwrap();
        let (mut writer, mut reader, _shutdown) =
            common::spawn_single_db_handler(Arc::new(NoAuthProvider), registry).await;
        common::bolt_send(&mut writer, &BoltRequest::Hello { extra: vec![] }).await;
        assert!(matches!(
            common::bolt_recv(&mut reader).await,
            BoltResponse::Success { .. }
        ));
        for end in [
            BoltRequest::Commit,
            BoltRequest::Rollback,
            BoltRequest::Reset,
        ] {
            let extra = if explicit {
                vec![(
                    "db".into(),
                    PackStreamValue::String(common::DEFAULT_TEST_DB.into()),
                )]
            } else {
                vec![]
            };
            common::bolt_send(&mut writer, &BoltRequest::Begin { extra }).await;
            let response = common::bolt_recv(&mut reader).await;
            assert!(
                matches!(response, BoltResponse::Success { .. }),
                "explicit={explicit}: {response:?}"
            );
            common::bolt_send(
                &mut writer,
                &BoltRequest::Run {
                    query: "CREATE (:Issue10)".into(),
                    params: vec![],
                    extra: vec![],
                },
            )
            .await;
            assert!(matches!(
                common::bolt_recv(&mut reader).await,
                BoltResponse::Success { .. }
            ));
            common::bolt_send(&mut writer, &BoltRequest::Pull { extra: vec![] }).await;
            assert!(matches!(
                common::bolt_recv(&mut reader).await,
                BoltResponse::Success { .. }
            ));
            common::bolt_send(&mut writer, &end).await;
            assert!(matches!(
                common::bolt_recv(&mut reader).await,
                BoltResponse::Success { .. }
            ));
        }
        let graph = db.graph();
        let mut graph = graph.write().unwrap();
        graph.vacuum_once().unwrap();
        assert_eq!(graph.nodes_by_label("Issue10").len(), 1);
        assert_eq!(graph.pending_version_chains(), 0);
    }
}

#[tokio::test]
async fn begin_rejects_invalid_database_metadata_and_recovers_after_reset() {
    for value in [
        PackStreamValue::Int(7),
        PackStreamValue::String("system".into()),
    ] {
        let (registry, _dir) = common::single_db_registry().await;
        let (mut writer, mut reader, _shutdown) =
            common::spawn_single_db_handler(Arc::new(NoAuthProvider), registry).await;
        common::bolt_send(&mut writer, &BoltRequest::Hello { extra: vec![] }).await;
        common::bolt_recv(&mut reader).await;
        common::bolt_send(
            &mut writer,
            &BoltRequest::Begin {
                extra: vec![("db".into(), value)],
            },
        )
        .await;
        assert!(matches!(
            common::bolt_recv(&mut reader).await,
            BoltResponse::Failure { .. }
        ));
        common::bolt_send(&mut writer, &BoltRequest::Reset).await;
        common::bolt_recv(&mut reader).await;
        common::bolt_send(
            &mut writer,
            &BoltRequest::Begin {
                extra: vec![("db".into(), PackStreamValue::String("neo4j".into()))],
            },
        )
        .await;
        assert!(matches!(
            common::bolt_recv(&mut reader).await,
            BoltResponse::Success { .. }
        ));
        common::bolt_send(&mut writer, &BoltRequest::Rollback).await;
        assert!(matches!(
            common::bolt_recv(&mut reader).await,
            BoltResponse::Success { .. }
        ));
    }
}

struct DenyingRegistry;
#[async_trait::async_trait]
impl ermya_graph_server::registry::GraphRegistry for DenyingRegistry {
    async fn acquire(
        &self,
        db: &str,
        _user: &str,
    ) -> Result<ermya_graph_server::registry::DbHandle, ermya_graph_server::registry::RegistryError>
    {
        use ermya_graph_server::registry::RegistryError;
        if db == "missing" {
            Err(RegistryError::DatabaseNotFound(db.into()))
        } else {
            Err(RegistryError::Unauthorized)
        }
    }
    async fn close_all(&self, _timeout: std::time::Duration) {}
}

#[tokio::test]
async fn begin_uses_registry_authorization_and_not_found_mapping() {
    for (db, code) in [
        ("missing", "Neo.ClientError.Database.DatabaseNotFound"),
        ("private", "Neo.ClientError.Security.Unauthorized"),
    ] {
        let (mut writer, mut reader, _shutdown) = common::spawn_bolt_handler_with_cap(
            Arc::new(NoAuthProvider),
            Arc::new(DenyingRegistry),
            0,
        )
        .await;
        common::bolt_send(&mut writer, &BoltRequest::Hello { extra: vec![] }).await;
        common::bolt_recv(&mut reader).await;
        common::bolt_send(
            &mut writer,
            &BoltRequest::Begin {
                extra: vec![("db".into(), PackStreamValue::String(db.into()))],
            },
        )
        .await;
        let response = common::bolt_recv(&mut reader).await;
        let BoltResponse::Failure { metadata } = response else {
            panic!("{response:?}")
        };
        assert!(
            metadata.contains(&("code".into(), PackStreamValue::String(code.into()))),
            "{metadata:?}"
        );
    }
}

#[tokio::test]
async fn database_switch_inside_transaction_is_rejected_and_reset_rolls_back() {
    let (registry, _dir) = common::single_db_registry().await;
    let db = registry
        .acquire(common::DEFAULT_TEST_DB, "test")
        .await
        .unwrap();
    let (mut writer, mut reader, _shutdown) =
        common::spawn_single_db_handler(Arc::new(NoAuthProvider), registry).await;
    for request in [
        BoltRequest::Hello { extra: vec![] },
        BoltRequest::Begin { extra: vec![] },
        BoltRequest::Run {
            query: "CREATE (:SwitchLoser)".into(),
            params: vec![],
            extra: vec![],
        },
        BoltRequest::Pull { extra: vec![] },
    ] {
        common::bolt_send(&mut writer, &request).await;
        assert!(matches!(
            common::bolt_recv(&mut reader).await,
            BoltResponse::Success { .. }
        ));
    }
    common::bolt_send(
        &mut writer,
        &BoltRequest::Run {
            query: "RETURN 1".into(),
            params: vec![],
            extra: vec![("db".into(), PackStreamValue::String("other".into()))],
        },
    )
    .await;
    assert!(matches!(
        common::bolt_recv(&mut reader).await,
        BoltResponse::Failure { .. }
    ));
    common::bolt_send(&mut writer, &BoltRequest::Reset).await;
    assert!(matches!(
        common::bolt_recv(&mut reader).await,
        BoltResponse::Success { .. }
    ));
    let graph = db.graph();
    let mut graph = graph.write().unwrap();
    graph.vacuum_once().unwrap();
    assert!(graph.nodes_by_label("SwitchLoser").is_empty());
    assert_eq!(graph.pending_version_chains(), 0);
}
