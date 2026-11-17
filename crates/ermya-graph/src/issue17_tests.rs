use crate::{Graph, GraphConfig, props};

#[test]
fn duplicate_page_release_is_rejected_before_reuse() {
    use crate::storage::{
        backend::{DataFile, StorageBackend},
        memory::MemoryBackend,
    };
    let mut backend = MemoryBackend::new();
    let page = backend.allocate_page(DataFile::Overflow).unwrap();
    backend.free_page(DataFile::Overflow, page).unwrap();
    assert!(
        backend.free_page(DataFile::Overflow, page).is_err(),
        "double free must not create duplicate allocation candidates"
    );
    let first = backend.allocate_page(DataFile::Overflow).unwrap();
    let second = backend.allocate_page(DataFile::Overflow).unwrap();
    assert_ne!(first, second);
}

#[test]
fn open_rejects_a_free_page_still_referenced_by_a_live_node() {
    use crate::storage::{
        backend::{DataFile, StorageBackend},
        file::FileBackend,
    };
    let dir = tempfile::tempdir().unwrap();
    let config = GraphConfig::default();
    {
        let mut graph = Graph::open(dir.path(), &config).unwrap();
        graph
            .add_node("Live", props! {"payload" => "x".repeat(100)})
            .unwrap();
        graph.flush().unwrap();
    }
    {
        let mut backend = FileBackend::open(dir.path(), &config).unwrap();
        backend
            .meta_mut()
            .set_free_spare_page(DataFile::Overflow, 0);
        backend
            .meta_mut()
            .set_free_page_count(DataFile::Overflow, 1);
        backend.flush().unwrap();
    }
    assert!(
        Graph::open(dir.path(), &config).is_err(),
        "historically inconsistent free lists must not overwrite a live property's page"
    );
}

#[test]
fn missing_property_blob_reports_entity_and_page() {
    use crate::storage::{backend::StorageBackend, codec::prop_slab_codec, file::FileBackend};
    let dir = tempfile::tempdir().unwrap();
    let config = GraphConfig::default();
    let id;
    {
        let mut graph = Graph::open(dir.path(), &config).unwrap();
        id = graph
            .add_node("Live", props! {"payload" => "x".repeat(100)})
            .unwrap();
        graph.flush().unwrap();
    }
    {
        let mut backend = FileBackend::open(dir.path(), &config).unwrap();
        prop_slab_codec::free_blob(&mut backend, 0, id.0, prop_slab_codec::EntityKind::Node)
            .unwrap();
        backend.flush().unwrap();
    }
    assert!(matches!(
        Graph::open(dir.path(), &config),
        Err(crate::Error::MissingPropertyBlob {
            entity_id: 1,
            page_id: 0,
            entity_kind: "node"
        })
    ));
}

#[test]
fn persistence_failures_keep_acknowledged_commits_recoverable() {
    use crate::storage::file::test_faults::{self, Point};
    for point in [
        Point::NodeWrite,
        Point::PagesFlush,
        Point::MetaFlush,
        Point::CheckpointSync,
        Point::WalTruncate,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let config = GraphConfig::default();
        let id;
        {
            let mut graph = Graph::open(dir.path(), &config).unwrap();
            graph.enable_mvcc();
            let tx = graph.begin_txn().unwrap();
            id = graph
                .add_node_in_txn(tx, "Durable", props! {"payload" => "data".repeat(30)})
                .unwrap();
            graph.commit_txn(tx).unwrap();
            test_faults::arm(point);
            assert!(graph.flush().is_err(), "{point:?}");
            assert_eq!(
                graph.node(id).unwrap().properties(),
                &props! {"payload" => "data".repeat(30)}
            );
            if point == Point::NodeWrite {
                assert_eq!(graph.pending_version_chains(), 1);
            }
            assert!(std::fs::metadata(dir.path().join("wal.log")).unwrap().len() > 0);
        }
        let graph = Graph::open(dir.path(), &config).unwrap();
        assert_eq!(
            graph.node(id).unwrap().properties(),
            &props! {"payload" => "data".repeat(30)},
            "{point:?}"
        );
    }
}
