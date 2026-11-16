use ermya_graph::{Graph, GraphConfig, props};

#[test]
fn committed_node_survives_flush_without_vacuum() {
    let dir = tempfile::tempdir().unwrap();
    let config = GraphConfig::default();
    let id;
    {
        let mut graph = Graph::open(dir.path(), &config).unwrap();
        graph.enable_mvcc();
        let txn = graph.begin_txn().unwrap();
        id = graph
            .add_node_in_txn(txn, "Committed", props! {"payload" => "x".repeat(100)})
            .unwrap();
        graph.commit_txn(txn).unwrap();
        assert!(graph.node(id).is_ok());
        graph.flush().unwrap();
    }
    let graph = Graph::open(dir.path(), &config).unwrap();
    assert_eq!(
        graph.node(id).unwrap().properties(),
        &props! {"payload" => "x".repeat(100)}
    );
    assert_eq!(graph.node_ids(), vec![id]);
}

#[test]
fn committed_update_survives_flush_with_old_reader() {
    let dir = tempfile::tempdir().unwrap();
    let config = GraphConfig::default();
    let id;
    {
        let mut graph = Graph::open(dir.path(), &config).unwrap();
        id = graph
            .add_node("Committed", props! {"payload" => "before".repeat(30)})
            .unwrap();
        graph.flush().unwrap();
        graph.enable_mvcc();
        let reader = graph.begin_txn().unwrap();
        let txn = graph.begin_txn().unwrap();
        let mut node = graph.node_in_txn(txn, id).unwrap();
        *node.properties_mut() = props! {"payload" => "after".repeat(30)};
        graph.update_node_in_txn(txn, id, &node).unwrap();
        graph.commit_txn(txn).unwrap();
        graph.flush().unwrap();
        assert_eq!(
            graph.node_in_txn(reader, id).unwrap().properties(),
            &props! {"payload" => "before".repeat(30)}
        );
        // Process exit with the old reader still open must retain the writer's commit.
    }
    let graph = Graph::open(dir.path(), &config).unwrap();
    assert_eq!(
        graph.node(id).unwrap().properties(),
        &props! {"payload" => "after".repeat(30)}
    );
}

#[test]
fn overflow_update_keeps_old_reader_and_unrelated_node_intact() {
    let dir = tempfile::tempdir().unwrap();
    let config = GraphConfig::default();
    let mut graph = Graph::open(dir.path(), &config).unwrap();
    let id = graph
        .add_node("Item", props! {"payload" => "before".repeat(30)})
        .unwrap();
    let other = graph
        .add_node("Item", props! {"payload" => "other".repeat(30)})
        .unwrap();
    graph.flush().unwrap();
    graph.enable_mvcc();
    let reader = graph.begin_txn().unwrap();
    let writer = graph.begin_txn().unwrap();
    let mut node = graph.node_in_txn(writer, id).unwrap();
    *node.properties_mut() = props! {"payload" => "after".repeat(30)};
    graph.update_node_in_txn(writer, id, &node).unwrap();
    graph.commit_txn(writer).unwrap();
    assert_eq!(
        graph.node_in_txn(reader, id).unwrap().properties(),
        &props! {"payload" => "before".repeat(30)}
    );
    assert_eq!(
        graph.node(other).unwrap().properties(),
        &props! {"payload" => "other".repeat(30)}
    );
    assert_eq!(
        graph.node(id).unwrap().properties(),
        &props! {"payload" => "after".repeat(30)}
    );
}

#[test]
fn flush_keeps_uncommitted_writes_out_of_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let config = GraphConfig::default();
    let (committed, pending);
    {
        let mut graph = Graph::open(dir.path(), &config).unwrap();
        graph.enable_mvcc();
        let tx = graph.begin_txn().unwrap();
        committed = graph
            .add_node_in_txn(tx, "Item", props! {"id" => 1i64})
            .unwrap();
        graph.commit_txn(tx).unwrap();
        let tx = graph.begin_txn().unwrap();
        pending = graph
            .add_node_in_txn(tx, "Item", props! {"id" => 2i64})
            .unwrap();
        graph.flush().unwrap();
    }
    let graph = Graph::open(dir.path(), &config).unwrap();
    assert!(graph.node(committed).is_ok());
    assert!(graph.node(pending).is_err());
}

#[test]
fn failed_flush_retains_committed_wal() {
    let dir = tempfile::tempdir().unwrap();
    let config = GraphConfig::default();
    let id;
    {
        let mut graph = Graph::open(dir.path(), &config).unwrap();
        graph.enable_mvcc();
        let tx = graph.begin_txn().unwrap();
        id = graph
            .add_node_in_txn(tx, "Item", props! {"payload" => "x".repeat(100)})
            .unwrap();
        graph.commit_txn(tx).unwrap();
        std::fs::create_dir(dir.path().join("index.bin.tmp")).unwrap();
        assert!(graph.flush().is_err());
        assert!(std::fs::metadata(dir.path().join("wal.log")).unwrap().len() > 0);
        std::fs::remove_dir(dir.path().join("index.bin.tmp")).unwrap();
    }
    let graph = Graph::open(dir.path(), &config).unwrap();
    assert_eq!(
        graph.node(id).unwrap().properties(),
        &props! {"payload" => "x".repeat(100)}
    );
}

#[test]
#[ignore = "subprocess fixture; invoked by crash_recovery_after_acknowledged_flush"]
fn checkpoint_child() {
    use std::io::Write;
    let path = std::env::var_os("ERMYA_ISSUE17_CHILD_DIR").unwrap();
    let mut graph = Graph::open(path, &GraphConfig::default()).unwrap();
    graph.enable_mvcc();
    let tx = graph.begin_txn().unwrap();
    graph
        .add_node_in_txn(tx, "Durable", props! {"payload" => "x".repeat(100)})
        .unwrap();
    graph.commit_txn(tx).unwrap();
    graph.flush().unwrap();
    println!("ISSUE17_COMMIT_ACK");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}

#[test]
fn crash_recovery_after_acknowledged_flush() {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};
    let dir = tempfile::tempdir().unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", "checkpoint_child", "--nocapture"])
        .env("ERMYA_ISSUE17_CHILD_DIR", dir.path())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = BufReader::new(child.stdout.take().unwrap());
    let (sender, receiver) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in stdout.lines() {
            if line.unwrap().contains("ISSUE17_COMMIT_ACK") {
                let _ = sender.send(());
                break;
            }
        }
    });
    let acknowledged = receiver.recv_timeout(std::time::Duration::from_secs(30));
    let _ = child.kill();
    child.wait().unwrap();
    reader.join().unwrap();
    assert!(acknowledged.is_ok(), "child did not acknowledge checkpoint");
    let graph = Graph::open(dir.path(), &GraphConfig::default()).unwrap();
    let nodes = graph.nodes_by_label("Durable");
    assert_eq!(nodes.len(), 1);
    assert_eq!(
        graph.node(nodes[0]).unwrap().properties(),
        &props! {"payload" => "x".repeat(100)}
    );
}

#[test]
fn wal_recovery_invalidates_label_index_from_previous_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let config = GraphConfig::default();
    {
        let mut graph = Graph::open(dir.path(), &config).unwrap();
        graph.add_node("Item", props! {"id" => 1i64}).unwrap();
        graph.flush().unwrap();
        graph.enable_mvcc();
        let tx = graph.begin_txn().unwrap();
        graph
            .add_node_in_txn(tx, "Item", props! {"id" => 2i64})
            .unwrap();
        graph.commit_txn(tx).unwrap();
    }
    for _ in 0..2 {
        let graph = Graph::open(dir.path(), &config).unwrap();
        assert_eq!(graph.node_ids().len(), 2);
        assert_eq!(graph.nodes_by_label("Item").len(), 2);
    }
}

#[test]
fn committed_node_after_rolled_back_id_gaps_survives_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let config = GraphConfig::default();
    let id;
    {
        let mut graph = Graph::open(dir.path(), &config).unwrap();
        graph.enable_mvcc();
        let abandoned = graph.begin_txn().unwrap();
        for _ in 0..34 {
            graph
                .add_node_in_txn(abandoned, "Aborted", props! {})
                .unwrap();
        }
        graph.rollback_txn(abandoned).unwrap();
        let tx = graph.begin_txn().unwrap();
        id = graph
            .add_node_in_txn(tx, "Durable", props! {"key" => "value"})
            .unwrap();
        graph.commit_txn(tx).unwrap();
        graph.flush().unwrap();
    }
    let graph = Graph::open(dir.path(), &config).unwrap();
    assert_eq!(
        graph.node(id).unwrap().properties(),
        &props! {"key" => "value"}
    );
    assert_eq!(graph.nodes_by_label("Durable"), vec![id]);
    assert!(graph.nodes_by_label("Aborted").is_empty());
}

#[test]
fn relationship_with_id_gaps_and_property_size_transitions_survives_restarts() {
    let dir = tempfile::tempdir().unwrap();
    let config = GraphConfig::default();
    let edge;
    {
        let mut graph = Graph::open(dir.path(), &config).unwrap();
        let a = graph.add_node("Endpoint", props! {}).unwrap();
        let b = graph.add_node("Endpoint", props! {}).unwrap();
        graph.enable_mvcc();
        let tx = graph.begin_txn().unwrap();
        for _ in 0..34 {
            graph
                .add_edge_in_txn(tx, "ABORTED", a, b, props! {})
                .unwrap();
        }
        graph.rollback_txn(tx).unwrap();
        let tx = graph.begin_txn().unwrap();
        edge = graph
            .add_edge_in_txn(tx, "LINK", a, b, props! {"value" => "initial"})
            .unwrap();
        graph.commit_txn(tx).unwrap();
        graph.flush().unwrap();
    }
    let mut previous = "initial".to_string();
    for size in [200, 9000, 3] {
        let mut graph = Graph::open(dir.path(), &config).unwrap();
        assert_eq!(
            graph.edge(edge).unwrap().properties(),
            &props! {"value" => previous.clone()}
        );
        graph.enable_mvcc();
        let reader = graph.begin_txn().unwrap();
        let writer = graph.begin_txn().unwrap();
        let mut updated = graph.edge_in_txn(writer, edge).unwrap();
        let value = "x".repeat(size);
        *updated.properties_mut() = props! {"value" => value.clone()};
        graph.update_edge_in_txn(writer, edge, &updated).unwrap();
        graph.commit_txn(writer).unwrap();
        graph.flush().unwrap();
        assert_eq!(
            graph.edge_in_txn(reader, edge).unwrap().properties(),
            &props! {"value" => previous}
        );
        graph.rollback_txn(reader).unwrap();
        graph.flush().unwrap();
        previous = value;
    }
    let graph = Graph::open(dir.path(), &config).unwrap();
    assert_eq!(
        graph.edge(edge).unwrap().properties(),
        &props! {"value" => previous}
    );
}
