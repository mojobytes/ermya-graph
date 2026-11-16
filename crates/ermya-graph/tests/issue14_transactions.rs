use ermya_graph::{Error, Graph, NodeId, Properties, Property};

fn properties(value: &str) -> Properties {
    let mut p = Properties::new();
    p.insert("key".into(), Property::String(value.into()));
    p
}

fn graph() -> Graph {
    let mut g = Graph::new();
    g.enable_mvcc();
    g.schema_catalog_mut().add_unique_constraint("Item", "key");
    g
}

fn insert(g: &mut Graph, value: &str) -> NodeId {
    let t = g.begin_txn().unwrap();
    let id = g.add_node_in_txn(t, "Item", properties(value)).unwrap();
    g.commit_txn(t).unwrap();
    id
}

#[test]
fn unique_concurrent_commits_reject_loser_atomically_in_both_orders() {
    for reverse in [false, true] {
        let mut g = graph();
        let a = g.begin_txn().unwrap();
        let b = g.begin_txn().unwrap();
        let ia = g.add_node_in_txn(a, "Item", properties("same")).unwrap();
        let ib = g.add_node_in_txn(b, "Item", properties("same")).unwrap();
        let (winner, loser, iw, il) = if reverse {
            (b, a, ib, ia)
        } else {
            (a, b, ia, ib)
        };
        let extra = g
            .add_node_in_txn(loser, "Other", Properties::new())
            .unwrap();
        g.commit_txn(winner).unwrap();
        assert!(matches!(
            g.commit_txn(loser),
            Err(Error::ConstraintViolation { .. })
        ));
        assert!(g.node(iw).is_ok());
        assert!(g.node(il).is_err());
        assert!(g.node(extra).is_err());
        g.rollback_txn(loser).unwrap();
        assert_eq!(g.node_ids(), vec![iw]);
    }
}

#[test]
fn unique_sequential_and_same_transaction_duplicates_fail() {
    for same_transaction in [false, true] {
        let mut g = graph();
        let t = g.begin_txn().unwrap();
        g.add_node_in_txn(t, "Item", properties("same")).unwrap();
        let t = if same_transaction {
            t
        } else {
            g.commit_txn(t).unwrap();
            g.begin_txn().unwrap()
        };
        g.add_node_in_txn(t, "Item", properties("same")).unwrap();
        assert!(matches!(
            g.commit_txn(t),
            Err(Error::ConstraintViolation { .. })
        ));
        g.rollback_txn(t).unwrap();
    }
}

#[test]
fn unique_update_cannot_take_existing_key() {
    let mut g = graph();
    let a = insert(&mut g, "a");
    insert(&mut g, "b");
    let t = g.begin_txn().unwrap();
    let mut n = g.node_in_txn(t, a).unwrap();
    *n.properties_mut() = properties("b");
    g.update_node_in_txn(t, a, &n).unwrap();
    assert!(matches!(
        g.commit_txn(t),
        Err(Error::ConstraintViolation { .. })
    ));
    g.rollback_txn(t).unwrap();
    assert_eq!(g.node(a).unwrap().properties(), &properties("a"));
}

#[test]
fn unique_reuses_changed_or_deleted_key_before_vacuum() {
    for delete in [false, true] {
        let mut g = graph();
        let a = insert(&mut g, "a");
        let t = g.begin_txn().unwrap();
        if delete {
            g.remove_node_in_txn(t, a).unwrap();
        } else {
            let mut n = g.node_in_txn(t, a).unwrap();
            *n.properties_mut() = properties("b");
            g.update_node_in_txn(t, a, &n).unwrap();
        }
        g.commit_txn(t).unwrap();
        let replacement = g.add_node("Item", properties("a")).unwrap();
        assert_eq!(g.node(replacement).unwrap().properties(), &properties("a"));
    }
}

#[test]
fn unique_allows_distinct_keys_rollback_and_final_state_reuse() {
    let mut g = graph();
    let a = g.begin_txn().unwrap();
    let b = g.begin_txn().unwrap();
    g.add_node_in_txn(a, "Item", properties("a")).unwrap();
    g.add_node_in_txn(b, "Item", properties("b")).unwrap();
    g.commit_txn(a).unwrap();
    g.commit_txn(b).unwrap();
    let t = g.begin_txn().unwrap();
    g.add_node_in_txn(t, "Item", properties("rollback"))
        .unwrap();
    g.rollback_txn(t).unwrap();
    let id = insert(&mut g, "rollback");
    let t = g.begin_txn().unwrap();
    g.remove_node_in_txn(t, id).unwrap();
    g.add_node_in_txn(t, "Item", properties("rollback"))
        .unwrap();
    g.commit_txn(t).unwrap();
    assert_eq!(
        g.node_ids()
            .iter()
            .filter(|&&id| g.node(id).is_ok())
            .count(),
        3
    );
}

#[test]
fn unique_autocommit_winner_invalidates_pending_duplicate() {
    let mut g = graph();
    let t = g.begin_txn().unwrap();
    g.add_node_in_txn(t, "Item", properties("same")).unwrap();
    g.add_node("Item", properties("same")).unwrap();
    assert!(matches!(
        g.commit_txn(t),
        Err(Error::ConstraintViolation { .. })
    ));
    g.rollback_txn(t).unwrap();
}

#[test]
fn conflict_same_snapshot_has_one_winner_in_both_commit_orders() {
    for reverse in [false, true] {
        let mut g = graph();
        let id = g.add_node("Version", properties("zero")).unwrap();
        let reader = g.begin_txn().unwrap();
        let a = g.begin_txn().unwrap();
        let b = g.begin_txn().unwrap();
        for (t, value) in [(a, "one"), (b, "two")] {
            let mut n = g.node_in_txn(t, id).unwrap();
            *n.properties_mut() = properties(value);
            g.update_node_in_txn(t, id, &n).unwrap();
        }
        let (first, second, value) = if reverse {
            (b, a, "two")
        } else {
            (a, b, "one")
        };
        g.commit_txn(first).unwrap();
        let error = g
            .commit_txn(second)
            .expect_err("stale writer must not commit");
        assert!(error.to_string().contains("write conflict"), "{error}");
        g.rollback_txn(second).unwrap();
        assert_eq!(g.node(id).unwrap().properties(), &properties(value));
        assert_eq!(
            g.node_in_txn(reader, id).unwrap().properties(),
            &properties("zero")
        );
        g.rollback_txn(reader).unwrap();
        let retry = g.begin_txn().unwrap();
        let mut n = g.node_in_txn(retry, id).unwrap();
        *n.properties_mut() = properties("retry");
        g.update_node_in_txn(retry, id, &n).unwrap();
        g.commit_txn(retry).unwrap();
        assert_eq!(g.node(id).unwrap().properties(), &properties("retry"));
    }
}

#[test]
fn conflict_autocommit_update_invalidates_old_writer_and_preserves_reader() {
    let mut g = graph();
    let id = g.add_node("Version", properties("zero")).unwrap();
    let t = g.begin_txn().unwrap();
    let mut stale = g.node_in_txn(t, id).unwrap();
    *stale.properties_mut() = properties("stale");
    g.update_node_in_txn(t, id, &stale).unwrap();
    let reader = g.begin_txn().unwrap();
    let mut current = g.node(id).unwrap();
    *current.properties_mut() = properties("winner");
    g.update_node(id, &current).unwrap();
    assert!(g.commit_txn(t).is_err());
    g.rollback_txn(t).unwrap();
    assert_eq!(g.node(id).unwrap().properties(), &properties("winner"));
    assert_eq!(
        g.node_in_txn(reader, id).unwrap().properties(),
        &properties("zero")
    );
    g.rollback_txn(reader).unwrap();
}

#[test]
fn conflict_update_delete_rejects_stale_commit() {
    for delete_first in [false, true] {
        let mut g = graph();
        let id = g.add_node("Version", properties("zero")).unwrap();
        let a = g.begin_txn().unwrap();
        let b = g.begin_txn().unwrap();
        let mut n = g.node_in_txn(a, id).unwrap();
        *n.properties_mut() = properties("changed");
        g.update_node_in_txn(a, id, &n).unwrap();
        g.remove_node_in_txn(b, id).unwrap();
        let (first, second) = if delete_first { (b, a) } else { (a, b) };
        g.commit_txn(first).unwrap();
        assert!(g.commit_txn(second).is_err());
        g.rollback_txn(second).unwrap();
        assert_eq!(g.node(id).is_err(), delete_first);
    }
}

#[test]
fn conflict_independent_entities_and_own_repeated_writes_succeed() {
    let mut g = graph();
    let a = g.add_node("Version", properties("a")).unwrap();
    let b = g.add_node("Version", properties("b")).unwrap();
    let ta = g.begin_txn().unwrap();
    let tb = g.begin_txn().unwrap();
    for (t, id) in [(ta, a), (tb, b)] {
        for value in ["first", "last"] {
            let mut n = g.node_in_txn(t, id).unwrap();
            *n.properties_mut() = properties(value);
            g.update_node_in_txn(t, id, &n).unwrap();
            assert_eq!(
                g.node_in_txn(t, id).unwrap().properties(),
                &properties(value)
            );
        }
    }
    g.commit_txn(ta).unwrap();
    g.commit_txn(tb).unwrap();
}

#[test]
fn delete_committed_insert_before_and_after_vacuum() {
    for vacuum in [false, true] {
        for explicit in [false, true] {
            let mut g = graph();
            let id = insert(&mut g, "target");
            let other = insert(&mut g, "other");
            if vacuum {
                g.vacuum_once().unwrap();
            }
            let reader = g.begin_txn().unwrap();
            if explicit {
                let t = g.begin_txn().unwrap();
                g.remove_node_in_txn(t, id).unwrap();
                g.commit_txn(t).unwrap();
            } else {
                g.remove_node(id).unwrap();
            }
            assert!(g.node(id).is_err());
            assert!(g.node_in_txn(reader, id).is_ok());
            assert!(g.node(other).is_ok());
            g.rollback_txn(reader).unwrap();
            g.vacuum_once().unwrap();
            assert_eq!(g.node_count(), 1);
            assert_eq!(g.node_ids(), vec![other]);
            assert!(
                g.nodes_by_label_and_property("Item", "key", &Property::String("target".into()))
                    .is_empty()
            );
        }
    }
}

#[test]
fn delete_rollback_preserves_committed_insert() {
    let mut g = graph();
    let id = insert(&mut g, "target");
    let t = g.begin_txn().unwrap();
    g.remove_node_in_txn(t, id).unwrap();
    assert!(g.node_in_txn(t, id).is_err());
    g.rollback_txn(t).unwrap();
    assert_eq!(g.node(id).unwrap().properties(), &properties("target"));
}

#[test]
fn delete_autocommit_invalidates_pending_update() {
    let mut g = graph();
    let id = insert(&mut g, "target");
    let t = g.begin_txn().unwrap();
    let mut n = g.node_in_txn(t, id).unwrap();
    *n.properties_mut() = properties("changed");
    g.update_node_in_txn(t, id, &n).unwrap();
    g.remove_node(id).unwrap();
    assert!(g.commit_txn(t).is_err());
    g.rollback_txn(t).unwrap();
    assert!(g.node(id).is_err());
}

#[test]
fn delete_survives_reopen_without_resurrection() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = ermya_graph::GraphConfig::default();
    let id;
    {
        let mut g = Graph::open(dir.path(), &cfg).unwrap();
        g.enable_mvcc();
        id = insert(&mut g, "target");
    }
    {
        let mut g = Graph::open(dir.path(), &cfg).unwrap();
        g.enable_mvcc();
        assert!(g.node(id).is_ok());
        g.remove_node(id).unwrap();
    }
    let g = Graph::open(dir.path(), &cfg).unwrap();
    assert!(g.node(id).is_err());
    assert_eq!(g.node_count(), 0);
}

#[test]
fn delete_vacuum_removes_every_historical_key_and_cascades_edges() {
    let mut g = graph();
    let id = insert(&mut g, "first");
    let other = insert(&mut g, "other");
    let t = g.begin_txn().unwrap();
    let edge = g
        .add_edge_in_txn(t, "LINK", id, other, Properties::new())
        .unwrap();
    g.commit_txn(t).unwrap();
    for value in ["second", "third"] {
        let mut n = g.node(id).unwrap();
        *n.properties_mut() = properties(value);
        g.update_node(id, &n).unwrap();
    }
    g.remove_node(id).unwrap();
    assert!(g.edge(edge).is_err());
    g.vacuum_once().unwrap();
    assert_eq!(g.node_count(), 1);
    assert_eq!(g.edge_count(), 0);
    assert!(g.incoming_edges(other).unwrap().is_empty());
    for value in ["first", "second", "third"] {
        assert!(
            g.nodes_by_label_and_property("Item", "key", &Property::String(value.into()))
                .is_empty()
        );
    }
}

#[test]
fn rejected_commit_never_reappears_during_wal_recovery() {
    for conflict in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let cfg = ermya_graph::GraphConfig::default();
        let (winner, rejected);
        {
            let mut g = Graph::open(dir.path(), &cfg).unwrap();
            g.enable_mvcc();
            g.schema_catalog_mut().add_unique_constraint("Item", "key");
            winner = g.add_node("Item", properties("same")).unwrap();
            let t = g.begin_txn().unwrap();
            rejected = g
                .add_node_in_txn(t, "Other", properties("rejected"))
                .unwrap();
            if conflict {
                let mut node = g.node_in_txn(t, winner).unwrap();
                *node.properties_mut() = properties("loser");
                g.update_node_in_txn(t, winner, &node).unwrap();
                *node.properties_mut() = properties("winner");
                g.update_node(winner, &node).unwrap();
            } else {
                g.add_node_in_txn(t, "Item", properties("same")).unwrap();
            }
            assert!(g.commit_txn(t).is_err());
            // Drop without rollback/flush: recovery must not find a Commit
            // marker for any of this transaction's writes.
        }
        let mut g = Graph::open(dir.path(), &cfg).unwrap();
        g.enable_mvcc();
        assert!(g.node(rejected).is_err());
        assert_eq!(
            g.node(winner).unwrap().properties(),
            &properties(if conflict { "winner" } else { "same" })
        );
        assert_eq!(g.node_count(), 1);
    }
}
