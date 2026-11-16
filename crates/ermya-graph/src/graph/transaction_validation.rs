use super::Graph;
use crate::error::{Error, NodeId, Result};
use crate::index::PropertyIndex;
use crate::mvcc::{Delta, EntityKey, EntitySnapshot};
use crate::property::Property;
use std::collections::HashSet;

impl Graph {
    pub(super) fn with_autocommit<T>(
        &mut self,
        write: impl FnOnce(&mut Self, u64) -> Result<T>,
    ) -> Result<T> {
        let txn = self.begin_txn()?;
        let result = write(self, txn).and_then(|value| {
            self.commit_txn(txn)?;
            Ok(value)
        });
        if result.is_err() && self.txn_is_active(txn) {
            // rollback always releases in-memory state even if its WAL fails.
            let _ = self.rollback_txn(txn);
        }
        result
    }

    pub(super) fn validate_write_conflicts(
        &self,
        txn: u64,
        writes: &[(EntityKey, Delta)],
    ) -> Result<()> {
        let start = self.txn_start_ts(txn)?;
        let table = self.delta_table.as_ref().ok_or(Error::MvccNotEnabled)?;
        for (key, _) in writes {
            if table.chain_for(*key).is_some_and(|chain| {
                chain.iter().any(|delta| {
                    delta.txn_id() != txn
                        && delta
                            .commit_ts()
                            .is_some_and(|committed| committed >= start)
                })
            }) {
                return Err(Error::WriteConflict);
            }
        }
        Ok(())
    }

    /// Index entries are a historical superset until vacuum. Check the current
    /// committed version, never the committing transaction's older snapshot.
    pub(super) fn committed_node_has_value(
        &self,
        id: NodeId,
        label: &str,
        key: &str,
        value: &Property,
    ) -> Result<bool> {
        let node = match self.node(id) {
            Ok(node) => node,
            Err(Error::NodeNotFound(_)) => return Ok(false),
            Err(error) => return Err(error),
        };
        Ok(node.label() == label
            && node
                .properties()
                .get(key)
                .is_some_and(|actual| crate::index::property_values_equal(actual, value)))
    }

    /// Called under exclusive Graph access before any WAL redo is emitted.
    /// Validate the final write set so deletes/key changes in this commit can
    /// release a key, while two inserts in the same commit cannot claim it.
    pub(super) fn validate_unique_commit(&self, writes: &[(EntityKey, Delta)]) -> Result<()> {
        if self.schema_catalog.constraints().is_empty() {
            return Ok(());
        }
        let mut changed = HashSet::new();
        let mut proposed = PropertyIndex::new();
        for (entity, delta) in writes {
            if let EntityKey::Node(id) = entity {
                changed.insert(id.0);
                if let Some(EntitySnapshot::Node(node)) = delta.new_state() {
                    for (key, value) in node.properties() {
                        if self.schema_catalog.has_unique_constraint(node.label(), key) {
                            proposed.insert(node.label(), key, value, id.0);
                        }
                    }
                }
            }
        }
        for (entity, delta) in writes {
            let (EntityKey::Node(id), Some(EntitySnapshot::Node(node))) =
                (entity, delta.new_state())
            else {
                continue;
            };
            for (key, value) in node.properties() {
                if !self.schema_catalog.has_unique_constraint(node.label(), key) {
                    continue;
                }
                let mut duplicate = proposed
                    .lookup_set(node.label(), key, value)
                    .is_some_and(|ids| ids.iter().any(|other| *other != id.0));
                if let Some(candidates) =
                    self.node_property_index
                        .lookup_set(node.label(), key, value)
                {
                    for &other in candidates {
                        if !changed.contains(&other)
                            && self.committed_node_has_value(
                                NodeId(other),
                                node.label(),
                                key,
                                value,
                            )?
                        {
                            duplicate = true;
                            break;
                        }
                    }
                }
                if duplicate {
                    return Err(Error::ConstraintViolation {
                        label: node.label().to_owned(),
                        prop: key.clone(),
                        value: format!("{value:?}"),
                    });
                }
            }
        }
        Ok(())
    }
}
