//! Tracker — per-tx state machine dla klasyfikacji inclusion.

use alloy::primitives::B256;
use serde::Serialize;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum InclusionStatus {
    Pending { target_block: u64 },
    IncludedTarget,
    IncludedLate(u64),
    Dropped,
    SendError(String),
}

pub struct Tracker {
    lookahead: u64,
    states: HashMap<B256, InclusionStatus>,
    targets: HashMap<B256, u64>,
    resolved: HashMap<B256, ResolvedTx>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResolvedTx {
    pub tx_hash: B256,
    pub status: InclusionStatus,
    pub included_block: Option<u64>,
}

impl Tracker {
    pub fn new(lookahead: u64) -> Self {
        Self {
            lookahead,
            states: HashMap::new(),
            targets: HashMap::new(),
            resolved: HashMap::new(),
        }
    }

    pub fn record_sent(&mut self, tx_hash: B256, _sent_at_block: u64, target_block: u64) {
        self.states
            .insert(tx_hash, InclusionStatus::Pending { target_block });
        self.targets.insert(tx_hash, target_block);
    }

    pub fn record_send_error(&mut self, tx_hash: B256, err: String) {
        self.states
            .insert(tx_hash, InclusionStatus::SendError(err.clone()));
        self.resolved.insert(
            tx_hash,
            ResolvedTx {
                tx_hash,
                status: InclusionStatus::SendError(err),
                included_block: None,
            },
        );
    }

    pub fn observe_block(&mut self, block_num: u64, tx_hashes: &HashSet<B256>) {
        let mut newly_resolved = Vec::new();
        for (tx, state) in self.states.iter() {
            if !matches!(state, InclusionStatus::Pending { .. }) {
                continue;
            }
            let target = self.targets[tx];
            if tx_hashes.contains(tx) {
                let status = if block_num == target {
                    InclusionStatus::IncludedTarget
                } else {
                    InclusionStatus::IncludedLate(block_num.saturating_sub(target))
                };
                newly_resolved.push((*tx, status, Some(block_num)));
            } else if block_num > target + self.lookahead {
                newly_resolved.push((*tx, InclusionStatus::Dropped, None));
            }
        }
        for (tx, status, included) in newly_resolved {
            self.states.insert(tx, status.clone());
            self.resolved.insert(
                tx,
                ResolvedTx {
                    tx_hash: tx,
                    status,
                    included_block: included,
                },
            );
        }
    }

    pub fn status(&self, tx: &B256) -> Option<InclusionStatus> {
        self.states.get(tx).cloned()
    }

    pub fn pending_count(&self) -> usize {
        self.states
            .values()
            .filter(|s| matches!(s, InclusionStatus::Pending { .. }))
            .count()
    }

    pub fn drain_resolved(&mut self) -> Vec<ResolvedTx> {
        let out: Vec<_> = self.resolved.values().cloned().collect();
        self.resolved.clear();
        out
    }
}
