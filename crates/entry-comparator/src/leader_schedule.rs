use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use arc_swap::ArcSwap;
use serde::Serialize;
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;

use crate::correlator::LeaderLookup;

/// Solana mainnet has 432_000 slots per epoch.
const SLOTS_PER_EPOCH: u64 = 432_000;

pub struct LeaderCache {
    schedule: ArcSwap<HashMap<u64, [u8; 32]>>,
}

impl LeaderCache {
    /// Build cache from RPC for the epoch containing `current_slot`.
    pub fn from_rpc(rpc_url: &str, current_slot: u64) -> anyhow::Result<Arc<Self>> {
        let client = RpcClient::new(rpc_url.to_string());
        let schedule_for_slot = client
            .get_leader_schedule(Some(current_slot))?
            .ok_or_else(|| anyhow::anyhow!("RPC returned empty leader schedule"))?;

        let epoch_first_slot = current_slot - (current_slot % SLOTS_PER_EPOCH);
        let mut map = HashMap::with_capacity(SLOTS_PER_EPOCH as usize);
        for (leader_str, slots_in_epoch) in schedule_for_slot {
            let pk: Pubkey = leader_str.parse()?;
            let bytes = pk.to_bytes();
            for off in slots_in_epoch {
                map.insert(epoch_first_slot + off as u64, bytes);
            }
        }

        Ok(Arc::new(Self {
            schedule: ArcSwap::from_pointee(map),
        }))
    }

    /// Construct a cache from an in-memory map (test/loading use).
    pub fn from_map(map: HashMap<u64, [u8; 32]>) -> Arc<Self> {
        Arc::new(Self {
            schedule: ArcSwap::from_pointee(map),
        })
    }

    pub fn snapshot_to_json(&self, path: &Path) -> anyhow::Result<()> {
        let map = self.schedule.load();
        #[derive(Serialize)]
        struct Out<'a> {
            slots: &'a HashMap<u64, String>,
        }
        let serializable: HashMap<u64, String> = map
            .iter()
            .map(|(s, k)| (*s, bs58::encode(k).into_string()))
            .collect();
        let out = Out {
            slots: &serializable,
        };
        std::fs::write(path, serde_json::to_string_pretty(&out)?)?;
        Ok(())
    }
}

impl LeaderLookup for LeaderCache {
    fn lookup(&self, slot: u64) -> Option<[u8; 32]> {
        self.schedule.load().get(&slot).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_map_lookup_works() {
        let mut m = HashMap::new();
        m.insert(100u64, [7u8; 32]);
        let cache = LeaderCache::from_map(m);
        assert_eq!(cache.lookup(100), Some([7u8; 32]));
        assert_eq!(cache.lookup(101), None);
    }

    #[test]
    fn snapshot_to_json_writes_file() {
        let mut m = HashMap::new();
        m.insert(42u64, [9u8; 32]);
        let cache = LeaderCache::from_map(m);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ls.json");
        cache.snapshot_to_json(&path).unwrap();
        let s = std::fs::read_to_string(&path).unwrap();
        assert!(s.contains("\"42\""));
    }
}
