//! Preparer — pre-signs upcoming triggers' transactions off the hot path,
//! one variant per enabled sender, in a shuffled order so no sender_id
//! occupies a fixed first/last position across triggers.
//!
//! Per `ScheduleEntry`:
//!   1. Pull current blockhash from cache (skip if not yet primed).
//!   2. For each enabled sender: build + sign a tx with that sender's
//!      memo byte and (if applicable) a rotating tip account.
//!   3. Shuffle the resulting `Vec<PreSignedTx>` with a deterministic seed
//!      derived from `(schedule_seed, slot, tick)` — gives different orders
//!      per trigger while remaining reproducible.
//!   4. Insert the full vec into the pool keyed by `(slot, tick)`.
//!
//! The dispatcher then takes the whole vec at fire time and sends in vec
//! order (no extra shuffle needed on the hot path).

use crate::blockhash_cache::BlockhashCache;
use crate::config::{SenderConfig, TxConfig};
use crate::nonce::manager::{NonceId, NonceManager};
use crate::schedule::ScheduleEntry;
use crate::tip_accounts::{tip_accounts_for, TipAccountRotator};
use crate::trigger_engine::TriggerId;
use crate::tx_builder;
use crate::tx_pool::{PreSignedTx, TxPool};
use crossbeam_channel::Receiver;
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

#[derive(Debug, Default)]
pub struct PreparerCounters {
    /// Number of triggers fully prepared (Vec<PreSignedTx> inserted into pool).
    pub triggers_prepared: AtomicU64,
    /// Total individual variants signed (≈ triggers_prepared × n_senders).
    pub variants_signed: AtomicU64,
    pub signing_errors: AtomicU64,
    pub blockhash_not_ready: AtomicU64,
    pub pool_evictions: AtomicU64,
    /// Nonce-mode only: count of iterations where take_ready() returned None.
    /// On stall we retry the same entry instead of dropping it, so this is
    /// "retry attempts" rather than "entries lost".
    pub nonce_stall: AtomicU64,
    /// Schedule entries we received from the channel but whose slot was
    /// already in the past by the time we tried to sign — happens if we
    /// stalled on nonces for so long that the engine moved beyond them.
    pub entries_past_slot: AtomicU64,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct PreparerCountersSnapshot {
    pub triggers_prepared: u64,
    pub variants_signed: u64,
    pub signing_errors: u64,
    pub blockhash_not_ready: u64,
    pub pool_evictions: u64,
    pub nonce_stall: u64,
    pub entries_past_slot: u64,
}

impl PreparerCounters {
    pub fn snapshot(&self) -> PreparerCountersSnapshot {
        let l = |c: &AtomicU64| c.load(Ordering::Relaxed);
        PreparerCountersSnapshot {
            triggers_prepared: l(&self.triggers_prepared),
            variants_signed: l(&self.variants_signed),
            signing_errors: l(&self.signing_errors),
            blockhash_not_ready: l(&self.blockhash_not_ready),
            pool_evictions: l(&self.pool_evictions),
            nonce_stall: l(&self.nonce_stall),
            entries_past_slot: l(&self.entries_past_slot),
        }
    }
}

pub struct PreparerConfig {
    pub schedule_rx: Receiver<ScheduleEntry>,
    pub pool: Arc<TxPool>,
    pub keypair: Arc<Keypair>,
    pub blockhash_cache: Arc<BlockhashCache>,
    pub tx_cfg: TxConfig,
    /// All enabled senders. Preparer signs one variant per sender per trigger.
    pub senders: Vec<SenderConfig>,
    /// Seed used (together with slot+tick) to make the per-trigger shuffle
    /// deterministic. Pass `0` for random-but-reproducible-across-runs-with-
    /// same-config; the schedule_seed from the run config is the natural
    /// value here.
    pub shuffle_seed: u64,
    /// Highest slot the observer has seen — drives pool eviction.
    pub current_slot: Arc<AtomicU64>,
    /// When `Some`, durable-nonce mode is on: preparer takes a Ready nonce
    /// from the manager per trigger and signs with
    /// `recent_blockhash = nonce.blockhash` plus an `AdvanceNonceAccount`
    /// instruction. When `None`, fresh-blockhash mode (legacy).
    pub nonce_manager: Option<Arc<NonceManager>>,
    pub counters: Arc<PreparerCounters>,
    pub stop: Arc<AtomicBool>,
}

struct SenderSlot {
    config: SenderConfig,
    tip_rotator: Arc<TipAccountRotator>,
}

pub fn spawn(cfg: PreparerConfig) -> std::io::Result<JoinHandle<()>> {
    std::thread::Builder::new()
        .name("preparer".into())
        .spawn(move || run_loop(cfg))
}

fn run_loop(cfg: PreparerConfig) {
    // Build sender slots with per-sender tip rotators up front so the hot
    // path is just rotator.next() + tx_builder::build.
    let senders: Vec<SenderSlot> = cfg
        .senders
        .iter()
        .filter(|s| s.enabled)
        .map(|s| SenderSlot {
            config: s.clone(),
            tip_rotator: Arc::new(TipAccountRotator::new(
                tip_accounts_for(s.kind).to_vec(),
            )),
        })
        .collect();
    if senders.is_empty() {
        tracing::error!("preparer started with zero enabled senders — exiting");
        return;
    }
    let mut last_evict = Instant::now();
    // Entry currently being processed but waiting for a Ready nonce. When set,
    // the next loop iteration retries this entry instead of pulling a new one
    // from the channel. This prevents dropping schedule entries when all
    // nonces are momentarily InFlight.
    let mut pending: Option<ScheduleEntry> = None;
    loop {
        if cfg.stop.load(Ordering::Relaxed) {
            break;
        }
        let entry = match pending.take() {
            Some(e) => e,
            None => match cfg.schedule_rx.recv_timeout(Duration::from_millis(200)) {
                Ok(e) => e,
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    maybe_evict(&cfg, &mut last_evict);
                    continue;
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
            },
        };

        // Drop entries whose slot is already in the past — no point signing for
        // a slot the engine will never fire on. Without this, a long nonce
        // stall would spin forever on one stale entry.
        let current = cfg.current_slot.load(Ordering::Relaxed);
        if current > 0 && entry.slot + 1 < current {
            cfg.counters
                .entries_past_slot
                .fetch_add(1, Ordering::Relaxed);
            continue;
        }

        let trigger_id = TriggerId::from_slot_tick(entry.slot, entry.tick);
        let prepared_at = Instant::now();
        let authority_pk = cfg.keypair.pubkey();

        // Source the blockhash + optional nonce params for this trigger.
        // Nonce mode: take Ready nonce from manager. Fresh mode: use
        // blockhash cache. On stall, KEEP the entry — retry next iter.
        let (bh, nonce_params, nonce_id): (
            solana_sdk::hash::Hash,
            Option<tx_builder::NonceParams>,
            Option<NonceId>,
        ) = match &cfg.nonce_manager {
            Some(mgr) => match mgr.take_ready() {
                Some((id, nonce_pubkey, nonce_blockhash)) => (
                    nonce_blockhash,
                    Some(tx_builder::NonceParams {
                        nonce_pubkey,
                        authority: authority_pk,
                    }),
                    Some(id),
                ),
                None => {
                    cfg.counters.nonce_stall.fetch_add(1, Ordering::Relaxed);
                    pending = Some(entry);
                    // Brief sleep to avoid spinning. 1ms is short enough that
                    // a returning nonce is grabbed promptly, long enough not
                    // to peg a CPU.
                    std::thread::sleep(Duration::from_millis(1));
                    continue;
                }
            },
            None => {
                if cfg.blockhash_cache.is_empty() {
                    cfg.counters
                        .blockhash_not_ready
                        .fetch_add(1, Ordering::Relaxed);
                    pending = Some(entry);
                    std::thread::sleep(Duration::from_millis(1));
                    continue;
                }
                (cfg.blockhash_cache.current(), None, None)
            }
        };

        let mut variants: Vec<PreSignedTx> = Vec::with_capacity(senders.len());
        for slot in &senders {
            let tip_account = if slot.config.tip_lamports > 0 {
                slot.tip_rotator.next()
            } else {
                None
            };
            let built = tx_builder::build(tx_builder::BuildParams {
                payer: &cfg.keypair,
                blockhash: bh,
                sender_id: slot.config.id,
                trigger_id: trigger_id.0,
                tip_account,
                tip_lamports: slot.config.tip_lamports,
                nonce: nonce_params,
                tx_cfg: &cfg.tx_cfg,
            });
            variants.push(PreSignedTx {
                sender_id: slot.config.id,
                tx: Arc::new(built.tx),
                signature: built.signature,
                blockhash: bh,
                prepared_at,
                nonce_id,
            });
            cfg.counters.variants_signed.fetch_add(1, Ordering::Relaxed);
        }

        // Per-trigger deterministic shuffle so no sender_id always wins
        // first dispatch. Seed combines shuffle_seed with (slot, tick).
        let perm_seed = cfg
            .shuffle_seed
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            ^ (entry.slot << 8)
            ^ entry.tick as u64;
        let mut rng = SmallRng::seed_from_u64(perm_seed);
        variants.shuffle(&mut rng);

        cfg.pool.insert_all(entry.slot, entry.tick, variants);
        cfg.counters
            .triggers_prepared
            .fetch_add(1, Ordering::Relaxed);

        maybe_evict(&cfg, &mut last_evict);
    }
    // Ensure we use Vec accessor (silences unused field warning if any).
    let _ = senders.len();
}

fn maybe_evict(cfg: &PreparerConfig, last_evict: &mut Instant) {
    if last_evict.elapsed() < Duration::from_millis(500) {
        return;
    }
    *last_evict = Instant::now();
    let current = cfg.current_slot.load(Ordering::Relaxed);
    if current == 0 {
        return;
    }
    let cutoff = current.saturating_sub(4);
    let before = cfg.pool.len();
    cfg.pool.evict_below(cutoff);
    let after = cfg.pool.len();
    if before > after {
        cfg.counters
            .pool_evictions
            .fetch_add((before - after) as u64, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SenderKind;

    fn cfg(id: u8, name: &str) -> SenderConfig {
        SenderConfig {
            id,
            name: name.into(),
            kind: SenderKind::Helius,
            endpoint_url: "http://x".into(),
            tip_lamports: 1000,
            enabled: true,
        }
    }

    #[test]
    fn senders_list_filtered_to_enabled_only() {
        // smoke: building the slot list filters disabled. We exercise the
        // logic via a small in-process construction; full preparer thread is
        // tested via integration runs.
        let mut a = cfg(0, "a");
        a.enabled = false;
        let b = cfg(1, "b");
        let enabled: Vec<_> = [a, b].iter().filter(|s| s.enabled).cloned().collect();
        assert_eq!(enabled.len(), 1);
        assert_eq!(enabled[0].id, 1);
    }

    use crate::tip_accounts::helius_tip_accounts;
    use rand::SeedableRng;

    #[test]
    fn shuffle_is_deterministic_per_seed() {
        // Two shuffles with the same seed must yield the same order.
        let items: Vec<u8> = (0..8).collect();
        let mut a = items.clone();
        let mut b = items.clone();
        let mut rng_a = SmallRng::seed_from_u64(42);
        let mut rng_b = SmallRng::seed_from_u64(42);
        a.shuffle(&mut rng_a);
        b.shuffle(&mut rng_b);
        assert_eq!(a, b);
    }

    #[test]
    fn shuffle_changes_with_different_triggers() {
        // (slot, tick) differing → different seed → likely different order.
        // We can't guarantee inequality but for 5 items + different seeds the
        // probability of identical permutation is 1/120 (≈0.8%). Run multiple
        // seeds; at least one must differ.
        let items: Vec<u8> = (0..5).collect();
        let base = items.clone();
        let mut all_same = true;
        for slot in 100..120u64 {
            let seed = 0x9E37_79B9_7F4A_7C15u64
                .wrapping_mul(slot)
                ^ (slot << 8)
                ^ 5u64;
            let mut shuffled = items.clone();
            shuffled.shuffle(&mut SmallRng::seed_from_u64(seed));
            if shuffled != base {
                all_same = false;
                break;
            }
        }
        assert!(!all_same, "shuffles across 20 different triggers all identical?");
    }

    #[test]
    fn tip_rotator_for_helius_yields_distinct_accounts() {
        let r = TipAccountRotator::new(helius_tip_accounts().to_vec());
        let a = r.next().unwrap();
        let b = r.next().unwrap();
        assert_ne!(a, b);
    }
}
