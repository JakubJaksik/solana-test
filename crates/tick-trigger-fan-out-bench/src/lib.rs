//! tick-trigger-fan-out-bench — modular rebuild of the Solana send-path bench.
//!
//! Built phase-by-phase. Each phase MUST be:
//!   1. Independently runnable end-to-end (own `bin/phaseN_*.rs`)
//!   2. Metric'd — observable behaviour captured in counters/JSON
//!   3. Tested — unit + integration coverage
//!   4. Composable — next phase consumes the previous phase's output channel
//!
//! ## Current scope (phase 1)
//!
//! Two-source entry observation pipeline:
//!
//! ```text
//!   ShredStream gRPC ──┐
//!                      ├─▶ entry_merger ──▶ MergedEntry stream
//!   Yellowstone gRPC ──┘     (dedup +         (downstream consumers
//!                             ordering         get unique entries in
//!                             tracking)        slot/index order)
//! ```
//!
//! The phase 1 binary [`phase1_observe`](../bin/phase1_observe.rs) ingests
//! ~60s of mainnet entries and reports:
//! - per-source receive counts
//! - dedup hit rate (which source saw a given entry first; how often the
//!   second source also reported it)
//! - per-slot entry/tick counts (expecting 64 ticks for healthy slots)
//! - **out-of-order arrival counts** — how often an entry with index N arrived
//!   AFTER the merger had already emitted some entry with index > N for the
//!   same slot. This drives downstream design: anything later that wants
//!   strict ordering must tolerate this rate.
//!
//! ## Future phases (not yet implemented)
//!
//! - phase 2: PoH tick tracking + schedule firing
//! - phase 3: nonce-free fan-out sender (multi-vendor, fresh blockhash)
//! - phase 4: parquet writer + finality tracker
//! - phase 5: optional durable-nonce mode
//!
//! Modules below are deliberately small; new phases will add their own
//! `phaseN_*` modules in parallel rather than extending existing ones.

pub mod blockhash_cache;
pub mod config;
pub mod merger;
pub mod nonce;
pub mod ordering;
pub mod poh_supervisor;
pub mod preparer;
pub mod recorder;
pub mod schedule;
pub mod senders;
pub mod tip_accounts;
pub mod trigger_engine;
pub mod tx_builder;
pub mod tx_pool;
pub mod wallet;
