//! Durable nonce subsystem.
//!
//! Ported from `crates/fan-out-bench/src/nonce/`. Three layers:
//!
//! - [`manager`]: state machine per nonce account
//!   (Ready → InFlight → AwaitingUpdate → Stale → Ready). RR allocator.
//! - [`bootstrap`]: load nonce keypairs + fetch initial blockhashes from RPC.
//! - [`rpc_fallback`]: periodic refresh of Stale entries via RPC.
//! - [`state`]: parse `Versions` nonce-account data (80-byte layout).
//! - [`local_compute`]: consumer of `OrderedEvent::SlotComplete` from
//!   the PoH supervisor — computes the next durable nonce locally
//!   (`sha256("DURABLE_NONCE" || last_entry_hash_of_prev_slot)`) and pushes
//!   it to the manager. Eliminates the YS-account-subscription path used
//!   by the legacy bench; the supervisor already gives us a clean
//!   `last_entry_hash` per sealed slot.

pub mod bootstrap;
pub mod local_compute;
pub mod manager;
pub mod rpc_fallback;
pub mod state;
