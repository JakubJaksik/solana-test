# fan-out-bench

Etap 1 multi-sender Solana tx send benchmark z durable nonce dedup. Wysyła pre-signed self-transfer tx równolegle przez N senderów (Helius, Jito, Nozomi, BlockRazor, AllenHark, etc.), używa durable nonce do dedup w validation phase, zapisuje per-sender outcome/latency do parquet.

## Status

Plan 1 — foundation:
- ✅ Crate scaffolding + workspace member
- ✅ Config types + example JSON
- ✅ Schedule (deterministic, chunked, open-ended)
- ✅ Memo encoder (ASCII printable, UTF-8 safe)
- ✅ Tip account registry (12 senderów, RR rotator)
- ✅ TxBuilder z hard asserts (AdvanceNonce ix[0], etc.)
- ✅ Pool (DashMap-backed)
- ✅ TxSender trait + MockSender
- ✅ Parquet schema (61 kolumn) + writer
- ✅ Counters
- ✅ End-to-end mock pipeline test

Plan 2 — nonce infrastructure:
- ✅ Wallet keypair loader
- ✅ Nonce state parsing (80-byte layout → authority + blockhash)
- ✅ NonceManager state machine (Ready/InFlight/AwaitingUpdate/Stale)
- ✅ RR allocator with take_ready()
- ✅ Bootstrap (getMultipleAccounts)
- ✅ YS gRPC subscription for live updates
- ✅ RPC fallback poller for Stale nonces
- ✅ setup_nonces binary (create N pool)
- ✅ teardown_nonces binary (refund rent)

Plan 3 — entry observation:
- ✅ TriggerEvent type
- ✅ EntryMerger (SS + YS dedup by (slot, entry_hash))
- ✅ Observer (PoH tick counter + schedule match + trigger emit)
- ✅ Integration test (mock stream → merger → observer → trigger)

Plan 4 — pipeline + first senders:
- ✅ HTTP JSON-RPC helper
- ✅ HeliusSender (HTTP, with optional swqos_only + api-key)
- ✅ JitoSender (single tx via /api/v1/transactions)
- ✅ TriggerId hashing (slot + tick + nonce_id → 16-byte id)
- ✅ MatchEvent + observer sig matching (pending_sigs DashSet)
- ✅ Matcher state machine (single-owner per (TriggerId, sender_id))
- ✅ Preparer (NonceManager + tx_builder + Pool integration)
- ✅ Dispatcher (async fan-out, deterministic perm seed)
- ✅ Runtime wiring (merger+observer+preparer+dispatcher+matcher+parquet)
- ✅ `run` CLI binary (3rd bin, idle until SS/YS hookup)
- ✅ Pipeline mock integration test (1 LANDED + 2 DEDUPED verified)
- ⏸ Finality tracker (Plan 5)
- ⏸ RPC fallback for UNKNOWN_PENDING (Plan 5)
- ⏸ SS/YS gRPC client wiring in runtime (Plan 5)

Not yet implemented (later plans):
- Plan 5: REST senders (Nozomi, 0slot, bloXroute, Astralane, Syncro, Triton)
- Plan 6: gRPC/QUIC senders (BlockRazor, AllenHark, NextBlock, Harmonic)
- Plan 7: Ops + polish (budget watcher, clock monitor, probe-senders, smoke harness)

## Reference

- Design spec: `../../docs/superpowers/specs/2026-05-14-fan-out-bench-design.md`
- Implementation plans: `../../docs/superpowers/plans/2026-05-14-fan-out-bench-*.md`

## Quick test

```bash
cargo test -p fan-out-bench
```

Should run all unit + integration tests (~76 total), all passing.

## Setup nonce pool (one-time, manual)

Creates N durable nonce accounts on Solana mainnet/devnet. Rent (~0.0014 SOL × N) is locked and refundable via teardown.

```bash
cargo build --release -p fan-out-bench
./target/release/setup_nonces \
  --rpc-url <HELIUS_OR_TRITON_RPC_URL> \
  --wallet ~/.config/solana/dex-bench.json \
  --count 150 \
  --output-keypairs nonce-keypairs.json \
  --output-config nonce-config.json
```

Cost: ~0.22 SOL lockup for N=150 (refundable), <0.005 SOL tx fees.

## Teardown nonce pool

Withdraws rent from all nonce accounts back to wallet:

```bash
./target/release/teardown_nonces \
  --rpc-url <RPC_URL> \
  --wallet ~/.config/solana/dex-bench.json \
  --keypairs nonce-keypairs.json
```
