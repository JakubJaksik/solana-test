# fan-out-bench

Etap 1 multi-sender Solana tx send benchmark z durable nonce dedup. Wysyła pre-signed self-transfer tx równolegle przez N senderów (Helius, Jito, Nozomi, BlockRazor, AllenHark, etc.), używa durable nonce do dedup w validation phase, zapisuje per-sender outcome/latency do parquet.

## Status (Plan 1 complete)

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

Not yet implemented (later plans):
- Plan 2: nonce setup/teardown binaries, NonceManager, YS subscription
- Plan 3: SS+YS entry merger, Observer with PoH tick tracking
- Plan 4: First real senders (Helius, Jito), Matcher state machine, Finality tracker, runtime wiring
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

Should run ~52 unit tests + 1 integration test, all passing.
