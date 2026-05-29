# Triton sender: minimal single-tx HTTP `sendTransaction` (Jet/SWQoS)

**Status:** design approved, ready for implementation plan
**Date:** 2026-05-29
**Crate:** `tick-trigger-fan-out-bench`
**Adds:** `crates/tick-trigger-fan-out-bench/src/senders/triton.rs` (new) + `SenderKind::Triton`

## Goal

Add Triton One as a third send method in the fan-out latency benchmark, alongside Helius (`/fast`)
and Jito, optimized for the lowest possible trigger→send and send→inclusion latency. The Triton
variant reuses the same pre-signed durable-nonce transaction as the other senders (one shared nonce
per trigger → exactly one variant lands → memo attributes the winner), so the bench measures Triton's
send path head-to-head against Helius and Jito.

This is intentionally the **simplest** of the three senders: a near-clone of `HeliusSender`.

## Background / research findings

Verified against official Triton docs + `rpcpool/yellowstone-jet` (2026-05):

- **Transport: HTTP JSON-RPC `sendTransaction`.** Yellowstone gRPC ("Dragon's Mouth") is
  subscribe/read-only — it has **no transaction-submission method**. Sending is over HTTP, exactly
  like the Helius `/fast` path.
- **Fast path = Jet + SWQoS, automatic and free.** Since 2026-03-05 every Triton customer's
  `sendTransaction` is routed through Jet (tracks leader schedule, pre-connects to upcoming leaders
  over QUIC, stake-weighted delivery) at no extra cost. Nothing to enable in code or config. This is
  the dominant send→inclusion lever and it is delivered server-side.
- **Auth = token in the URL path:** `https://<endpoint>.mainnet.rpcpool.com/<TOKEN>`. (The `x-token`
  header is the gRPC mechanism — not used for HTTP send.)
- **No tip required.** Inclusion is driven by the priority fee (`SetComputeUnitPrice`). Unlike Jito,
  no tip account / tip instruction is needed.
- **Durable nonce is safe.** Plain `sendTransaction` forwards the pre-signed bytes verbatim to the
  leader; `skipPreflight: true` avoids any nonce-vs-recent-blockhash preflight rejection. The
  `replace_recent_blockhash` hazard exists only in the opt-in Jito `simulateBundle` API
  (dedicated-node feature, defaults `false`) — which we do not call.
- **Request shape is identical to Helius `/fast`:** base64, `skipPreflight: true`, `maxRetries: 0`
  (no `preflightCommitment`).
- **Self-hosting `yellowstone-jet` is out of scope:** the SWQoS speed advantage requires a staked
  validator identity, which a ~$125 shared/PAYG plan does not provide; an unstaked self-hosted jet
  would land in the congested public TPU lane (slower than hosted `sendTransaction`). Revisit only
  if a staked-connection arrangement is obtained.

## Non-goals (YAGNI)

- No multi-region / multi-IP fan-out (Jet routes to leaders server-side; client-side fan-out adds
  cost and rate-limit pressure with no latency gain).
- No `min_send_interval_ms` local throttle (Helius does not have it either; easy to add later).
- No pre-serialization of the base64 body in the preparer (cross-cutting; ~µs gain; separate change).
- No changes to the Helius or Jito senders (including not back-porting connection warm-up to them).

## Design

### New module `senders/triton.rs`

```rust
pub struct TritonSender {
    id: u8,
    name: String,
    endpoint: String,          // full URL incl. token — private, POST target only
    endpoint_display: String,  // token-redacted (scheme + host) — used for logs/records
    client: reqwest::Client,
}

impl TritonSender {
    pub fn new(id: u8, name: impl Into<String>, endpoint: impl Into<String>) -> Self { ... }

    /// Fire-and-forget connection pre-warm: spawns a lightweight `getHealth` on the
    /// background runtime so the first real send reuses a warm keep-alive connection
    /// instead of paying TCP+TLS handshake on the hot path. reqwest::Client clones
    /// share the same connection pool, so warming a clone warms the sender's pool.
    pub fn spawn_warmup(&self, handle: &tokio::runtime::Handle) { ... }
}

#[async_trait]
impl TxSender for TritonSender {
    fn id(&self) -> u8;
    fn name(&self) -> &str;
    fn endpoint_url(&self) -> &str;          // returns endpoint_display (NO token)
    fn protocol(&self) -> &'static str { "TRITON" }
    async fn send(&self, tx: &Transaction) -> SendOutcome { ... }
}
```

- **Client tuning** (mirrors `HeliusSender::new`, helius.rs:22-35): `timeout`, `tcp_nodelay(true)`,
  `pool_max_idle_per_host`, `tcp_keepalive`. HTTP/1.1 keep-alive (crate's `reqwest` is
  `default-features = false`, so no HTTP/2 feature is assumed/added).
- **`send()`**: capture `send_at` **before** the await (so the timestamp and the actual transmission
  do not wait on the ack) → `bincode::serialize(tx)` → base64 → POST. On reply, parse JSON-RPC and
  map to `SendOutcome` (signature / `http_status` / `rpc_err_code` + `rpc_err_message` /
  `endpoint_url_used`, token-redacted — see Secret handling). Network error → `SendOutcome` with `error`.
- **Secret handling:** the token lives in `endpoint`'s path and is used only as the POST target.
  `endpoint_url()` and `SendOutcome.endpoint_url_used` return `endpoint_display` (scheme + host, token
  stripped), so the secret never reaches tracing (phase3_run.rs:346) or the run JSONL
  (recorder.rs:649 `endpoint_url` / `endpoint_url_used`). Computed once in `new()`.

### Request format

```
POST https://<endpoint>.mainnet.rpcpool.com/<TOKEN>
Content-Type: application/json

{ "jsonrpc": "2.0", "id": 1, "method": "sendTransaction",
  "params": [ "<base64(bincode(tx))>",
              { "encoding": "base64", "skipPreflight": true, "maxRetries": 0 } ] }
```

Differs from Helius only by the **absence of `preflightCommitment`** (Triton does not need it under
`skipPreflight: true`).

### Testable units (TDD)

To keep tests network-free (no mock-HTTP crate in dev-deps), the serde request/response types and the
outcome mapping are factored so they can be unit-tested directly (same approach as the Jito sender's
`send_transaction_request_serializes_per_jito_spec`):

- `SendRequest` / `SendOptions` serde structs → assert serialized JSON has `method=sendTransaction`,
  `encoding=base64`, `skipPreflight=true`, `maxRetries=0`, **no** `preflightCommitment`, and
  `params[0] == base64(bincode(tx))`.
- `JsonRpcResponse` deserialization + a small `outcome_from_*` mapper → success body yields the
  signature; error body yields `rpc_err_code` + `rpc_err_message`; non-JSON body yields `error`.

### Touch points (5 localized changes)

1. **`config.rs:155-158`** — add `Triton` to `SenderKind` (serde `rename_all = "snake_case"` →
   config value `"triton"`).
2. **`senders/mod.rs:7-8`** — `pub mod triton;`.
3. **`tip_accounts.rs:66-71`** — add `SenderKind::Triton => &[]` arm to `tip_accounts_for` (no tips).
4. **`bin/phase3_run.rs:311`** — add factory arm:
   ```rust
   SenderKind::Triton => {
       let t = TritonSender::new(sc.id, sc.name.clone(), sc.endpoint_url.clone());
       t.spawn_warmup(&bg_handle);
       Arc::new(t) as Arc<dyn TxSender>
   }
   ```
5. **`senders/triton.rs`** — new module + unit tests.

### Unchanged components (and why)

- **`preparer.rs` — no change.** Triton is non-Jito and runs with `tip_lamports = 0`, so the existing
  tip logic (preparer.rs:217-240) yields `tip_account = None` and `tx_builder` skips the tip transfer
  (tx_builder.rs:124-132). The shared durable nonce, memo, priority fee, and self-transfer flow
  identically. Triton just needs a `NonceParams` like every other sender — already provided per
  trigger by `take_ready()`.
- **`tx_builder.rs`, `nonce/*`, `recorder.rs` — no change.** Triton receives its own `sender_id`;
  memo encoding and signature-based attribution work automatically; it shares the same nonce as the
  other variants (exactly one lands per trigger).

### Transaction shape (Triton variant)

`[ AdvanceNonceAccount, SetComputeUnitLimit, SetComputeUnitPrice, Memo(triton_sender_id), SelfTransfer ]`
— no tip instruction.

### Config

```json
{ "id": <next>, "name": "triton-fra", "kind": "triton",
  "endpoint_url": "https://<ep>.mainnet.rpcpool.com/<TOKEN>",
  "tip_lamports": 0, "enabled": true }
```

- The token is a **secret**: it lives only in the (gitignored) config file, never in code.
- The shared `tx.priority_fee_microlamports` must be `> 0` (Triton relies on the priority fee).

## Durable-nonce safety

Triton's `sendTransaction` forwards the pre-signed bytes verbatim (no re-sign, no blockhash
replacement); `skipPreflight: true` bypasses the preflight that would otherwise reject a tx whose
`recent_blockhash` is a nonce value rather than a live blockhash. The only `replace_recent_blockhash`
path is the opt-in Jito `simulateBundle` API (defaults `false`) — not on this send path. **Do not**
route these txs through `simulateBundle` with `replace_recent_blockhash: true`.

## Attribution

The memo `"{sender_id:02x}:{trigger_id:016x}"` carries Triton's `sender_id`; `TriggerId = (slot<<8) | tick`
is reversible, so the on-chain memo alone identifies the winning method and the `(slot, tick)`. The
recorder additionally maps `signature → (trigger_id, sender_id)` and logs `sender_name`,
`endpoint_url`, `protocol`. No recorder changes needed.

## Latency rationale

- **send→inclusion:** Jet + SWQoS (stake-weighted, QUIC, leader pre-connect) — automatic, server-side.
- **trigger→send:** tx is pre-signed off the hot path by the preparer; the sender does
  serialize + POST only. Warm keep-alive connection (pre-warm at startup) avoids handshake on the
  first send; `tcp_nodelay` avoids Nagle delay; `maxRetries: 0` makes it fire-once (no retry
  blocking); `send_at` is taken before the await so measurement and transmission do not wait on the
  HTTP ack.

## Testing strategy (TDD)

Network-free unit tests (no mock-HTTP dependency):

1. Request serialization: method/encoding/`skipPreflight`/`maxRetries`, **no** `preflightCommitment`,
   `params[0] == base64(bincode(tx))`.
2. Response mapping: success → signature; JSON-RPC error → `rpc_err_code` + `rpc_err_message`;
   non-JSON body → `error`.
3. Config: `"triton"` deserializes to `SenderKind::Triton`; `tip_accounts_for(Triton)` is empty.
4. Secret redaction: `endpoint_url()` (and the recorded `endpoint_url_used`) never contain the token.

Run with the project's existing `cargo test -p tick-trigger-fan-out-bench`.

## Account / ops checklist (user, outside code)

1. **Activate the Tier 3 endpoint** (currently inactive); token is already Active.
2. Build the send URL `https://<endpoint>.mainnet.rpcpool.com/<TOKEN>` and put it in the config's
   Triton `endpoint_url`.
3. Bench runs in **Frankfurt** → want EU routing. GeoDNS auto-routes to the nearest EU DC; hard region
   pinning (Amsterdam/EU) is not self-serve on shared/PAYG — confirm with Triton support / BGP Anycast.
4. Confirm with Triton support: (a) is the account on the default Jet/SWQoS send path; (b) is the plan
   shared or dedicated and is sustained `sendTransaction` bot traffic allowed without throttling;
   (c) `sendTransaction` RPS / connection limits (shared default ≈ 1200 req / 10 s per method).

## Risks / open questions

- **Shared-plan throttling:** Triton docs steer sustained bot traffic to dedicated nodes. For a
  bursty bench it is likely fine, but a 429 under load would show up as `SendError` in the recorder.
- **Region pinning:** EU pinning may require support/Anycast; without it GeoDNS handles routing.
- **Empirical verification:** confirm on-chain that a Triton-sent durable-nonce tx both **lands** and
  **advances the nonce** (Triton publishes no explicit durable-nonce guarantee — it is safe by the
  standard `sendTransaction` contract + `skipPreflight`).
- **Pre-existing secret-in-URL (Helius):** the Helius sender keeps its `?api-key=` URL and logs/records
  it unredacted; Triton redacts its token. Flagged for awareness — not changing Helius here (out of
  requested scope).

## Out of scope / future

- Self-hosted `yellowstone-jet` in Frankfurt (needs staked identity — not at the $125 tier).
- Client-side multi-IP/region fan-out for Triton.
- Pre-serializing the base64 body in the preparer (cross-cutting micro-opt).
- Connection warm-up for the Helius/Jito senders.
