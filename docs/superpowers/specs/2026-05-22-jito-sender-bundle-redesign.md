# Jito sender redesign: bundle fan-out with durable nonce, throwaway tipper, multi-IP per-protocol

**Status:** design approved, ready for implementation plan
**Date:** 2026-05-22
**Crate:** `tick-trigger-fan-out-bench`
**Replaces:** `crates/tick-trigger-fan-out-bench/src/senders/jito.rs` (single-tx multi-region sender)

## Goal

Replace the current single-transaction Jito sender with a **bundle sender** that mirrors how
`dex-trader` (working reference at `/home/jjaksik/Repos/dex-trader`) submits Jito bundles, with
two project-specific twists:

1. The benchmark uses **durable nonces** (preparer pre-signs transactions ahead of `(slot, tick)`).
2. We rotate **5 outbound IPs**, applying the binding to **both JSON-RPC and gRPC** transports
   (dex-trader binds only JSON-RPC; we are intentionally more strict).

The exclusion property — "fan-out N copies, exactly one lands" — is enforced by a single
durable-nonce account shared by all pre-signed copies of a given trigger's first transaction.

## Behavior summary (per Jito trigger)

- **1 bundle = 2 transactions**, pre-signed by preparer:
  - **Tx1** (durable nonce, signed by main wallet):
    `AdvanceNonceAccount(A)` + memo + self-transfer + `transfer(main → tipper, tip + rent_exempt + base_fee)`
  - **Tx2** (standard tx, signed by **throwaway tipper keypair**, fresh blockhash):
    `transfer(tipper → jito_tip_account, tip_lamports)` + `transfer(tipper → main_wallet, rent_exempt)`
- **Fan-out**: same bundle posted to **8 Jito hosts × 2 methods (JSON-RPC + gRPC) = 16 parallel sends**.
- **IP rotation**: 5 outbound IPs, **per-trigger** — one IP index picked, applied to all 16 requests for that trigger; cursor advances to next IP for next trigger.
- **Throttle**: `min_send_interval_ms` (existing knob) gates send frequency; if a trigger arrives within
  the interval, the send is skipped with `throttled_local`.
- **Tip lamports**: dynamic, refreshed every 30s by `JitoTipUpdater` against
  `https://bundles.jito.wtf/api/v1/bundles/tip_floor`, log-interpolated to configured percentile,
  clamped to `[tip_floor, tip_ceiling]`. Preparer snapshots the current value at pre-sign time.
- **Tip account**: random per pre-sign from the existing 8-entry `tip_accounts_for(SenderKind::Jito)` list.
- **Throwaway tipper**: fresh `Keypair::new()` per pre-sign, private key dropped after signing;
  on-chain account is left at 0 lamports after Tx2 (rent_exempt returned to main wallet via `send_back`).

## Durable nonce semantics — exclusion guarantee

- Tx1 is a durable-nonce transaction: `AdvanceNonceAccount` is the first instruction, and
  `recent_blockhash` is set to nonce account A's current stored hash `H`.
- Preparer pre-signs N identical bundles for the same `(slot, tick)`, each using nonce A with stored hash `H`.
- When the first bundle lands, `AdvanceNonceAccount(A)` advances `H → H'`. Every other bundle's Tx1
  fails nonce validation (`stored_hash (H') ≠ tx.recent_blockhash (H)`) **pre-execution**, the
  entire bundle is dropped, Tx2 never executes, no fee is charged.
- Net cost of a losing fan-out copy: **0 lamports**.
- Net cost of the winning bundle: `2 × base_fee + tip_lamports` (Tx1 base + Tx2 base + tip).
  `rent_exempt` is returned in the same Tx2 via `send_back`.

## Architecture changes

### Files (module layout)

```
crates/tick-trigger-fan-out-bench/src/
├── senders/
│   ├── mod.rs                  # TxSender trait extended with send_bundle default
│   ├── helius.rs               # unchanged
│   └── jito/
│       ├── mod.rs              # JitoBundleSender (impl TxSender::send_bundle)
│       ├── json_rpc.rs         # HostIpMatrix<reqwest::Client> + per-host JSON-RPC POST
│       ├── grpc.rs             # HostIpMatrix<tonic::Channel> + per-host SearcherService::send_bundle
│       ├── tip_updater.rs      # JitoTipUpdater background task
│       └── proto/
│           └── searcher.proto  # vendored from jito-labs/mev-protos
├── preparer.rs                 # extended to build 2-tx Jito bundles
├── tx_builder.rs               # BuildParams.fund_tipper + build_tipper_tx()
├── tx_pool.rs                  # PreSignedTx.extra_txs + bundle_metadata
└── bin/phase3_run.rs           # dispatcher picks send vs send_bundle based on extra_txs
```

The current `senders/jito.rs` (single-tx multi-region) is **removed in full** and its config kind
`jito` is reused by the new bundle sender (config-compatible; old single-tx mode no longer exists).

### `TxSender` trait extension

```rust
#[async_trait]
pub trait TxSender: Send + Sync {
    fn id(&self) -> u8;
    fn name(&self) -> &str;
    fn endpoint_url(&self) -> &str;
    fn protocol(&self) -> &'static str;
    async fn send(&self, tx: &Transaction) -> SendOutcome;

    /// Default: returns an error. Implemented only by JitoBundleSender.
    async fn send_bundle(&self, _txs: &[Transaction]) -> SendOutcome {
        SendOutcome {
            send_at: Instant::now(),
            error: Some("sender does not support bundles".into()),
            ..SendOutcome::default()  // requires deriving Default
        }
    }
}
```

Helius and other single-tx senders are untouched. `JitoBundleSender` overrides `send_bundle` and
returns an error from `send(&Transaction)` (Jito always bundles in this benchmark).

### `tx_builder.rs` changes

```rust
pub struct BuildParams<'a> {
    // existing fields…
    pub nonce: Option<NonceParams>,
    pub tx_cfg: &'a TxConfig,

    // NEW:
    /// When Some, append a transfer(payer → target, lamports) instruction to Tx1.
    /// Used by Jito bundle build to fund the throwaway tipper.
    pub fund_tipper: Option<(Pubkey, u64)>,
}

/// Build Tx2 for a Jito bundle: signed by throwaway tipper, fresh blockhash,
/// pays tip to a Jito tip account and returns rent_exempt back to main wallet.
pub fn build_tipper_tx(
    tipper: &Keypair,
    blockhash: Hash,
    tip_account: Pubkey,
    tip_lamports: u64,
    main_wallet: Pubkey,
    rent_exempt_lamports: u64,
) -> BuiltTx { /* ... */ }
```

Constants:
- `RENT_EXEMPT_MIN_LAMPORTS: u64 = 890_880` (System-owned 0-byte account; stable). Hardcoded; if
  Solana ever changes the rent schedule, update the constant.
- `BASE_TX_FEE_LAMPORTS: u64 = 5_000`.

### `tx_pool.rs` changes

```rust
pub struct PreSignedTx {
    pub sender_id: u8,
    pub tx: Arc<Transaction>,                  // Tx1 (signature tracked in pending_sigs)
    pub signature: Signature,                  // Tx1.signatures[0]
    pub blockhash: Hash,
    pub prepared_at: Instant,
    pub nonce_id: Option<NonceId>,

    // NEW:
    pub extra_txs: Vec<Arc<Transaction>>,      // Tx2 for Jito; empty Vec for other senders
    pub bundle_metadata: Option<BundleMeta>,   // diagnostic only
}

pub struct BundleMeta {
    pub tipper_pubkey: Pubkey,
    pub tip_account: Pubkey,
    pub tip_lamports: u64,
    pub tx2_blockhash: Hash,
}
```

`extra_txs` is `Vec<Arc<Transaction>>` (not `Option<Vec>`) — empty vec for single-tx senders, cheap.

### `preparer.rs` changes

For Jito sender, per `(slot, tick)` pre-sign:

```rust
let tipper = Keypair::new();
let rent_exempt = RENT_EXEMPT_MIN_LAMPORTS;
let base_fee = BASE_TX_FEE_LAMPORTS;
let tip_lamports = jito_sender.current_tip_lamports();   // atomic snapshot
let tip_account  = jito_tip_rotator.next_random();        // random from 8-entry list
let fund_amount  = tip_lamports + rent_exempt + base_fee;

let nonce = nonce_manager.take_ready()?;                  // 1 nonce per send
let stored_hash = nonce.stored_hash;
let fresh_bh    = bh_cache.current();                     // for Tx2

let tx1 = tx_builder::build(BuildParams {
    payer: &main_keypair, blockhash: stored_hash,
    sender_id: jito_sender.id(), trigger_id,
    tip_account: None, tip_lamports: 0,                   // tip is NOT in Tx1 anymore
    nonce: Some(NonceParams { nonce_pubkey: nonce.pubkey, authority: main_keypair.pubkey() }),
    tx_cfg, fund_tipper: Some((tipper.pubkey(), fund_amount)),
});

let tx2 = tx_builder::build_tipper_tx(
    &tipper, fresh_bh, tip_account, tip_lamports, main_keypair.pubkey(), rent_exempt,
);

pool.push(PreSignedTx {
    sender_id: jito_sender.id(),
    tx: Arc::new(tx1.tx), signature: tx1.signature,
    blockhash: stored_hash, prepared_at: now(),
    nonce_id: Some(nonce.id),
    extra_txs: vec![Arc::new(tx2.tx)],
    bundle_metadata: Some(BundleMeta {
        tipper_pubkey: tipper.pubkey(), tip_account, tip_lamports,
        tx2_blockhash: fresh_bh,
    }),
});
// tipper keypair dropped here — private key gone; account will GC after epoch
```

Other senders (Helius, Triton in the future) use the same preparer with `extra_txs: vec![]` and
`fund_tipper: None`. **Multi-sender benchmarks work out-of-the-box.**

### Dispatcher (`bin/phase3_run.rs`)

Per-variant branch in `dispatch_one`:

```rust
if presigned.extra_txs.is_empty() {
    let outcome = sender.send(&presigned.tx).await;
    /* … */
} else {
    let mut bundle = Vec::with_capacity(1 + presigned.extra_txs.len());
    bundle.push((*presigned.tx).clone());
    for extra in &presigned.extra_txs {
        bundle.push((**extra).clone());
    }
    let outcome = sender.send_bundle(&bundle).await;
    /* … */
}
```

`pending_sigs.insert(presigned.signature)` (Tx1 sig only). Tx2 signature is intentionally not tracked.

### `JitoBundleSender`

```rust
pub struct JitoBundleSender {
    id: u8,
    name: String,
    endpoint_template: String,
    json_rpc: HostIpMatrix<reqwest::Client>,            // 8 hosts × N IPs
    grpc:     Option<HostIpMatrix<tonic::transport::Channel>>,  // 8 × N or None
    ip_count: usize,
    ip_cursor: AtomicUsize,                              // per-trigger rotation
    current_tip_lamports: Arc<AtomicU64>,               // updated by JitoTipUpdater
    min_send_interval: Duration,
    last_send_at: parking_lot::Mutex<Option<Instant>>,
}

struct HostIpMatrix<T> {
    hosts: Vec<String>,                                   // 8 resolved endpoint URLs
    grid:  Vec<Vec<T>>,                                   // grid[host_idx][ip_idx]
}
```

`send_bundle(&self, txs: &[Transaction])`:
1. Throttle check (`min_send_interval`). If throttled → return `throttled_local`.
2. Pick `ip_idx = ip_cursor.fetch_add(1) % ip_count`.
3. Serialize bundle: `txs_b64 = txs.iter().map(|t| base64(bincode::serialize(t))).collect()`.
4. Build JSON-RPC body once (`Arc<String>`); for gRPC build `Bundle { packets }` with raw bytes.
5. Spawn 8 JSON-RPC tasks (one per host, client = `json_rpc.grid[host_idx][ip_idx]`).
6. If `grpc.is_some()`: spawn 8 gRPC tasks (one per host, channel = `grpc.grid[host_idx][ip_idx]`).
7. Use `tokio::sync::mpsc` (cap 16). First reply → build `SendOutcome`. Drop the receiver to
   ignore remaining replies; background tasks still complete so the bundle reaches all 16 paths.

**SendOutcome for a bundle:**
- `signature` = `txs[0].signatures[0]` (Tx1 anchor — matches what's in `pending_sigs`).
- `provider_request_id` = `bundle_id` from first successful reply (JSON-RPC returns a string;
  gRPC returns a UUID — serialize as string).
- `endpoint_url_used` = `"{host}/{JSON-RPC|gRPC}"`.

### gRPC details

- Proto: `searcher.proto` from `https://github.com/jito-labs/mev-protos/blob/master/searcher/searcher.proto`,
  vendored to `senders/jito/proto/searcher.proto`. `tonic-build` in `build.rs` generates Rust types.
- Service / method: `searcher.SearcherService/SendBundle(SendBundleRequest)` with
  `Bundle { packets: Vec<Packet>, header: ... }`, `Packet { data: Vec<u8>, meta: ... }`.
  `data` carries the raw bincode-serialized transaction bytes (no base64).
- Endpoint: `https://{host}:443` (TLS HTTP/2).
- **Auth: none** — Jito public block engine accepts unauthenticated `sendBundle` calls; default rate
  limit is 1 req/s per IP per region (confirmed in Jito docs: `https://docs.jito.wtf/lowlatencytxnsend/`).
- **Per-IP binding** uses `tonic 0.13+`'s native `Endpoint::local_address(Some(IpAddr::V4(src)))`.
  This requires upgrading the workspace `tonic` dep from `0.12` → `0.13` (also impacts `entry-sources`
  which uses tonic for Geyser gRPC). Minor API breakage expected; included in the implementation plan
  as an early step.

```rust
// Per (host, ip) channel — built once at sender construction:
let channel = Endpoint::from_shared(format!("https://{host}:443"))?
    .tls_config(ClientTlsConfig::new().domain_name(host))?
    .local_address(Some(IpAddr::V4(src_ip)))
    .connect_lazy();
```

### `JitoTipUpdater`

- `tokio::spawn` background task started at sender construction.
- Every `tip_refresh_interval_ms` (default 30_000), GET `https://bundles.jito.wtf/api/v1/bundles/tip_floor`.
- Parse JSON: `[{ landed_tips_25th_percentile, landed_tips_50th_percentile, landed_tips_75th_percentile,
  landed_tips_95th_percentile, landed_tips_99th_percentile, ... }]`.
- Log-interpolate at `tip_percentile` config value (matches dex-trader's `jito-tip-calculator.ts`):
  pick the two surrounding percentile data points, take `Math.log` of each tip, linear-interpolate the
  log values at the target percentile, `Math.exp` the result, multiply by `10^9` (SOL → lamports).
- Clamp result to `[tip_floor_lamports, tip_ceiling_lamports]`.
- Store in `Arc<AtomicU64>`. Sender exposes `current_tip_lamports() -> u64` for the preparer.
- On HTTP error: log warning, retain previous value, retry next interval.

### Config schema (per Jito sender entry)

```jsonc
{
  "id": 2,
  "name": "jito-bundle",
  "kind": "jito",
  "endpoint_url": "https://{region}.mainnet.block-engine.jito.wtf",
  "regions": ["frankfurt","amsterdam","dublin","london","ny","tokyo","slc","singapore"],
  "outbound_ips": ["1.2.3.4","1.2.3.5","1.2.3.6","1.2.3.7","1.2.3.8"],
  "use_grpc": true,
  "tip_percentile": 75,
  "tip_floor_lamports": 15000,
  "tip_ceiling_lamports": 2000000,
  "tip_refresh_interval_ms": 30000,
  "min_send_interval_ms": 0
}
```

The fields `tip_lamports` (static) and any single-tx leftovers are removed from this sender's config
section. Other senders' config is unchanged.

### Metrics (added to existing `DispatcherCounters` / report)

- `jito_bundles_sent: AtomicU64`
- `jito_first_reply_json_rpc: AtomicU64`
- `jito_first_reply_grpc: AtomicU64`
- `jito_ip_send_count: [AtomicU64; 5]` (per-IP sanity check for rotation)

All increments use `Ordering::Relaxed`; cost is negligible (< 1 ns) and does not impact
trigger→send latency.

## Compatibility

- **Helius and other current senders**: untouched. They implement `TxSender::send`, dispatcher
  routes to single-tx path because `extra_txs` is empty.
- **Future Triton (or any new single-tx sender)**: drop-in. Add a new module under `senders/`,
  add a `SenderKind` variant, route in the config-to-sender builder in `phase3_run.rs`. The preparer
  treats it like Helius.
- **Multi-sender benchmark runs (Helius + Jito + future Triton)**: supported. Each trigger fans out
  to all enabled senders; each sender uses its own variant from the pre-signed pool. No
  cross-sender coupling.

## Out of scope (intentionally NOT done in this change)

- gRPC auth (`x-jito-auth`) for higher rate limits — not needed for default sends.
- `Versioned`/`v0` transaction support — current code uses legacy `Transaction`. Jito bundles
  accept both; we stick with legacy.
- Address Lookup Tables.
- Tipper account explicit `closeAccount` instruction (rent reclaim already happens via the
  `send_back` transfer; remaining 0-lamport account is GC'd by Solana epoch cleanup).
- Persistence of tipper keypairs (intentionally throwaway; private key dropped after signing).

## Pre-existing context for implementers

- Current single-tx `JitoSender` at `crates/tick-trigger-fan-out-bench/src/senders/jito.rs:27`
  (entire file is deleted by this change).
- Preparer at `crates/tick-trigger-fan-out-bench/src/preparer.rs` (extended).
- `tx_builder.rs` at `crates/tick-trigger-fan-out-bench/src/tx_builder.rs:49` (BuildParams).
- Dispatcher loop at `crates/tick-trigger-fan-out-bench/src/bin/phase3_run.rs:596-756`.
- Tip account list at `crates/tick-trigger-fan-out-bench/src/tip_accounts.rs:54`
  (`jito_tip_accounts()`).
- dex-trader reference (TypeScript, working in production):
  `/home/jjaksik/Repos/dex-trader/app/trader-module/chains/solana/jito/`.

## Cleanup pre-implementation

The branch `master` carries 6 temp commits (`9f039ff..02e72cf`, "temp: …") from prior debugging.
Per user direction, these are removed via:

```
git reset --hard 49ca45d
git push --force-with-lease origin master
```

This is a destructive operation on a shared branch; user explicitly authorized it (it is a personal
benchmark repo with a single contributor).

## Verified facts (codex cross-check 2026-05-22)

- **dex-trader gRPC** uses `@grpc/grpc-js`, no auth header, **no per-IP binding** (round-robin
  client pool only). Our design intentionally adds per-IP for gRPC — stricter than dex-trader.
- **Tonic 0.13.0** added native `Endpoint::local_address` (release note #1567). Workspace upgrade
  from 0.12.3 → 0.13.x required; `entry-sources` gRPC code may need minor API adjustments.
- **Jito public block engine gRPC** accepts unauthenticated `sendBundle` (cited:
  `https://docs.jito.wtf/lowlatencytxnsend/`).
