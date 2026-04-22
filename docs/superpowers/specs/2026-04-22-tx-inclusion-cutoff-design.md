# tx-cutoff — Design Spec

**Data:** 2026-04-22
**Autor:** brainstorm z jjaksik
**Status:** approved design, awaiting user spec review before implementation plan

---

## 1. Problem statement

Dla HFT na EVM potrzebujemy precyzyjnie zmierzyć **transaction inclusion cutoff** — najpóźniejszy moment (względem poprzedniego bloku), w którym wysłana transakcja nadal trafia do kolejnego bloku. Wynikiem ma być krzywa "% inclusion w target block vs. slot_ms (czas po block.timestamp)", pozwalająca stwierdzić: "jeśli wysyłam później niż X ms po block N.timestamp, prawdopodobieństwo inclusion w N+1 spada poniżej Y%".

Narzędzie testowo działa na **Base**, docelowo na **Ethereum mainnet**. Ma być EVM-kompatybilne, z konfigurowalnym RPC.

## 2. Goals / Non-goals

### Goals

- Zmierzyć inclusion cutoff w konfigurowalnym oknie (domyślnie 8500–11500 ms po `block.timestamp`, krok 50 ms).
- 100 tx na slot czasowy (5 walletów × 20 sampli), zebrane przez wiele bloków (1 tx/wallet/blok, deterministic sequential fill).
- Minimalna latencja send-path (pre-signed tx, hybrid sleep, pinowane worker threads).
- Realistyczne swapy (Uniswap V3 ping-pong WETH↔USDC), żeby tx nie failowały z powodu semantyki swapu.
- Precyzyjny pre-flight z pomiarem gazu i confirm prompt.
- Działa na serwerze (statyczna musl-binarka, x86_64 i aarch64).
- Raport: JSONL per-tx + CSV summary + markdown report + stdout table.

### Non-goals

- Nie jest to tool production-trading — to tool pomiarowy.
- Nie mierzymy mempool propagation / P2P observability.
- Nie robimy private-pool / MEV-builder routing (zwykły `eth_sendRawTransaction` do jednego RPC).
- Bez retry/replace-by-fee wygasłych tx (missed → counted as dropped, kontynuujemy).
- Bez GUI / web dashboardu.
- Bez resume after crash.

## 3. Design decisions (podsumowanie)

| # | Decyzja | Wybór | Uzasadnienie |
|---|---------|-------|--------------|
| 1 | Punkt odniesienia dla slot_ms | `block.timestamp` poprzedniego bloku | Mierzymy "ile ms po deklarowanym czasie bloku N tx nadal trafia w N+1". |
| 2 | Język | Rust + tokio + alloy-rs | Niska latencja (microsecond send path), zero runtime deps, statyczna binarka. |
| 3 | Swap mechanism | Uniswap V3 ping-pong (WETH↔USDC) | Realistyczny DEX flow, niski gas, per-wallet state machine unika "wyschnięcia" tokenu. |
| 4 | Gas strategy | Profil konfigurowalny `{max_priority_fee_gwei, max_fee_multiplier}` | Deterministyczny, pozwala robić sweepy (różny tip → przesunięta krzywa cutoff). |
| 5 | Tx type | EIP-1559 type-2 | Standard na dzisiaj, spójny z jak się realnie handluje. |
| 6 | Pre-flight calibration | Realny swap per wallet (nie `eth_estimateGas`) | Prawdziwy `gasUsed`, rozgrzewa storage slots, robi approve w tym samym runie. |
| 7 | Auto-approve | Tak, `type(uint256).max`, raz per wallet w pre-flight | Eliminuje pierwszorazowy approve z main loop (inny gas, failure risk). |
| 8 | Schedulowanie | 1 tx / wallet / blok, wszystkie 5 walletów targetują ten sam slot per blok, deterministyczne sekwencyjne wypełnianie: bloki 0–19 → slot[0], 20–39 → slot[1], ... | Czyste pomiary (brak nonce coupling między slotami), prosty mental model. |
| 9 | Nonce | Pre-signed raw tx, lokalny cache inkrementowany per udany send | Minimalizuje hot-path: hot path = tylko `sendRawTransaction`. |
| 10 | Config format | JSON, wallety inline | User preference. |
| 11 | RPC | Osobny WS (`newHeads` subscription) + HTTP (send) | WS dla push-powiadomień o blokach, HTTP dla send — każdy zoptymalizowany per rola. |
| 12 | Sleep precision | Hybrid: tokio `sleep_until(target - 2ms)` + busy-wait ostatnie 2 ms | Wake jitter ~<100 μs p99 przy ~0.5% CPU cost. |
| 13 | Runtime isolation | 2× tokio runtime: `send_rt` (hot path, pinowane) + `main_rt` (reszta) | Tracker / report / log nie spowalnia send path. |
| 14 | Tracker | 1 RPC per newHead (`eth_getBlockByNumber(num, false)`) | Tylko tx_hashes, minimum ruchu po sieci. |
| 15 | Abort conditions | Pre-flight fail / insufficient funds / N consecutive blocks all-errors / WS dead / SIGTERM graceful | Fail-fast, bez defensive retry masking real issues. |
| 16 | Reporting | Minimal progress co 50 bloków + finalny: JSONL + CSV + markdown + stdout table z ASCII chart | Dane dla analizy + human summary. |
| 17 | Cutoff percentiles w raporcie | 50%, 90%, 95%, 99% | User preference. |
| 18 | Deployment | Statyczna musl binarka (x86_64 + aarch64), bez Dockera / systemd | AWS EC2 xlarge, Amazon Linux / Ubuntu, ręczne odpalanie. |
| 19 | Resume after crash | Nie | User preference. |

## 4. Architecture

### Module breakdown

```
tx-cutoff/
├── main.rs        — CLI entry, config load, orchestration
├── config.rs      — JSON config structs + validation
├── rpc.rs         — WS newHeads subscriber + HTTP sender (keep-alive)
├── wallet.rs      — Key load, nonce cache, EIP-1559 signing
├── swap.rs        — Uniswap V3 calldata builder, ping-pong state machine
├── scheduler.rs   — block_index → (slot_ms, sample_idx) deterministic
├── preflight.rs   — 6-step pre-flight + confirmation prompt
├── engine.rs      — Main loop: newHead → pre-sign batch → timed sends
├── tracker.rs     — Per-tx state machine: Sent → Included | Dropped | Error
├── report.rs      — JSONL writer + CSV + markdown + stdout
└── time.rs        — Monotonic clock, hybrid sleep helper
```

### Data flow

```
                 ┌───────────────────┐
                 │  WS newHeads sub  │
                 └────────┬──────────┘
                          │ newHead(block_N)
                          ▼
           ┌──────────────────────────────┐
           │  engine.on_new_head()         │
           │  1. get slot_ms from scheduler│
           │  2. build calldata (per wallet)│
           │  3. fetch next nonce (cached)  │
           │  4. build + sign EIP-1559 tx   │
           │  5. spawn per-wallet task      │
           └───────────┬──────────────────┘
                       │  pre-signed raw tx + target_time
                       ▼
           ┌──────────────────────────────┐
           │  hot-path task (send_rt)      │
           │  sleep_until(target - 2ms)    │
           │  busy-wait to target          │
           │  http.send_raw_transaction()  │◄── HTTP RPC
           │  record (t_pre, t_post, res)  │
           └───────────┬──────────────────┘
                       │  TxRecord
                       ▼
           ┌──────────────────────────────┐
           │  tracker (main_rt)            │
           │  on newHead: get block.tx_hashes│
           │  classify each pending TxRecord│
           │  write to jsonl               │
           └───────────┬──────────────────┘
                       │  final stats
                       ▼
           ┌──────────────────────────────┐
           │  report.generate()            │
           │  jsonl → aggregate per slot   │
           │  emit csv + md + stdout chart │
           └───────────────────────────────┘
```

### Threading / runtime

- **Main runtime (`main_rt`)**: default tokio runtime. WS subscriber, tracker, report writer, logging, shutdown handler.
- **Send runtime (`send_rt`)**: dedykowany `tokio::runtime::Builder` z `wallets.len()` worker threads (jeden dedicated thread per wallet), każdy pinowany do oddzielnego CPU core przez `core_affinity`. Tasks: timed send per wallet per block. Brak innej pracy na tym runtime. Powód: wszystkie wallety budzą się na ten sam `target_time` i wysyłają równolegle — gdyby liczba wątków < liczby walletów, niektóre wysyłki czekałyby w kolejce schedulera przez czas trwania poprzedniej, dodając 2–20 ms do części sampli.
- Komunikacja między runtime: `tokio::sync::mpsc` channels (bounded).

### Krytyczna ścieżka latencji (hot path)

Po `sleep_until` wake:

1. `now_monotonic()` → ~50 ns
2. `http.post(pre_serialized_json_rpc)` → ~2–20 ms (RTT do RPC)
3. `now_monotonic()` → ~50 ns
4. `channel.send(record)` → ~1 μs

Wszystko poza tym robione **przed** `sleep_until`:
- Raw signed tx (`Bytes`)
- Pre-serialized JSON-RPC payload (jako `String`)
- Keep-alive HTTP connection (pre-warmed `reqwest::Client`)

## 5. Configuration

### Format: JSON, wallety inline, `.gitignore` + `chmod 600`

```json
{
  "chain": {
    "name": "base",
    "chain_id": 8453,
    "rpc_ws": "wss://base-mainnet.g.alchemy.com/v2/KEY",
    "rpc_http": "https://base-mainnet.g.alchemy.com/v2/KEY"
  },
  "timing": {
    "start_ms": 8500,
    "end_ms": 11500,
    "step_ms": 50,
    "samples_per_wallet_per_slot": 20
  },
  "gas": {
    "max_priority_fee_gwei": 0.01,
    "max_fee_multiplier": 3.0
  },
  "tracking": {
    "inclusion_lookahead_blocks": 10,
    "abort_on_consecutive_failed_blocks": 5
  },
  "swap": {
    "router_address": "0x2626664c2603336E57B271c5C0b26F421741e481",
    "pool_fee_tier": 500,
    "token_a": "0x4200000000000000000000000000000000000006",
    "token_b": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
    "amount_in_a": "1000000000000000",
    "amount_in_b": "3000000",
    "slippage_bps": 300
  },
  "wallets": [
    {"label": "w1", "private_key": "0xabc..."},
    {"label": "w2", "private_key": "0xdef..."}
  ],
  "output": {
    "dir": "./runs",
    "stdout_report": true
  }
}
```

Opcjonalna sekcja `send` do tuningu (defaults działają dla większości scenariuszy):

```json
  "send": {
    "worker_threads": 5,     // default = wallets.len()
    "spin_window_us": 2000   // default = 2000 (2 ms)
  }
```

### Validation (fail-fast przy starcie)

- `start_ms < end_ms`, `step_ms > 0`
- `(end_ms - start_ms) % step_ms == 0` (warning, nie error, jeśli nie dzieli się równo)
- RPC `eth_chainId` == `chain.chain_id` (obrona przed pomyłką env)
- `router_address`, `token_a`, `token_b` są kontraktami (`eth_getCode` ≠ `0x`)
- `wallets.len() >= 1`, każdy private_key valid secp256k1
- `inclusion_lookahead_blocks >= 2`
- Labels walletów unique

### Security

- Private keys w configu → README ostrzeżenie: `chmod 600 config.json`, `.gitignore`
- Logi NIGDY nie zawierają private key — tylko label + skrócony address (`0xabc...1234`)
- Opcjonalny override per-pole przez env var (np. `TX_CUTOFF_CHAIN_RPC_WS=wss://...`) dla CI/secret managers (nice-to-have, nie MVP)

## 6. Scheduling

### Deterministyczny plan

Dla bloku o indeksie `i` (0-based, inkrementowany per `newHead`):

```
slots_count   = (end_ms - start_ms) / step_ms + 1        # np. 61
total_blocks  = slots_count * samples_per_wallet_per_slot # np. 1220
slot_index    = i / samples_per_wallet_per_slot           # np. i=0..19 → 0
sample_index  = i % samples_per_wallet_per_slot           # 0..19
slot_ms       = start_ms + slot_index * step_ms

if i >= total_blocks:
    stop()
```

Wszystkie wallety w tym samym bloku targetują ten sam `slot_ms`. 5 tx per blok (1 per wallet).

### Pre-sign flow (on newHead)

```
on newHead(block_N):
    received_instant = Instant::now()
    received_unix_ms = SystemTime::now() as unix_ms
    i = current_block_index
    if i >= total_blocks: shutdown(); return
    
    slot_ms = scheduler.slot_for(i)
    target_unix_ms = block_N.timestamp * 1000 + slot_ms
    offset_ms = target_unix_ms - received_unix_ms              # może być ujemne → skip tej serii (zbyt późno)
    if offset_ms < 0: log warning "missed target"; continue
    target_instant = received_instant + Duration::from_millis(offset_ms)
    
    batch = []
    for wallet in wallets:
        calldata = swap.next_calldata(wallet)    # ping-pong state
        nonce = wallet.next_nonce()              # cached, incremented on ok send
        tx = build_eip1559_tx(
            chain_id, nonce, to=router,
            data=calldata, value=0,
            gas_limit=gas_limit_cached[wallet],  # preflight + 20% buffer
            max_priority_fee, max_fee=baseFee_N * multiplier + tip,
        )
        signed_raw = wallet.sign(tx)
        payload_str = serialize_jsonrpc("eth_sendRawTransaction", [signed_raw])
        batch.push(SendTask{wallet, signed_raw, tx_hash, payload_str, ...})
    
    plan.record(block_index=i, block_hash=N.hash, target_unix_ms, target_instant, slot_ms, batch)
    
    for task in batch:
        send_rt.spawn(async move {
            hybrid_sleep_until(target_instant).await;
            let t_pre = Instant::now();
            let result = http.raw_jsonrpc_post(task.payload_str).await;
            let t_post = Instant::now();
            tx_records_tx.send(TxRecord{
                block_idx: i, block_hash, slot_ms, sample_idx,
                wallet: task.wallet.label, tx_hash: task.tx_hash, nonce,
                target_unix_ms, target_instant,
                t_pre_send: t_pre, t_post_send: t_post,
                wake_jitter_us: (t_pre - target_instant).as_micros(),
                rpc_rtt_us: (t_post - t_pre).as_micros(),
                send_result,
            }).await;
        });
```

**Edge case "missed target":** jeśli newHead dociera *po* tym jak target_unix_ms już minął (np. bo RPC miał spike, albo slot_ms jest mały a WS ma lag), skipujemy ten blok w planie — odnotowujemy jako "skipped_late_newhead" w logach i czekamy na następny newHead. Block index NIE jest inkrementowany w tym przypadku, żeby plan dokończył się zgodnie z konfiguracją.

### Hybrid sleep

Używamy **monotonic clock (`Instant`)** dla precyzyjnego busy-wait, nie unix ms (rozdzielczość ms byłaby za gruba).

Przy odbiorze `newHead` liczymy:
```rust
let received_instant = Instant::now();
let received_unix_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as i64;
let offset_ms = target_unix_ms as i64 - received_unix_ms;
let target_instant = received_instant + Duration::from_millis(offset_ms.max(0) as u64);
```

Hot-path sleep:
```rust
async fn hybrid_sleep_until(target: Instant) {
    let now = Instant::now();
    let spin_window = Duration::from_millis(2);
    if target > now + spin_window {
        tokio::time::sleep(target - now - spin_window).await;
    }
    // Busy-wait last ~2ms using nanosecond-precision Instant comparison
    while Instant::now() < target {
        std::hint::spin_loop();
    }
}
```

Dzięki `Instant` mamy rozdzielczość nanosekundową. `wake_jitter = t_pre_send - target` jest **zawsze ≥ 0** (busy-wait wychodzi tuż po osiągnięciu target).

Expected wake jitter przy pinowanym worker thread: p50 ~10–20 μs, p99 <500 μs.

CPU cost: `wallets.len()` busy-waits × 2 ms per blok. Base (2s blocks, 5 wallets): ~0.5% jednego rdzenia (łącznie). ETH (12s): ~0.08%.

**Core affinity a liczba vCPU:**

Dla optymalnego wake jitter: `vCPU ≥ wallets.len() + 2` (2 dla `main_rt`).
- 5 walletów → zalecany **2xlarge (8 vCPU)** lub większy.
- Standardowy **xlarge (4 vCPU)** — tool zadziała, ale przy `wallets.len() > 2` busy-wait niektórych walletów serializuje się z innymi, dodając 0.5–2 ms do wake jitter ostatnich w kolejce.
- Jeśli detekcja cores < wallets + 2: tool loguje warning i automatycznie zmniejsza `spin_window` do `max(100 μs, available_core_slack)` żeby nie zagłodzić main_rt.

Config override: `send.worker_threads` (opcjonalne, default = `wallets.len()`), `send.spin_window_us` (opcjonalne, default 2000).

### Nonce management

- Startup: `eth_getTransactionCount(wallet, "pending")` per wallet, cache lokalnie jako `AtomicU64`.
- Na udany `eth_sendRawTransaction`: `nonce += 1`.
- Na `nonce too low` error: re-fetch z chain, update cache, log warning, kontynuuj (nie retry tego tx — czas już minął).
- 1 tx/wallet/blok eliminuje wewnątrz-blokową contention.

### Swap ping-pong state

Per wallet, in-memory stan `SwapDirection::AtoB` albo `BtoA`. Przełączamy po każdym zaplanowanym swapie (nie po sukcesie — zakładamy że swap się wykona). Jeśli kolejność się rozjedzie z realnymi balansami (np. swap się powtórzył bo tx była w mempoolu), swap v2 failuje z `INSUFFICIENT_INPUT_AMOUNT` → liczone jako regular send error.

Przy starcie wallet direction inicjalizowane na podstawie faktycznych balansów: jeśli `balance_a > balance_b * price → start AtoB`, else `BtoA`.

## 7. Pre-flight

Wszystkie kroki blokujące; failure na dowolnym → abort z exit code ≠ 0.

### Kroki

1. **RPC sanity check**
   - Connect WS + HTTP
   - Verify `eth_chainId == config.chain.chain_id`
   - Verify `eth_blockNumber` zwraca recent block (timestamp w ciągu 2 min)
   - Measure HTTP RTT (3× `eth_blockNumber`, median). Log baseline. Warning >50 ms, abort >200 ms.

2. **Wallet validation**
   - `eth_getBalance` per wallet, log bal
   - Sprawdź że ≥1 z tokenów (A lub B) ma non-zero balance (żeby zacząć ping-pong)

3. **Allowance check + auto-approve**
   - Per wallet, oba tokeny: `token.allowance(wallet, router)`
   - Jeśli `< uint256::MAX / 2` → submit `approve(router, uint256::MAX)`
   - Wait for receipts (parallel, zwykle 1 blok)
   - Log per-wallet: `approve 0x{hash} gas_used={gas}`

4. **Kalibracyjny swap per wallet**
   - Submit 1 realny swap z użyciem docelowego calldata
   - Wait for receipt, capture `gasUsed`
   - `gas_limit_cached[wallet] = gasUsed * 1.2` (20% buffer)
   - Log per-wallet table

5. **Cost projection**
   - `baseFee = latest block baseFeePerGas`
   - `tx_per_wallet = total_blocks` (1 per blok; np. 1220)
   - `per_tx_realistic = gas_avg * (baseFee + tip)`
   - `per_tx_worst = gas_avg * (baseFee * multiplier + tip)`
   - Sprawdź `balance[wallet] >= per_tx_worst * tx_per_wallet` per wallet
   - Log table

6. **User confirmation prompt** — tabela + `[y/N]`. Flaga `--yes` omija.

### Przykład output (stdout)

```
══════════════════════════════════════════════════════════
 PRE-FLIGHT SUMMARY
══════════════════════════════════════════════════════════
 Chain: base (8453)
 RPC WS:   wss://base-mainnet.g.alchemy.com/v2/***
 RPC HTTP: https://base-mainnet.g.alchemy.com/v2/***
 RPC RTT baseline (p50): 12.4 ms
 Current baseFee: 0.00213 gwei

 Plan:
   Slots:        61 (8500-11500 ms, step 50 ms)
   Samples/slot: 100 (5 wallets × 20)
   Total tx:     6100
   Est. duration: ~40 min (Base 2s blocks)

 Wallets & calibration:
 ┌───────┬──────────────────────────┬──────────┬─────────┬──────────┐
 │ Label │ Address                  │ ETH bal  │ Gas used│ Approve? │
 ├───────┼──────────────────────────┼──────────┼─────────┼──────────┤
 │ w1    │ 0xAbc...1234             │ 0.0500   │ 141,832 │ ✓ (done) │
 │ w2    │ 0xDef...5678             │ 0.0480   │ 142,104 │ ✓        │
 └───────┴──────────────────────────┴──────────┴─────────┴──────────┘

 Cost projection (per wallet, 1220 tx each):
   Realistic: 0.000422 ETH  (~$1.05 @ $2500/ETH)
   Worst:     0.001263 ETH  (~$3.15)
 Total run (realistic): 0.00211 ETH (~$5.25)

 All wallets have sufficient balance: ✓

 Proceed? [y/N]
══════════════════════════════════════════════════════════
```

## 8. Tracker / inclusion classification

### Per-tx state machine

```
Sent → Pending ──(tx w block M, M == N+1)──→ IncludedTarget
            │
            ├──(tx w block M, M > N+1)─────→ IncludedLate(offset = M - (N+1))
            │
            └──(M > N + lookahead_blocks)──→ Dropped
                (nigdy nie widziana w bloku w oknie obserwacji)

Sent → SendError(error_type) (RPC odrzucił)
```

### Implementation

- `tracker` słucha `newHeads` niezależnie od engine (ten sam WS stream).
- Na każdy `newHead(block_M)`: `eth_getBlockByNumber(M, false)` → `Vec<tx_hash>` → zbuduj `HashSet<H256>`.
- Dla każdego `tx_hash` w `pending`: jeśli w set → mark inclusion (target lub late).
- Cleanup: tx_hash z `target_block_N+1 + lookahead_blocks < M` → mark as Dropped.
- Per klasyfikacja → update `TxRecord` → write to jsonl.

## 9. Reporting

### Output struktura

```
runs/
└── 2026-04-22-163012/
    ├── config.snapshot.json   # bez private_keys, do reproducibility
    ├── tx_log.jsonl           # per-tx record, flushowane live
    ├── summary.csv            # per-slot aggregate
    └── report.md              # ten sam content co stdout, markdown
```

### tx_log.jsonl (one record per line)

```json
{"block_idx":87,"block_num":22513042,"block_hash":"0x...","slot_ms":8650,"sample_idx":7,"wallet":"w1","tx_hash":"0x...","nonce":142,"target_unix_ms":1714400134650,"sent_at_unix_ms":1714400134650,"wake_jitter_us":47,"rpc_rtt_us":12564,"send_result":"ok","inclusion":"target","included_block":22513043}
```

Fields:
- `target_unix_ms`: wall-clock timestamp kiedy powinniśmy wysłać (ms)
- `sent_at_unix_ms`: faktyczny moment wysyłki (`target_unix_ms + wake_jitter_us/1000`, ms)
- `wake_jitter_us`: `t_pre_send - target_instant` w μs, zawsze ≥ 0 (busy-wait gwarantuje)
- `rpc_rtt_us`: czas od send do response w μs
- `send_result`: `"ok"` | `"error:<type>"` (nonce_too_low, underpriced, rpc_timeout, other)
- `inclusion`: `"target"` | `"late"` | `"dropped"` | `null` (still pending przy exit)
- `included_block`: numer bloku w którym tx wylądowała (null jeśli dropped)
- `rpc_rtt_us`: `t_post_send - t_pre_send`

### summary.csv (per slot)

```
slot_ms,sent,included_target,included_late,dropped,errors,pct_target,wake_jitter_p50_us,wake_jitter_p99_us,rpc_rtt_p50_ms,rpc_rtt_p99_ms
8500,100,100,0,0,0,100.00,18,234,11.2,47.3
8550,100,100,0,0,0,100.00,19,261,11.1,52.1
...
```

### report.md + stdout final

```
══════════════════════════════════════════════════════════
 RUN SUMMARY
══════════════════════════════════════════════════════════
 Duration: 00:41:23
 Blocks observed: 1220
 Total tx sent: 6100
 Total tx included (target): 5847 (95.85%)
 Total tx included (late):   203  (3.33%)
 Total tx dropped:           42   (0.69%)
 Total send errors:          8    (0.13%)

 Inclusion cutoff curve (% included in target block):

 slot_ms │ sent │ incT │  %   │ chart
 ────────┼──────┼──────┼──────┼──────────────────────────────
   8500  │  100 │  100 │ 100% │ ██████████████████████████████
  10650  │  100 │   71 │  71% │ █████████████████████░░░░░░░░░
  11500  │  100 │    0 │   0% │ ░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░

 Estimated cutoffs:
   99% inclusion:  ≤ 10500 ms
   95% inclusion:  ≤ 10600 ms
   90% inclusion:  ≤ 10600 ms
   50% inclusion:  ≤ 10700 ms

 Latency breakdown:
   wake_jitter_us  p50=18    p95=67    p99=234
   rpc_rtt_ms      p50=11.4  p95=24.1  p99=58.7

 Errors breakdown:
   nonce_too_low:     3
   replacement_underpriced: 2
   rpc_timeout:       2
   other:             1

 Output:
   Per-tx log:  runs/2026-04-22-163012/tx_log.jsonl
   Summary CSV: runs/2026-04-22-163012/summary.csv
   Report MD:   runs/2026-04-22-163012/report.md
══════════════════════════════════════════════════════════
```

### Progress indicator (during run)

Co 50 bloków:
```
[00:02:14] [87/1220] slot=8650ms incT=95% errs=0 rtt_p50=11ms
```

Wszystko inne tylko gdy WARN/ERROR (np. nonce refetch, WS reconnect).

## 10. Error handling & abort

### Per-tx errors (nie abortują)

- Send error (RPC reject) — zaloguj, zwiększ licznik, kontynuuj. Żadnego retry (time window minął).
- Inclusion timeout — mark Dropped po `lookahead_blocks`.
- WS disconnect → reconnect z backoff (1s, 2s, 4s, 8s, 16s, max 5 prób). Jeśli się uda, kontynuuj. Jeśli nie → abort.

### Abort conditions

- Pre-flight failure (dowolny krok) — exit natychmiast, przed main loop.
- `abort_on_consecutive_failed_blocks` kolejnych bloków z 100% send errors (default 5).
- `insufficient funds` na którymkolwiek wallet-cie — wykryte w pre-flight albo w trakcie runu.
- WS subscription padła i reconnect się nie udał.
- RPC `eth_chainId` zmienił się w trakcie runu (hijack / endpoint fail).
- `Ctrl+C` / SIGTERM → graceful: dokończ in-flight bieżącego bloku, flush jsonl, napisz report.md + summary.csv, exit 0.

### Panic policy

- `anyhow::Result` wszędzie, zero `.unwrap()` poza testami.
- Tokio panic hook flushuje plik logów i jsonl writer przed exit.
- Żadnego recovery po panic — zaufanie do brak panic jest weryfikowane testami.

## 11. Build & deployment

### Cargo targets

```bash
# Dev (natywne, szybka iteracja)
cargo build
cargo build --release

# Linux server x86_64 (statyczna binarka, działa na AL2023/Ubuntu/etc.)
rustup target add x86_64-unknown-linux-musl
cargo build --release --target x86_64-unknown-linux-musl

# Linux server ARM64 (Graviton)
rustup target add aarch64-unknown-linux-musl
cargo build --release --target aarch64-unknown-linux-musl
```

### justfile

```makefile
default:
    just --list

build:
    cargo build --release

build-linux-x64:
    rustup target add x86_64-unknown-linux-musl
    cargo build --release --target x86_64-unknown-linux-musl

build-linux-arm64:
    rustup target add aarch64-unknown-linux-musl
    cargo build --release --target aarch64-unknown-linux-musl

test:
    cargo test

fmt:
    cargo fmt

lint:
    cargo clippy --all-targets -- -D warnings
```

### Deployment recipe (README)

```bash
# local:
just build-linux-x64
scp target/x86_64-unknown-linux-musl/release/tx-cutoff <server>:/opt/hft/
scp config.json <server>:/opt/hft/

# on server:
chmod 600 /opt/hft/config.json
cd /opt/hft
./tx-cutoff --config config.json
# (lub w tmux/screen dla długich runów)
```

### CLI

```
Usage: tx-cutoff [OPTIONS]

Options:
  --config <PATH>        Path to config JSON  [default: config.json]
  --yes                  Skip pre-flight confirmation prompt
  --output-dir <PATH>    Override output dir from config
  --log-level <LEVEL>    trace|debug|info|warn|error  [default: info]
  --help
```

## 12. Dependencies

```toml
[dependencies]
alloy = { version = "0.x", features = ["full", "providers-ws", "signer-local", "contract"] }
tokio = { version = "1", features = ["full", "rt-multi-thread", "macros", "signal"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
reqwest = { version = "0.12", features = ["json", "rustls-tls"], default-features = false }
clap = { version = "4", features = ["derive"] }
anyhow = "1"
thiserror = "1"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
core_affinity = "0.8"
hdrhistogram = "7"  # percentile calculations (wake_jitter, rpc_rtt)

[dev-dependencies]
tokio-test = "0.4"
pretty_assertions = "1"
```

**Dokładne wersje** — zweryfikujemy przez context7 przed implementacją (alloy-rs rozwija się szybko; versioning może się zmienić między teraz a startem implementacji).

## 13. Testing

- TDD per pakiet (CLAUDE.md rule).
- `scheduler_test.rs`: deterministic plan generation (same config → identical plan; edge cases: `end_ms == start_ms`, single wallet, single sample).
- `swap_test.rs`: calldata encoding — testujemy `exactInputSingle` selector + argument encoding z known test vectors z Uniswap docs.
- `config_test.rs`: parsing + validation, wszystkie fail cases.
- `tracker_test.rs`: state machine transitions (Sent → Target / Late / Dropped).
- Integration test z anvil (local reth/foundry dev node) — opcjonalny, poza MVP.

## 14. Known limitations

### Base timestamp granularity

`block.timestamp` w protokole EVM ma rozdzielczość 1 sekundy. Na Ethereum mainnet timestamp aligns z deterministycznym 12-sekundowym slot clock z beacon chain — tool mierzy precyzyjnie.

Na Base (OP-stack sequencer) timestamp zwykle = wall-clock sekund przy produkcji bloku. Przy 2-sekundowym block time wprowadza to ±500 ms noise w "kiedy faktycznie produkowano blok" vs "co mówi timestamp". Dla testów na Base efektywnie obserwowany cutoff będzie uśredniony przez ~20 bloków per slot, więc systematyczny offset znika w średniej, ale jitter pozostaje.

Dla docelowego runu na ETH — ten problem znika.

### Nonce gap cascading

Jeśli tx z slotu `X` nie wchodzą (>= cutoff) i zostają w mempool z sekwencyjnymi nonce'ami, **kolejne** tx z tego samego walleta w następnych blokach będą blocked w mempool aż tamte się sklaruje (drop po timeout RPC, ~1–2 min) albo wejdą. W praktyce znaczy to: gdy w runie przekraczamy cutoff, kilka kolejnych bloków wygląda jak "wszystko dropped" mimo że slot jest poniżej cutoff. Effektem jest contamination danych dla kilku bloków po serii missów.

Mitigacja: tool loguje to jasno w `tx_log.jsonl` (widać dokładnie ile tx jest dropped per blok), analiza off-line może to uwzględnić. Jeśli w praktyce okaże się to problemem — przyszła iteracja może dodać proaktywny replace-by-fee stuck tx.

### Single RPC (brak fallback)

Celowo. Jeśli RPC pada — chcemy abortować i pomyśleć, nie silent-failover na wolniejszy endpoint który zafałszuje pomiar. Jeśli potrzeba resilience w przyszłości — dodamy jako explicit config option (nie domyślnie).

## 15. Future work (out of MVP scope)

- Resume-from-crash (`--resume-from-block-index N`).
- Replace-by-fee dla nonce-gap recovery.
- RPC fallback / multi-RPC comparison mode.
- Private pool / MEV-builder routing (Flashbots RPC itd.).
- Mempool observability (newPendingTransactions subscription, propagation timing).
- Web dashboard z krzywą live.

## 16. Implementation notes

- Wszystkie operacje czasowe: `std::time::Instant` dla monotonic, `std::time::SystemTime` dla unix ms. Nigdy mixed.
- `u256`/`u128` amounts — string w configu, parsed do `U256` (`alloy_primitives`).
- HTTP client: jeden `reqwest::Client` per process, `pool_max_idle_per_host >= wallets.len()`, `tcp_keepalive(Some(Duration::from_secs(60)))`.
- JSON-RPC payload pre-serialized raz, tylko ID się zmienia — można cache'ować.
- Structured logging via `tracing`: each send event ma pola `block_idx`, `wallet`, `slot_ms`, `tx_hash`.

## 17. Approval checklist

- [x] Problem statement
- [x] Goals / non-goals
- [x] Architecture
- [x] Config schema
- [x] Scheduling semantics
- [x] Pre-flight steps + prompt
- [x] Reporting outputs
- [x] Error handling / abort
- [x] Build & deployment
- [x] Known limitations documented
- [ ] **User review of this spec document** ← next step
