# fan-out-bench — design spec

**Status:** draft (rev 2 — post codex review)  
**Data:** 2026-05-14  
**Autor:** Jakub Jaksik + Claude  
**Powiązane crate'y:** `tick-trigger-bench` (reference baseline), `entry-sources` (reuse), `solana-leader-map` (analiza ex-post), `entry-comparator` (źródło wiedzy o YS/SS spójności)

**Changelog:**
- rev 2 (2026-05-14): codex review integrated — dwustopniowa rezolucja (tentative→finalized), nonce pool 50→150, rent calc fixed, ASCII memo encoding, hard assert ix[0], schema rozszerzona o 13 kolumn, finality-tracker component dodany, Nozomi QoS caveat, per-sender Advance-first compat probe w pre-flight

---

## 1. Cel i scope

### 1.1 Cel biznesowy

Zwiększenie include ratio dla flow swapowego DEX-tradera na Solanie (obecnie 20-70% dziennie, średnio 40-60%). Bench jest **Etapem 1** całego researchu — testem na self-transferach przed włączeniem multi-sendera do prawdziwego flow tradera.

### 1.2 Cel benchu

Wyłonić — empirycznie, na realnej sieci — które ze sposobów wysyłki transakcji **wstawiają tx na chain najczęściej i najszybciej**, dla naszego ingressu we Frankfurcie, przy minimalnym koszcie per sender.

### 1.3 Co bench mierzy

Per (slot, tick) trigger:
- Per-sender: M1 (trigger → POST), M2 (sender ack RTT), M3 (POST → on-chain observation), M4/M4' (trigger/POST → include w PoH ticks/hashes)
- Outcome: który sender wygrał race (= którego tx faktycznie się znalazł na chainie po deduplikacji durable nonce)
- Failure mode: czemu pozostałe nie weszły (deduped, send error, timeout)
- Per-leader/per-region/per-stake-bucket landing distribution
- Per-sender ranking po wygranych race'ach + per-sender API responsiveness (M1+M2 niezależne od race outcome)

### 1.4 Czego bench NIE robi

- **Nie testuje real swap flow** — to Etap 2 (durable nonce już wbudowany, więc transition do real flow będzie polegał głównie na zmianie payload tx)
- **Nie testuje slippage / economic quality** — Etap 2
- **Nie testuje competitive context** (czyje inne tx są na tej samej puli) — nieaplikowalne dla self-transferów
- **Nie testuje wszystkich vendorów** — tylko tych z dropek dostępem lub z wykupionym kontem; pełna lista w sekcji 5

### 1.5 Stop criterion

Bench biega **dopóki nie wyzeruje konta** (lub `Ctrl-C`). Stop happens gdy `wallet_balance < min_balance_lamports + (50 × 890_880)` (rezerwa = min_balance + lockowany rent 50 nonce kont, żeby teardown działał).

Default `min_balance_lamports = 1_500_000` (0.0015 SOL) — wystarczy na ostatnią tx z tipem 1M lamp.

---

## 2. Architektura

### 2.1 Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          fan-out-bench                                  │
│                                                                         │
│  ┌──────────┐    ┌──────────┐         ┌────────────────────┐            │
│  │ SS gRPC  │    │ YS gRPC  │ ←─── 2× │ NonceManager       │            │
│  │ (Jito)   │    │ (Helius) │         │  - 50 nonce kont   │            │
│  └────┬─────┘    └────┬─────┘         │  - YS sub na nonces│            │
│       │ entry         │ entry         │  - RR allocator    │            │
│       │ observ        │ observ        │  - state per nonce │            │
│       └──────┬────────┘               └────────┬───────────┘            │
│              ▼                                 │                        │
│       ┌─────────────┐                          ▼                        │
│       │ EntryMerger │                  ┌───────────────┐                │
│       │ dedup by    │                  │   Preparer    │                │
│       │ entry_hash, │                  │ - chunked sch │                │
│       │ min(t)      │                  │ - N variants  │                │
│       └──────┬──────┘                  │   per (s,t)   │                │
│              ▼                         │ - presigned   │                │
│       ┌────────────┐                   └───────┬───────┘                │
│       │  Observer  │                           ▼                        │
│       │ - PoH tick │                   ┌──────────────┐                 │
│       │   counter  │                   │   TxPool     │                 │
│       │ - schedule │ ──────────────────┤ (slot,tick,  │                 │
│       │   match    │                   │  sender_id)→ │                 │
│       │ - sig match│                   │  PreSignedTx │                 │
│       └──────┬─────┘                   └──────┬───────┘                 │
│              │                                │                        │
│              ▼  trigger (sids randomized)     ▼  take(sid,s,t)          │
│       ┌──────────────────────────────────────────┐                     │
│       │           Dispatcher (fan-out)           │                     │
│       │  per-sender clients (reqwest/tonic/      │                     │
│       │   quinn/tungstenite), rate-limit budget  │                     │
│       │  emit SendEvent per attempt              │                     │
│       └──────────────┬───────────────────────────┘                     │
│                      ▼                                                  │
│              ┌───────────────┐                                          │
│              │   Matcher     │  on first observed sig per trigger:      │
│              │ sig + memo    │   - emit LANDED for winner               │
│              │ + trigger_id  │   - emit DEDUPED_BY_NONCE for siblings   │
│              └───────┬───────┘   (no waiting on RPC fallback)           │
│                      │                                                  │
│   ┌──────────────────┼──────────────────┐                              │
│   ▼                  ▼                  ▼                              │
│  Parquet         RPC Fallback        Counters                          │
│  sink            (for TRULY_MISSING)  (per-sender + global)            │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Komponenty (wątki/runtime'y)

| Komponent | Typ | Rola | Pinned core |
|---|---|---|---|
| `ss-grpc` | tokio task | konsumuje Jito shredstream-proxy | tak |
| `ys-grpc` | tokio task | konsumuje Helius Yellowstone gRPC | tak |
| `entry-merger` | std thread | dedup `(slot, entry_hash)`, min(t) | tak |
| `observer` | std thread | PoH tick counter, schedule match, sig match | tak |
| `preparer` | std thread | sign N wariantów per (slot,tick), pool insert | tak |
| `nonce-manager` | tokio task | YS sub na 50 nonce kont, state machine | tak |
| `dispatcher` | tokio runtime (4 threads) | fan-out HTTP/gRPC/QUIC/WS per sender | — |
| `matcher` | std thread | merge SendEvent + EntryObservation, emit FinalRecord | tak |
| `parquet-writer` | std thread | serialize FinalRecord → parquet | tak |
| `rpc-fallback` | std thread | poll `getSignatureStatuses` dla `UNKNOWN_PENDING` | tak |
| `finality-tracker` | std thread | poll `getSignatureStatuses` (commitment=`finalized`) dla TENTATIVE rows, emit finalization updates | tak |
| `tick-sidecar` | std thread | dump TickEvent do JSONL (diagnostyka PoH) | tak |
| `budget-watcher` | std thread | sprawdza wallet balance co N slotów, signals stop | — |
| `clock-monitor` | std thread | periodic NTP-derived clock_offset_ns, emit do parquet metadata | — |

### 2.3 Kanały (bounded, `crossbeam_channel::bounded` lub `tokio::sync::mpsc`)

| Kanał | Producent → Konsument | Capacity |
|---|---|---|
| `ss_entry_rx`, `ys_entry_rx` | source → merger | 65536 |
| `merged_entry_rx` | merger → observer | 65536 |
| `send_q` (tokio mpsc) | observer → dispatcher | 65536 |
| `send_ev` | dispatcher → matcher | 65536 |
| `match_ev` | observer → matcher | 65536 |
| `final_tx` | matcher → parquet | 65536 |
| `tick_event` | observer → sidecar | 65536 |
| `fallback_queue` | matcher → rpc-fallback | bounded 8192 |
| `finality_queue` | matcher → finality-tracker | bounded 32768 |
| `finality_updates` | finality-tracker → parquet (sidecar JSONL) | bounded 8192 |
| `nonce_update` | nonce-manager → preparer | 256 |

Każdy kanał emituje queue_full counter — backpressure surveillance.

### 2.4 Reused komponenty

- `entry_sources::ShredStreamGrpcSource` (z `entry-sources`) — drop-in
- `entry_sources::YellowstoneGrpcSource` (z `entry-sources`) — drop-in
- `entry_sources::EntryObservation` — wspólny format

---

## 3. Decyzje techniczne (finalne)

### 3.1 Sources i merge

- **Dual source:** SS + YS, dedup po `(slot, entry_hash)`, `observed_at = min(ss_at, ys_at)`
- **Implication:** M3 semantyka = "first-seen across sources" (udokumentowane w analyses)
- **Dedup map:** rolling window `slot ∈ [current_slot - 50, current_slot + 5]`, evict starsze

### 3.2 Schedule

- **Cadence:** 1 tx/slot, **1 random tick** per slot (z deterministicznego seed)
- **Chunking:** `chunk_size_slots = 1000` (~67 min), generowany lazy gdy poprzedni chunk się kończy
- **Resumable:** seed + chunk_index + start_slot zapisywane w `run-meta.json`, po crash można dokończyć
- **Open-ended:** brak `num_slots`; stop tylko przez budget lub Ctrl-C

### 3.3 Durable nonce dedup

- **Pool size:** **150 nonce accounts** (default, configurable), lock ~0.217 SOL rent (refundable)
  - Rationale: worst-case in-flight przy degradacji sieci = `deadline_sec × triggers_per_sec` = 90s × 2.5 = 225 jest theoretyczne ceiling; 150 pokrywa typowy duży spike (95% scenariuszy). Codex sugerował 300 dla pełnego safety, my idziemy 150 jako compromise cost/safety, configurable
  - Trade-off: 50 = $11 rent, ryzyko bench stall przy degradation; 150 = $33 rent, znacznie bezpieczniejsze; 300 = $66 rent, full worst-case
- **Allocator:** round-robin per trigger, state machine `ready → in_flight → awaiting_update → ready`
- **Bootstrap:** `getMultipleAccounts(150)` na starcie, cache nonce_blockhash
- **Live updates:** YS gRPC subscription na 150 pubkeyów, parse `NonceState::Initialized.blockhash`
- **Fallback:** dla state `stale` (90s timeout bez update) — polling `getAccountInfo` co 30s
- **Per trigger:** wszystkie N wariantów współdzielą jeden nonce account + jego current nonce_blockhash
- **Setup tooling:** dwa binary `setup-nonces` + `teardown-nonces` (sekcja 4)

### 3.4 Per-trigger workflow (dwustopniowa rezolucja)

Dla każdego (slot, tick) z schedule:

1. Preparer pobiera następny `nonce_id` z RR allocator (mark `in_flight`)
2. Preparer pobiera `nonce_blockhash` z cache
3. Dla **każdego enabled sendera** (N wariantów):
   - Build tx **przez central `tx_builder::build_variant()`** (jedyne miejsce składania tx, hard assert na layout):
     ```
     [0] SystemProgram::AdvanceNonceAccount(nonce_pubkey, authority)  // MUSI być instrukcją 0
     [1] SystemProgram::Transfer(payer → payer, 1 + sender_id)        // self-tx, unique amount
     [2] SystemProgram::Transfer(payer → tip_account_sender_rotating, sender.min_tip)
     [3] ComputeBudgetProgram::SetComputeUnitLimit(200_000)
     [4] ComputeBudgetProgram::SetComputeUnitPrice(priority_fee_microlamports)
     [5] MemoProgram::Memo(memo_bytes = [b'!' + sender_id])            // ASCII printable, UTF-8 safe
     recent_blockhash = nonce_blockhash
     sign with [Arc<Keypair>]
     ```
   - **Wyjątki:** Triton i Harmonic nie mają vendor tip → ix[2] usuwane, ix[4] (priority fee) podbity. Inni sender impl dopisują dodatkowe ix-y (np. AllenHark może wymagać specific layout) — wszystko przez `tx_builder` z hard assertem że ix[0] = AdvanceNonce
4. Insert do TxPool: key `(slot, tick, sender_id)` → PreSignedTx + zapis `prepared_at_ns`, `pool_ready_at_ns`
5. Observer, gdy PoH tick counter osiąga `(slot, tick)`:
   - Bierze z pool wszystkie warianty (N per trigger)
   - **Randomizuje kolejność** (deterministic perm seed z schedule)
   - Emituje N × SendCommand z `TriggerId = (slot, tick, nonce_id)` na `send_q`
   - Tworzy AttemptState w Matcher: `HashMap<(TriggerId, SenderId), AttemptState>` z N rekordami, każdy stan `SENT_PENDING`
6. Dispatcher fan-out: każdy SendCommand → per-sender client → POST/gRPC/QUIC; emit SendEvent (ack/error/timeout) do Matcher → aktualizuje AttemptState

**Rezolucja stage 1 (TENTATIVE, latency metryki):**
7. Matcher: gdy widzi pierwszą sygnaturę z którejkolwiek z N attempts w merged entry stream:
   - Emit tentative outcome: `LANDED_TENTATIVE` dla winnera
   - Emit `DEDUPED_TENTATIVE` dla pozostałych N-1 sibling attempts
   - Zapisz do parquet z `final_status=PENDING`
   - Dorzuć trigger_id do `finality_queue` dla tracker'a
8. Observer detect nonce advance via YS account update → marks `nonce_id` jako `awaiting_update → ready`
9. Po deadline 90s bez ANY landing → emit `UNKNOWN_PENDING` (tentative), wszystkie N attempts do `fallback_queue` → po sprawdzeniu emit `TRULY_MISSING`

**Rezolucja stage 2 (FINALIZED, correctness):**
10. Finality tracker polluje `getSignatureStatuses` (commitment=`finalized`) co 30s dla rows z `final_status=PENDING`:
    - Jeśli winner sig confirmed at `finalized` slot → emit `finality-updates.jsonl`: `{trigger_id, winner_sig, final_status=CONFIRMED, finalization_slot}`
    - Jeśli winner sig nie confirmed po 180s od tentative landing → re-query wszystkie siblings; jeśli któryś sibling ma confirmed status → **reassign winner**, emit `{trigger_id, original_winner_sig, final_status=REORGED_OUT, new_winner_sig, ...}`
    - Jeśli żadna sig confirmed po 180s → emit `final_status=UNCERTAIN_NO_STATUS`
11. Analiza ex-post joinuje parquet z finality-updates.jsonl po `(trigger_id, sender_id)`. Per-sender ranking robimy na **CONFIRMED rows only**; latency metryki możemy na PENDING+CONFIRMED.

### Outcome enum (dwie kolumny zamiast jednej)

- **`tentative_outcome`** (emit w real-time): `LANDED_TENTATIVE | DEDUPED_TENTATIVE | UNKNOWN_PENDING | TRULY_MISSING | SEND_ERROR`
- **`final_status`** (post-finalization update): `PENDING | CONFIRMED | REORGED_OUT | UNCERTAIN_NO_STATUS`
- Analiza standardowa: filter `final_status=CONFIRMED AND tentative_outcome IN (LANDED_TENTATIVE, DEDUPED_TENTATIVE)`

### 3.5 Wysyłka

- **Per-sender minimum tip** zgodnie z vendor docs (sekcja 5). Override przez config per-sender
- **Per-sender protocol:** najszybszy dostępny dla danego sendera (REST/gRPC/QUIC/WS — sekcja 5)
- **Per-region jako osobny sender:** np. Jito-FRA i Jito-AMS to dwa osobne sender_id, każdy z własnym budgetem rate-limitu i własnym memo identifier
- **No retry** w dispatcher — pojedyncza próba, brak fallback
- **No cancellation** wariantów po landingu — wszystkie wysyłki dochodzą do końca naturalnie (response/error/timeout), tylko Matcher rezolwuje status sibling sigs natychmiast (`DEDUPED_BY_NONCE`)
- **Connection keep-alive:** każdy sender impl ma własny background heartbeat (sekcja 5 per-sender)

### 3.6 Atrybucja

- **Memo program** z **1-byte ASCII printable** char jako payload: `memo_byte = b'!' + sender_id` (range 0x21..0x7E, sender_id 0-93)
  - **KRYTYCZNE:** SPL Memo program waliduje UTF-8 i odrzuca tx jeśli bytes nie są valid UTF-8. Raw `sender_id` jako bajt 128-255 (które wpadałyby w invalid UTF-8) **łamie tx PO** wykonaniu AdvanceNonce → nonce advanced + base fee + tip skradzione, ale tx zwraca error. Dlatego encoding ASCII jest wymagany
  - 93 możliwych sender_id = bezpieczna nadwyżka nad obecnymi ~20 (12 vendorów × kilka regionów)
  - Decoder: `sender_id = memo_byte - b'!'`
- Memo widoczne w shred-stream entry → matcher reconstructuje sender per landed signature przez parsing tx body
- **Per-sender tip account** jest **dodatkowym** identyfikatorem (belt-and-suspenders): nawet jeśli Memo parser failuje, tip account address ujawnia sendera
- Triton i Harmonic (brak vendor tip account) → atrybucja TYLKO przez Memo dla nich

### 3.7 Budget management

- Wallet balance check co N slotów (default N=50, ~20s) przez `getBalance` na background thread
- Stop condition: `balance < (nonce_pool_size × rent_lamports) + min_balance_lamports` (rezerwa na teardown nonces + min user-defined). Rent fetched at startup via `getMinimumBalanceForRentExemption(80)` ≈ 1_447_680 lamp per konto.
- Soft stop: brak schedule generation dla nowych chunków, dokończ in-flight, flush parquet, exit clean

---

## 4. Durable nonce — setup procedure

### 4.1 Pre-bench tooling

Dwa osobne binary w `crates/fan-out-bench/src/bin/`:
- `setup-nonces.rs` — utwórz N nonce accounts (default 150, configurable), zapisz keypairs + verify
- `teardown-nonces.rs` — withdraw rent ze wszystkich, refund do wallet

### 4.2 setup-nonces flow

```
1. Load authority keypair (= naszego walleta) z secure path
2. Fetch rent: `rent_lamports = rpc.get_minimum_balance_for_rent_exemption(80)?` (~1_447_680 lamp)
3. Generate N keypairs Keypair::new() (N default 150, configurable via CLI arg)
4. Save keypairs do `nonce-keypairs.json` (chmod 600)
5. Batch tworzenie: ~10 nonce per transakcja (mieści się w 1232 bytes)
   - Per batch:
     for i in batch:
       create_nonce_account(payer=authority, nonce=keypairs[i], authority, rent_lamports)
     sign + send via Helius RPC, wait for confirmation
6. Verify każde:
   getAccountInfo(nonce_pubkey)
   parse NonceState → assert Initialized + authority match
7. Save `nonce-config.json` = lista (id, pubkey, initial_blockhash)
   - id = 0..N-1, używany jako index w RR allocator
   - pubkey użytkowany przy AdvanceNonce ix
   - initial_blockhash dla quick start (przed pierwszym YS update)
```

### 4.3 teardown-nonces flow

```
1. Load nonce-config.json + nonce-keypairs.json + authority keypair
2. Batch (~15 per tx):
   for i in batch:
     withdraw_nonce_account(nonce, authority, recipient=authority_pubkey, rent_lamports)
3. Verify balance returned to authority (~0.217 SOL refund dla N=150)
4. Optional: delete nonce-keypairs.json (już niepotrzebne, mogą być compromised)
```

### 4.4 Koszt

| Operacja | SOL | USD |
|---|---|---|
| Rent (lock 150 × 1_447_680) | 0.217 | $33 |
| Tx fees setup (15 tx × 5000 + sig fees) | ~0.0015 | $0.22 |
| Tx fees teardown (10 tx × 5000) | ~0.0010 | $0.15 |
| **Net cost (rent refunded)** | **~0.003** | **~$0.37** |

### 4.5 Per-tx instruction layout (bench runtime)

**Wszystkie tx budowane przez `tx_builder::build_variant()` — jedyne miejsce składania tx, z hard assert na layout.**

Standardowy wariant (sender ma vendor tip account):

```
TX MESSAGE:
  recent_blockhash: <nonce_blockhash from cache>
  instructions:
    [0] SystemProgram::AdvanceNonceAccount {          // HARD ASSERT: program_id = system, ix index = 0
          nonce_account: <nonce_pubkey>,
          recent_blockhashes_sysvar,
          authority: <our_wallet>,
        }
    [1] SystemProgram::Transfer {                      // self-tx, unique amount
          from: <payer>, to: <payer>,
          lamports: 1 + (sender_id as u64),
        }
    [2] SystemProgram::Transfer {                      // vendor tip
          from: <payer>, to: <SENDER_TIP_ACCOUNT_ROTATING>,
          lamports: <sender.min_tip>,
        }
    [3] ComputeBudgetProgram::SetComputeUnitLimit(200_000)
    [4] ComputeBudgetProgram::SetComputeUnitPrice(priority_fee_microlamports)
    [5] MemoProgram::Memo {                            // ASCII-encoded sender_id
          signers: [],
          memo: [b'!' + sender_id],                    // 1 byte, UTF-8 safe (0x21..0x7E)
        }
  signers: [Arc<Keypair>]                              // payer = authority = wallet, jedna sygnatura
```

Wariant dla Triton/Harmonic (brak vendor tip):

```
  instructions:
    [0] AdvanceNonceAccount (jak wyżej)
    [1] Transfer (self, unique amount)
    [2] SetComputeUnitLimit(200_000)
    [3] SetComputeUnitPrice(priority_fee_microlamports_HIGHER)   // wyższy priority fee zamiast tipa
    [4] Memo
```

**Hard asserts w `tx_builder` (debug + property test):**
- `assert_eq!(msg.instructions[0].program_id, system_program::ID)`
- `assert!(is_advance_nonce_instruction(&msg.instructions[0]))`
- `assert_eq!(msg.recent_blockhash, nonce_blockhash)`
- `assert!(memo_byte >= b'!' && memo_byte <= b'~')` — UTF-8 safe range
- `assert_eq!(msg.account_keys[0], payer.pubkey())` — payer = first signer

**Kluczowe konsekwencje:**
- AdvanceNonce **musi być pierwszą** instrukcją — wymaganie Solany dla durable nonce semantics. Inaczej tx walidowana z `recent_blockhash` traktowanym jako network blockhash → BlockhashNotFound error
- Memo NIE wymaga signera (signers = [])
- ComputeBudget ix mogą być **po** AdvanceNonce (nie modyfikują semantyki nonce)
- Per-sender SDK NIE może dorzucać własnych ix-ów przed AdvanceNonce — central builder + hard assert wyklucza to
- ASCII encoding sender_id: byte = `b'!' + sender_id` (range '!' = 0x21 do '~' = 0x7E, 94 możliwości)

---

## 5. Senders catalog

12 wpisów. Bench konfigurowalny — można włączyć podzbiór. Każdy wiersz = jeden `sender_id`. Region per sender = osobny `sender_id` w finalnej konfiguracji (np. `jito-fra`, `jito-ams`, `0slot-de`, `0slot-ams`).

### 5.1 Tabela podsumowująca

| sender_id | Name | Wire protocol | FRA endpoint | Auth | Min tip (lamp) | RPS free | Tip accounts |
|---|---|---|---|---|---|---|---|
| 1 | helius | HTTP JSON-RPC | `http://fra-sender.helius-rpc.com/fast` | `?api-key=` (opt) | 200_000 dual / 5_000 swqos | 50 | 10 |
| 2 | triton | HTTPS JSON-RPC | `https://<app>.mainnet.rpcpool.com/<token>` | path token | n/a (priority fee only) | ~120 | brak |
| 3 | nozomi-fra | HTTP plaintext v2 | `http://fra2.nozomi.temporal.xyz/api/sendTransaction2?c=<key>` | `?c=<key>` | 1_000_000 | per-key | 17 |
| 4 | nozomi-ams | HTTP plaintext v2 | `http://ams1.nozomi.temporal.xyz/api/sendTransaction2?c=<key>` | `?c=<key>` | 1_000_000 | per-key | 17 |
| 5 | syncro-pub | HTTP JSON-RPC | TBD przez onboarding (path `/public`) | none | 100_000 | 1 | 9 |
| 6 | syncro-priv | HTTP JSON-RPC | TBD przez onboarding (path `/`) | Bearer/X-Api-Key | 1_000_000 | 50 | 9 |
| 7 | astralane-fra | HTTP plaintext `/iris2` | `http://fr.gateway.astralane.io/iris2?api-key=<key>&method=sendTransaction` | `?api-key=` | 10_000 | 5 | 8 |
| 8 | 0slot-de | HTTPS JSON-RPC | `https://de.0slot.trade?api-key=<key>` | `?api-key=` | 100_000 advanced / 1_000_000 trial | 5/20/50 | 21 |
| 9 | 0slot-ams | HTTPS JSON-RPC | `https://ams.0slot.trade?api-key=<key>` | `?api-key=` | 100_000/1_000_000 | 5/20/50 | 21 |
| 10 | allenhark-quic | QUIC custom | `84.32.223.83:4433` | inline `api-key:` first line | 1_000_000 | 100 z key | 11 |
| 11 | allenhark-https | HTTPS REST | `https://fra.relay.allenhark.com/v1/sendTx` | `x-api-key:` | 1_000_000 | 100 z key | 11 |
| 12 | nextblock-quic | QUIC custom | `frankfurt.nextblock.io:11100` | auth stream `Authorization` | 1_000_000 | 1 tx/10s trial | 8 |
| 13 | nextblock-http | HTTPS REST | `https://frankfurt.nextblock.io/api/v2/submit` | `Authorization: <key>` | 1_000_000 | 1 tx/10s trial | 8 |
| 14 | bloxroute-http | HTTP plain | `http://germany.solana.dex.blxrbdn.com/api/v2/submit` | `Authorization: <key>` | 1_000_000 | 60 credits/60s | 4 |
| 15 | blockrazor-grpc | gRPC | `frankfurt.solana-grpc.blockrazor.xyz:80` | metadata `apikey:` | 1_000_000 | 1 | 14 |
| 16 | blockrazor-http | HTTP v2 plaintext | `http://frankfurt.solana.blockrazor.xyz:443/v2/sendTransaction` | `?auth=` | 1_000_000 | 1 | 14 |
| 17 | jito-fra-tx | HTTPS JSON-RPC | `https://frankfurt.mainnet.block-engine.jito.wtf/api/v1/transactions` | none / `x-jito-auth:` opt | 1_000 | 1 per IP per region | 8 |
| 18 | jito-fra-bundle | HTTPS JSON-RPC | `https://frankfurt.mainnet.block-engine.jito.wtf/api/v1/bundles` | none / `x-jito-auth:` opt | 1_000 | 1 per IP per region | 8 |
| 19 | harmonic-fra | gRPC | `https://fra.be.harmonic.gg` (Bearer) | challenge-response auth | n/a (priority fee only) | TBD whitelist | brak |

(numeracja wskazuje że region = osobny sender_id; v1 zaczyna od kilku, reszta dorzucana gdy user załatwi dostęp)

### 5.2 Tip account lists (pełne, do `config.example.json`)

**Helius (10):**
```
4ACfpUFoaSD9bfPdeu6DBt89gB6ENTeHBXCAi87NhDEE
D2L6yPZ2FmmmTKPgzaMKdhu6EWZcTpLy1Vhx8uvZe7NZ
9bnz4RShgq1hAnLnZbP8kbgBg1kEmcJBYQq3gQbmnSta
5VY91ws6B2hMmBFRsXkoAAdsPHBJwRfBht4DXox3xkwn
2nyhqdwKcJZR2vcqCyrYsaPVdAnFoJjiksCXJ7hfEYgD
2q5pghRs6arqVjRvT5gfgWfWcHWmw1ZuCzphgd5KfWGJ
wyvPkWjVZz1M8fHQnMMCDTQDbkManefNNhweYk5WkcF
3KCKozbAaF75qEU33jtzozcJ29yJuaLJTy2jFdzUY8bT
4vieeGHPYPG2MmyPRcYjdiDmmhN3ww7hsFNap8pVN3Ey
4TQLFNWK8AovT1gFvda5jfw2oJeRMKEmw7aH6MGBJ3or
```

**Nozomi (17):**
```
TEMPaMeCRFAS9EKF53Jd6KpHxgL47uWLcpFArU1Fanq
noz3jAjPiHuBPqiSPkkugaJDkJscPuRhYnSpbi8UvC4
noz3str9KXfpKknefHji8L1mPgimezaiUyCHYMDv1GE
noz6uoYCDijhu1V7cutCpwxNiSovEwLdRHPwmgCGDNo
noz9EPNcT7WH6Sou3sr3GGjHQYVkN3DNirpbvDkv9YJ
nozc5yT15LazbLTFVZzoNZCwjh3yUtW86LoUyqsBu4L
nozFrhfnNGoyqwVuwPAW4aaGqempx4PU6g6D9CJMv7Z
nozievPk7HyK1Rqy1MPJwVQ7qQg2QoJGyP71oeDwbsu
noznbgwYnBLDHu8wcQVCEw6kDrXkPdKkydGJGNXGvL7
nozNVWs5N8mgzuD3qigrCG2UoKxZttxzZ85pvAQVrbP
nozpEGbwx4BcGp6pvEdAh1JoC2CQGZdU6HbNP1v2p6P
nozrhjhkCr3zXT3BiT4WCodYCUFeQvcdUkM7MqhKqge
nozrwQtWhEdrA6W8dkbt9gnUaMs52PdAv5byipnadq3
nozUacTVWub3cL4mJmGCYjKZTnE9RbdY5AP46iQgbPJ
nozWCyTPppJjRuw2fpzDhhWbW355fzosWSzrrMYB1Qk
nozWNju6dY353eMkMqURqwQEoM3SFgEKC6psLCSfUne
nozxNBgWohjR75vdspfxR5H9ceC7XXH99xpxhVGt3Bb
```

**Jito (8):**
```
96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5
HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe
Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY
ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49
DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh
ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt
DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL
3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT
```

**Syncro (9):**
```
BPZrtYhdoAhiHWV5EgGLoV7bZFbMamBZurGDq4DmST8v
7D5pdbkV75Sr73M1YFNZwXMed6DenwkdfbJwVWrX6drQ
ELpn2NryEW4B3psG36eSjF45YcGMQpGGuu9J2AgAccbV
FnckAPC9PitnRpGZM2M4WLwb3w9odRLJ7EDRZDngjvd6
3ZnDTgvVfwzqwWoqAUmDkgVtXvXqjmeb5t9zxD5pMbmv
3SLDFcdCzMbcFNguZhzmV4zqEAUvcPoKY13akpE4Tq1p
48tT6LJqrsoFrLpzZSHkjGdGTWtsJ1PvjgWZjh8qF1RK
7GM9fpVMHHcrK4cgzfVdzJvjiy1bSyfwSYzhxvgbfVLg
CBd8GE3ffMJKf3iCCcNNBEifMxH1WpgtTzRnXPxxbjGE
```

**Astralane (8):**
```
astrazznxsGUhWShqgNtAdfrzP2G83DzcWVJDxwV9bF
astra4uejePWneqNaJKuFFA8oonqCE1sqF6b45kDMZm
astra9xWY93QyfG6yM8zwsKsRodscjQ2uU2HKNL5prk
astraRVUuTHjpwEVvNBeQEgwYx9w9CFyfxjYoobCZhL
astraEJ2fEj8Xmy6KLG7B3VfbKfsHXhHrNdCQx7iGJK
astraubkDw81n4LuutzSQ8uzHCv4BhPVhfvTcYv8SKC
astraZW5GLFefxNPAatceHhYjfA1ciq9gvfEg2S47xk
astrawVNP4xDBKT7rAdxrLYiTSTdqtUr63fSMduivXK
```

**0slot (21):**
```
6fQaVhYZA4w3MBSXjJ81Vf6W1EDYeUPXpgVQ6UQyU1Av
4HiwLEP2Bzqj3hM2ENxJuzhcPCdsafwiet3oGkMkuQY4
7toBU3inhmrARGngC7z6SjyP85HgGMmCTEwGNRAcYnEK
8mR3wB1nh4D6J9RUCugxUpc6ya8w38LPxZ3ZjcBhgzws
6SiVU5WEwqfFapRuYCndomztEwDjvS5xgtEof3PLEGm9
TpdxgNJBWZRL8UXF5mrEsyWxDWx9HQexA9P1eTWQ42p
D8f3WkQu6dCF33cZxuAsrKHrGsqGP2yvAHf8mX6RXnwf
GQPFicsy3P3NXxB5piJohoxACqTvWE9fKpLgdsMduoHE
Ey2JEr8hDkgN8qKJGrLf2yFjRhW7rab99HVxwi5rcvJE
4iUgjMT8q2hNZnLuhpqZ1QtiV8deFPy2ajvvjEpKKgsS
3Rz8uD83QsU8wKvZbgWAPvCNDU6Fy8TSZTMcPm3RB6zt
DiTmWENJsHQdawVUUKnUXkconcpW4Jv52TnMWhkncF6t
HRyRhQ86t3H4aAtgvHVpUJmw64BDrb61gRiKcdKUXs5c
7y4whZmw388w1ggjToDLSBLv47drw5SUXcLk6jtmwixd
J9BMEWFbCBEjtQ1fG5Lo9kouX1HfrKQxeUxetwXrifBw
8U1JPQh3mVQ4F5jwRdFTBzvNRQaYFQppHQYoH38DJGSQ
Eb2KpSC8uMt9GmzyAEm5Eb1AAAgTjRaXWFjKyFXHZxF3
FCjUJZ1qozm1e8romw216qyfQMaaWKxWsuySnumVCCNe
ENxTEjSQ1YabmUpXAdCgevnHQ9MHdLv8tzFiuiYJqa13
6rYLG55Q9RpsPGvqdPNJs4z5WTxJVatMB8zV3WJhs5EK
Cix2bHfqPcKcM233mzxbLk14kSggUUiz2A87fJtGivXr
```

**AllenHark (11):**
```
hark1zxc5Rz3K8Kquz79WPWFEgNCFeJnsMJ16f22uNP
harkm2BTWxZuszoNpZnfe84jRbQTg6KGHaQBmWzDGQQ
hark4CwtTnN2y9FaxjcFBAJdJqQrpouu5pgEixfqdEz
harkoJfnM6dxrJydx5eVmDVwAgwC94KbhuxF69UbXwP
hark6hUDUTekc1DGxWdJcuyDZwf6pJdCxd4SXAVtta6
harkoTvFpKSrEQduYrNHXCurARVT19Ud3BnFhVxabos
harkEpXoJv5qVzHaN7HSuUAd6PHjyMcFMcDYBMDJCEQ
harkyXDdZSoJGyCxa24t2QXx1poPyp8YfghbtpzGSzK
harkR2YJ4Dpt4UDJTcBirjnSPBhNpQFcoFkNpCkVqNk
harkRBygM8pHYe4K8eBjfxyEX19oJn3LepFjvNbLbyi
harkYFxB6DuUFNwDLvA5CQ66KpfRvFgUoVypMagNcmd
```

**NextBlock (8):**
```
NextbLoCkVtMGcV47JzewQdvBpLqT9TxQFozQkN98pE
NexTbLoCkWykbLuB1NkjXgFWkX9oAtcoagQegygXXA2
NeXTBLoCKs9F1y5PJS9CKrFNNLU1keHW71rfh7KgA1X
NexTBLockJYZ7QD7p2byrUa6df8ndV2WSd8GkbWqfbb
neXtBLock1LeC67jYd1QdAa32kbVeubsfPNTJC1V5At
nEXTBLockYgngeRmRrjDV31mGSekVPqZoMGhQEZtPVG
NEXTbLoCkB51HpLBLojQfpyVAMorm3zzKg7w9NFdqid
nextBLoCkPMgmG8ZgJtABeScP35qLa2AMCNKntAP7Xc
```

**bloXroute (4):**
```
HWEoBxYs7ssKuudEjzjmpfJVX7Dvi7wescFsVx2L5yoY
95cfoy472fcQHaw4tPGBTKpn6ZQnfEPfBgDQx6gcRmRg
3UQUKjhMKaY2S6bjcQD6yHB7utcZt5bfarRCmctpRtUd
FogxVNs6Mm2w9rnGL1vkARSwJxvLE8mujTv3LK8RnUhF
```

**BlockRazor (14):**
```
Gywj98ophM7GmkDdaWs4isqZnDdFCW7B46TXmKfvyqSm
FjmZZrFvhnqqb9ThCuMVnENaM3JGVuGWNyCAxRJcFpg9
6No2i3aawzHsjtThw81iq1EXPJN6rh8eSJCLaYZfKDTG
A9cWowVAiHe9pJfKAj3TJiN9VpbzMUq6E4kEvf5mUT22
68Pwb4jS7eZATjDfhmTXgRJjCiZmw1L7Huy4HNpnxJ3o
4ABhJh5rZPjv63RBJBuyWzBK3g9gWMUQdTZP2kiW31V9
B2M4NG5eyZp5SBQrSdtemzk5TqVuaWGQnowGaCBt8GyM
5jA59cXMKQqZAVdtopv8q3yyw9SYfiE3vUCbt7p8MfVf
5YktoWygr1Bp9wiS1xtMtUki1PeYuuzuCF98tqwYxf61
295Avbam4qGShBYK7E9H5Ldew4B3WyJGmgmXfiWdeeyV
EDi4rSy2LZgKJX74mbLTFk4mxoTgT6F7HxxzG2HBAFyK
BnGKHAC386n4Qmv9xtpBVbRaUTKixjBe3oagkPFKtoy6
Dd7K2Fp7AtoN8xCghKDRmyqr5U169t48Tw5fEd3wT9mq
AP6qExwrbRgBAVaehg4b5xHENX815sMabtBzUzVB4v8S
```

### 5.3 Per-sender notatki integracyjne

**Helius** — base64 encoding (nie default base58), `skipPreflight=true`, `maxRetries=0`. `?swqos_only=true` zmienia min tip na 5000 lamp ale wyłącza Jito routing. **Decyzja:** dual-route domyślnie (zgodnie z user req), `swqos_only=true` jako opcjonalny drugi sender_id `helius-swqos` jeśli chcemy ranking obu trybów. Keep-alive: `/ping` co 5s.

**Triton** — endpoint per-account, user musi podać URL po onboardzie. Brak vendor tip. Konfiguracja: `priority_fee_microlamports` per-sender (zamiast tip-ix). Cascade z SWQoS bandwidth sales-gated.

**Nozomi** — API v2 plaintext szybsze niż JSON-RPC (no CORS, no JSON parse). User musi otworzyć Discord ticket dla API key. 

**⚠️ Strukturalny problem z dedup:** Nozomi dokumentuje QoS penalty: jeśli <10% landing w ostatnich 30min → priorytet karany. Bench używa durable nonce dedup → Nozomi może wygrać tylko gdy jest pierwszy (~10% triggerów przy 10 włączonych senderach) → wygląda jak klient z 10% landing rate → priority spada → spirala. To **systemowo zaniża** wynik Nozomi w naszym fan-out benchu.

**Decyzja (user-approved):** uruchamiamy Nozomi mimo to. Jeśli pierwsze 2-3h runu pokaże że Nozomi 0% wygranych → robimy **osobny benchmark** (single-sender lub Nozomi-only fan-out po regionach) dla fair Nozomi assessment.

W parquet output zachowujemy per-sender attribution, więc post-hoc możemy filtrować Nozomi i sprawdzać jego standalone performance niezależnie od ranking.

**Syncro** — full URL non-public. **Action item:** kontakt P2P.org dla onboard. W v1 bench możemy uruchomić bez Syncro, dorzucić gdy URL znany. Public path `/public` 1 TPS — testowalne ale słabe rate.

**Astralane** — pricing sales-gated. **Action item:** kontakt sales/Discord. `/iris2` endpoint plaintext, faster path. W v1 bench bez Astralane.

**0slot** — wybierz region per sender_id. Min tip zależy od tieru (trial 1M, advanced 100k). Konfiguracja per sender_id z explicit tip_lamports override.

**AllenHark** — dwa wire formaty: QUIC (najszybszy, deklarowane 0.1ms) + HTTPS REST (15-25ms). **Decyzja:** v1 implementuje **QUIC** jako preferowany. HTTPS REST tylko jako fallback gdy QUIC pada przy starcie. ALT tx są rejected (nie używamy ALT, OK). Keep-alive: QUIC ping co 3s, HTTPS `/keepalive` co 5-10s.

**NextBlock** — trial **1 tx/10s** = bardzo wolne. Przy 1 tx/slot trigger → 95% dropów rate-limit. Vodlo jeśli user nie wykupi, ale ciekawy ranking dla zwycięskich 5%. QUIC fire-and-forget bez response — uważać, M1/M2 niedostępne dla QUIC path. HTTPS daje response z `signature + uuid`.

**bloXroute** — `60 credits/60s` ≈ 1 TPS na Introductory. Podobny problem co NextBlock. Body shape z `useStakedRPCs:true`, `submitProtection:"SP_LOW"` rekomendowane dla low-latency. Keep-alive 60s.

**BlockRazor** — `mode=fast` dla raw latency benchu. v2 plaintext HTTP najlżejsze. gRPC dla większego throughput jeśli kiedyś. Default 1 TPS — bottleneck.

**Jito** — single tx via `/api/v1/transactions` ORAZ bundle via `/api/v1/bundles` jako dwa różne sender_id (`jito-fra-tx`, `jito-fra-bundle`). Single tx: `bundleOnly=true` nie używamy (zmienia semantykę). Encoding `base64` explicit (default base58 = slow). Default 1 rps/IP/region — najgorszy bottleneck, ale per-region rate split: FRA + AMS + Tokyo dają 3 osobne sender_id z osobnym 1 rps limitem. Rozważyć też 4 regiony.

**Harmonic** — closed beta, **wymagana whitelist**. Jeśli user nie ma, sender_id wyłączony w configu. Jeśli ma: gRPC bundle z proto Jito-compat ale różna role enum (SEARCHER=3, nie 1). Brak vendor tip; "tip" = priority fee. Implementacja gRPC z challenge-response auth.

---

## 6. Parquet output schema

Single parquet file per run: `runs/<run_id>/tx-events.parquet` + side file `runs/<run_id>/finality-updates.jsonl`.

Każdy rekord w parquet = pojedynczy (trigger, sender_id) attempt. Schema rozbudowana wg uwag codex review (kolumny oznaczone ★ to dodatki post-review).

### 6.1 Schema parquet (tx-events.parquet)

**Identyfikacja triggera/wariantu:**
| Kolumna | Type | Description |
|---|---|---|
| `trigger_slot` | u64 | scheduled slot |
| `trigger_tick` | u8 | scheduled tick (1-64) |
| `trigger_id` | bytes(16) | hash(slot, tick, nonce_id) — unique key |
| `nonce_account_id` | u16 | 0..pool_size-1, RR allocator |
| `nonce_blockhash_used` | bytes(32) | nonce blockhash przy signingu |
| `sender_id` | u8 | 0-93 (ASCII-encoded w memo) |
| `sender_name` | string | debug-friendly nazwa (np. "jito-fra-tx") |
| `tx_signature` | bytes(64) | Ed25519 sig |
| ★ `tx_message_hash` | bytes(32) | sha256(serialized message) — dedup verification |

**Sender config snapshot (per attempt — by widzieć condition tej próby):**
| Kolumna | Type | Description |
|---|---|---|
| ★ `endpoint_url` | string | full URL użyty przy tej próbie |
| ★ `protocol` | enum | HTTP_JSONRPC / HTTP_PLAIN / GRPC / QUIC / WS |
| ★ `auth_tier` | enum nullable | FREE / TRIAL / PAID / ENTERPRISE / NONE |
| `tip_account_used` | bytes(32) nullable | który tip account z listy sender'a (null jeśli sender bez tip account) |
| `tip_lamports` | u64 | actual tip amount |
| `priority_fee_microlamports` | u64 | per sender |
| `compute_unit_limit` | u32 | default 200_000 |

**Timestamps (anchor-relative monotonic ns):**
| Kolumna | Type | Description |
|---|---|---|
| ★ `prepared_at_ns` | u64 | preparer signed tx |
| ★ `pool_ready_at_ns` | u64 | tx wstawione do pool |
| `trigger_observed_at_ns` | u64 | observer's view of (slot, tick) reach |
| `send_at_ns` | u64 | dispatcher POST/gRPC/QUIC start |
| `send_ack_at_ns` | u64 nullable | sender response, null jeśli error/timeout |
| `send_order_in_trigger` | u8 | 0..(N-1), kolejność dispatcha (randomized) |
| ★ `host_clock_offset_ns` | i64 nullable | NTP-derived offset at time of send (drift monitoring) |

**Send outcome (transport layer):**
| Kolumna | Type | Description |
|---|---|---|
| `send_error` | string nullable | error message jeśli sender odmówił |
| ★ `rpc_err_code` | i32 nullable | JSON-RPC error code |
| ★ `rpc_err_message` | string nullable | JSON-RPC error.message |
| ★ `provider_request_id` | string nullable | AllenHark request_id / Jito bundle_id / NextBlock uuid / inne |
| ★ `http_status` | u16 nullable | HTTP status code (REST), null dla gRPC/QUIC |
| ★ `rate_limit_state` | enum | OK / THROTTLED_429 / CIRCUIT_OPEN / TIMEOUT |

**On-chain observation (tentative, real-time):**
| Kolumna | Type | Description |
|---|---|---|
| `observed_slot` | u64 nullable | slot landing |
| `observed_entry_index` | u32 nullable | |
| `observed_tick_in_slot` | u8 nullable | |
| `observed_cumulative_hashes_in_slot` | u64 nullable | hash-precise |
| ★ `ss_observed_at_ns` | u64 nullable | first-seen w SS gRPC stream |
| ★ `ys_observed_at_ns` | u64 nullable | first-seen w YS gRPC stream |
| `observed_at_ns` | u64 nullable | min(ss, ys) — first-seen across sources |
| `observed_source` | enum nullable | SS / YS / BOTH (która źródło zobaczyło pierwsza) |
| ★ `commitment_at_resolution` | enum nullable | PROCESSED (default dla SS/YS) |

**Outcome (DWIE kolumny):**
| Kolumna | Type | Description |
|---|---|---|
| `tentative_outcome` | enum | LANDED_TENTATIVE / DEDUPED_TENTATIVE / UNKNOWN_PENDING / TRULY_MISSING / SEND_ERROR |
| ★ `final_status` | enum | PENDING (default) — updated post-finality via side JSONL join |
| `siblings_resolved_at_ns` | u64 nullable | gdy sibling pierwszy → kiedy ten variant został marked DEDUPED_TENTATIVE |

**Leader context (z LeaderCache, post-process backfill OK):**
| Kolumna | Type | Description |
|---|---|---|
| `leader_pubkey` | bytes(32) nullable | leader for observed slot |
| `leader_region_cc` | string nullable | country code |
| `leader_dc_label` | string nullable | data center label |
| `leader_continent` | string nullable | |
| `leader_stake_lamports` | u64 nullable | |
| `validator_client` | string nullable | jito-solana / agave / firedancer (gdzie wiemy) |

**Computed deltas:**
| Kolumna | Type | Description |
|---|---|---|
| `tick_delta` | i32 nullable | M4 ticks (PoH) |
| `hash_delta` | i64 nullable | M4 hashes |
| `slot_delta` | i32 nullable | observed_slot - trigger_slot |
| `leader_changed` | bool | observed slot ma innego lidera niż trigger slot? |
| `wall_trigger_to_send_ns` | i64 nullable | M1 |
| `wall_send_rtt_ns` | i64 nullable | M2 |
| `wall_send_to_observed_ns` | i64 nullable | M3 (min(ss,ys)-based) |
| ★ `wall_send_to_ss_observed_ns` | i64 nullable | M3 SS only |
| ★ `wall_send_to_ys_observed_ns` | i64 nullable | M3 YS only |

**Nonce state context:**
| Kolumna | Type | Description |
|---|---|---|
| ★ `nonce_update_observed_at_ns` | u64 nullable | kiedy zobaczyliśmy advance dla tego nonce account |
| ★ `nonce_update_source` | enum nullable | YS / RPC_POLL (skąd dowiedzieliśmy się o advance) |
| ★ `nonce_advanced_to_slot` | u64 nullable | slot w którym advance wykonano (z winning tx) |

**Run metadata:**
| Kolumna | Type | Description |
|---|---|---|
| `run_id` | string | run timestamp |
| `chunk_index` | u32 | który chunk schedule |

### 6.2 Schema finality-updates.jsonl (side file)

Emitted by finality-tracker. JSON Lines, każda linia = jedna aktualizacja:
```json
{
  "trigger_id": "<hex16>",
  "sender_id": 0,
  "tx_signature": "<base58>",
  "final_status": "CONFIRMED",          // CONFIRMED | REORGED_OUT | UNCERTAIN_NO_STATUS
  "finalization_slot": 419266500,
  "finalization_checked_at_ns": 12345,
  "actual_fee_lamports": 5000,          // z getTransaction post-finality
  "reassigned_winner_sig": null         // non-null jeśli REORGED_OUT i sibling przejął
}
```

Analiza ex-post: `LEFT JOIN tx-events.parquet ON (trigger_id, sender_id)` z finality-updates.jsonl, fill default PENDING.

### 6.3 Side artifacts

- `tick-events.jsonl` — diagnostyka PoH (per merged entry)
- `rpc-fallback-annotations.jsonl` — decyzje fallback (UNKNOWN_PENDING → TRULY_MISSING)
- `nonce-events.jsonl` — state transitions nonces (debug)
- `final-counters.json` — total counters snapshot at exit
- `counters-<timestamp>.json` — periodic snapshots (5min interval) dla long runs
- `run-meta.json` — config snapshot, schedule seed, host, start/end balance, exit_reason
- `clock-drift.jsonl` — periodic NTP offset measurements

Schema design optimized for:
- Per-sender ranking: filter po `outcome=LANDED` group by `sender_id`
- Per-leader heatmap: group by `leader_pubkey + leader_stake_lamports bucket`
- Failure mode breakdown: group by `outcome` per sender
- API responsiveness analysis: percentiles `wall_send_rtt_ns` per sender (niezależne od outcome)

Side output:
- `tick-events.jsonl` — diagnostyka PoH (per merged entry)
- `rpc-fallback-annotations.jsonl` — fallback decisions
- `final-counters.json` — total counters snapshot
- `nonce-events.jsonl` — state transitions nonces (debug)
- `run-meta.json` — config snapshot, schedule seed, host, start_balance

---

## 7. Lifecycle / state machines

### 7.1 Nonce account state

```
                ┌─────────┐
                │ INITIAL │
                │ (boot)  │
                └────┬────┘
                     │ getMultipleAccounts → blockhash cached
                     ▼
                ┌─────────┐
       ┌────────┤  READY  │◄────────┐
       │        └────┬────┘         │
       │             │ allocator.   │
       │             │ take()       │
       │             ▼              │
       │        ┌──────────┐        │
       │        │IN_FLIGHT │        │
       │        │(N vars)  │        │
       │        └────┬─────┘        │
       │             │              │
       │  ┌──────────┴───────────┐  │
       │  │                      │  │
       │  ▼ ANY landed           ▼ deadline 90s, no landing
       │ ┌─────────────┐    ┌─────────┐
       │ │AWAITING_UPDATE│   │  STALE  │
       │ │(YS event)  │     │(fallback│
       │ └────┬────────┘    │ poll)   │
       │      │              └────┬────┘
       │      │ YS account upd    │ fallback resolved
       │      │ → new blockhash   │ OR poll error → reset
       │      ▼                    │
       └──────┴────────────────────┘
```

Timeouts:
- `IN_FLIGHT → STALE`: 90s bez landingu I bez YS update
- `AWAITING_UPDATE → STALE`: 5s bez YS update po wykryciu landingu
- `STALE → READY`: fallback `getAccountInfo` zwróci current blockhash → re-cache

### 7.2 Trigger lifecycle (Matcher state per attempt)

Każdy `(trigger_id, sender_id)` ma własny `AttemptState` w Matcher — single-owner pattern, eliminuje race conditions:

```
enum AttemptState {
  SENT_PENDING { send_at, sig },                          // dispatched, awaiting ack and/or observation
  SENT_ACKED { send_at, send_ack_at, sig },               // sender returned, awaiting observation
  SEND_FAILED { send_at, error },                         // terminal: sender odmówił/timeout
  OBSERVED_TENTATIVE { ..., observed_at, observed_source,  // tentative LANDED lub DEDUPED
                       outcome: LANDED | DEDUPED },
  UNKNOWN_PENDING { ... },                                // deadline 90s bez observation
  TRULY_MISSING { ... },                                  // fallback potwierdził brak
}
```

Pipeline:
```
schedule.next(slot, tick) →
  nonce_id = allocator.take()
  nonce_blockhash = cache[nonce_id]
  for sender in enabled_senders:
    tx = tx_builder::build_variant(sender, nonce_blockhash, sender_id)  // central, asserted
    pool.insert((slot, tick, sender.id), tx)

observer.on_tick(slot, tick) →
  variants = pool.take_all_for(slot, tick)
  if variants.empty: counter.pool_empty++
  perm = deterministic_perm(schedule_seed XOR (slot << 8) XOR tick)
  for variant in variants.shuffle_by(perm):
    matcher.register_attempt(trigger_id, sender_id, sig)  // creates AttemptState::SENT_PENDING
    send_q.send(SendCommand { variant, trigger_id })
    
dispatcher.fan_out(cmd) →
  client = sender_clients[cmd.sender_id]
  result = client.send(cmd.tx).await   // timeout per sender config
  matcher.on_send_event(SendEvent {
    trigger_id, sender_id, ack_at_ns | error, http_status, rpc_err, provider_request_id
  })

matcher.on_send_event(ev) →
  state = attempts.get_mut((ev.trigger_id, ev.sender_id))
  match (state, ev.result) {
    (SENT_PENDING, Ok(ack)) → state = SENT_ACKED
    (SENT_PENDING, Err(e))  → state = SEND_FAILED; emit_send_error_row()
    (OBSERVED_TENTATIVE { outcome: DEDUPED }, _) → record ack/error but keep outcome
    (OBSERVED_TENTATIVE { outcome: LANDED }, _)  → record ack/error
    _ → log unexpected, count race
  }

matcher.on_observed_sig(sig, observed_at, source) →
  // sig may be a "winner" or a "sibling"
  attempt = attempts.find_by_sig(sig)
  if attempt is None: return  // not ours
  if attempt.outcome == LANDED: return  // duplicate observation (SS+YS both saw it), just update ss/ys timestamps
  trigger_id = attempt.trigger_id
  // Mark this one LANDED
  attempt.transition_to(OBSERVED_TENTATIVE { outcome: LANDED, ... })
  // Mark siblings DEDUPED_TENTATIVE
  for sib in attempts.find_with_trigger(trigger_id) where sib != attempt:
    if sib.state in [SENT_PENDING, SENT_ACKED]:
      sib.transition_to(OBSERVED_TENTATIVE { outcome: DEDUPED, ... })
  // Queue for finality check
  finality_queue.push(trigger_id)
  // Emit rows to parquet writer
  emit_row(attempt)
  for sib: emit_row(sib)

matcher.on_deadline(trigger_id, 90s after dispatch) →
  for attempt in attempts.find_with_trigger(trigger_id):
    if attempt.state in [SENT_PENDING, SENT_ACKED]:
      attempt.transition_to(UNKNOWN_PENDING)
      fallback_queue.push(attempt.sig)
      emit_row(attempt)

rpc_fallback.poll(sig) → status from getSignatureStatuses →
  if status is None and 5min elapsed → emit final TRULY_MISSING row update
  if status is Confirmed → late landing, emit corrected LANDED row update

finality_tracker.poll(trigger_id) → getSignatureStatuses(all_sigs, commitment=finalized) →
  determine real winner / determine if winner reorged → emit finality-updates.jsonl record
```

### 7.3 Send order randomization

Per trigger: `perm = DeterministicShuffle(seed = schedule.seed ^ (slot << 8) ^ tick)` over enabled sender_ids. Replayable for ex-post analysis.

### 7.4 Finality tracker workflow

- Polluje raz na 30s wszystkie `trigger_id` z `finality_queue`
- Batch `getSignatureStatuses(sigs[100])` (max 100 sigs/call per Solana RPC limit)
- Per response:
  - `Some(confirmation_status == Finalized)` → emit CONFIRMED dla tej sig
  - `Some(confirmation_status == Processed|Confirmed)` → keep w queue, retry next round
  - `None` po 5 min od tentative landing → emit `UNCERTAIN_NO_STATUS`
- Per trigger_id: gdy KAŻDA sig ma ostateczny final_status → remove from queue
- Edge case (reorg): jeśli tentative winner has `None` finalized status PO tym jak sibling has `Finalized` → emit REORGED_OUT dla winnera, emit CONFIRMED dla sibling z `reassigned_winner_sig`
- Throttling: max 5 batch calls/sec żeby nie zabić RPC; uses dedicated RPC endpoint (Helius main)

---

## 8. Stability & operations

### 8.1 24h+ run requirements

| Risk | Mitigation |
|---|---|
| Memory leak w pending_sigs (sigs nie zresolved) | Periodic sweep co 30s: usuń sig starszą niż 5×deadline |
| Memory leak w dedup map (EntryMerger) | Rolling window, evict slot < current - 50 |
| Tokio queue overflow | Bounded queues + queue_full counters; jeśli przepełnione → log warn, drop tx, count drop reason |
| Blockhash refresh failure (sieć RPC pada) | Fallback RPC list (primary + 2 backup) w `nonce_manager` poller |
| Sender endpoint pada | Per-sender circuit breaker: 5 consecutive errors → 60s cooldown → retry; per-sender counter `circuit_open` |
| Wallet drained nieoczekiwanie | Budget watcher co 50 slotów, soft shutdown |
| Disk full (parquet rośnie) | Row group rotation co 32k records ≈ <100 MB; rotacja plików co 1h gdy run trwa wiele h |
| Authority keypair leak | chmod 600, secure path; refundable ryzyko 0.045 SOL |
| Crash mid-run (panic, OOM) | Atomic write `run-meta.json` z chunk_index; restart resumuje od ostatniego completed chunk |
| Geyser stream rozłącza się | Auto-reconnect z exponential backoff w `YS gRPC` task; bootstrap re-fetch nonce cache |
| Clock drift między source'ami | Anchor = monotonic Instant na starcie; wszystkie timestamps relative |

### 8.2 Logging

`tracing` (już używane w workspace). Level:
- ERROR — bench-killing problems
- WARN — recoverable degradation (sender down, queue full, retry)
- INFO — phase transitions (chunk start/end, balance check, shutdown reason)
- DEBUG — per-trigger trace (możliwy gdy chcemy debugować dedup)
- TRACE — entry stream contents

Counters w `final-counters.json` na końcu runu + periodic snapshot co 5 min do `counters-<timestamp>.json` dla long runs.

### 8.3 Shutdown sequence

```
1. Budget watcher / Ctrl-C / max-duration → stop.store(true)
2. Schedule generator zatrzymuje generowanie nowych chunków
3. Drain in-flight: czekamy ≤ 120s na wszystkie pending_sigs resolved
4. Drop all queue senders (signal to consumer threads)
5. Consumer threads exit, joined w order: ss, ys, merger, observer, preparer, dispatcher, matcher, parquet, rpc_fallback, nonce_manager, budget_watcher
6. Każdy join ma timeout 30s — jeśli przekroczone, log warn ale nie blokujemy całości
7. Final counters snapshot, parquet final flush, run-meta.json updated z exit_reason
```

---

## 9. Plan testowania

### 9.1 Unit tests

- `schedule::test_deterministic` — seed determinizm
- `schedule::test_chunking` — generacja chunków
- `tx_builder::test_advance_nonce_first` — instruction[0] = AdvanceNonce dla każdego sender variant
- `tx_builder::test_memo_ascii_safe` — memo byte zawsze w range 0x21..0x7E
- `tx_builder::test_no_vendor_tip_for_triton_harmonic` — wariant nie ma tip ix dla tych senderów
- `tx_builder::test_message_hash_unique` — N wariantów ma N różnych tx_message_hash
- `preparer::test_variants_unique_sigs` — N wariantów ma N różnych sygnatur (różne memo + amount)
- `nonce_manager::test_state_transitions` — fixture YS account updates
- `nonce_manager::test_rr_allocator_fairness` — po 1000 trigger każdy nonce użyty +/- 5%
- `matcher::test_dedup_resolution` — gdy 1 sig landed, N-1 siblings → DEDUPED_TENTATIVE
- `matcher::test_race_send_before_observed` — SendEvent dla siblinga PO już-observed → outcome zachowany
- `matcher::test_race_observed_before_send` — Observation przed SendEvent → state update correct
- `matcher::test_truly_missing` — gdy 0 sigs landed, deadline → fallback → TRULY_MISSING
- `matcher::test_finalization_confirms` — finality update CONFIRMED → side jsonl emitted
- `matcher::test_finalization_reorg` — winner reorgowany, sibling confirmed → REORGED_OUT + reassign
- `dispatcher::test_send_order_random` — perm deterministic per (slot, tick)
- `sources/merger::test_dedup_by_entry_hash` — entry from SS i YS o tym samym hash → 1 emit, oba timestamps zachowane
- `senders::test_memo_decoder` — byte → sender_id round-trip dla 0..93

### 9.2 Integration tests

- `test_e2e_local_mock` — mock senders + mock entry stream + mock RPC; verify pełen pipeline parquet output
- `test_nonce_lifecycle` — fake YS updates triggering state transitions
- `test_budget_shutdown` — mock balance < threshold → stop signal propagated

### 9.3 Smoke tests (real chain, minimal)

Bench config dla 2-min smoke:
- 2 enabled senderów (helius + jito-fra-tx) — minimal blast radius
- 10 nonce kont (zamiast 150)
- 30 slotów schedule
- run-meta validation + parquet schema sanity
- Verify CONFIRMED rows pojawia się w finality-updates.jsonl po ~30s

Łatwo wykrywa: signing bug, gRPC config bug, parquet write fail, dedup not working (jeśli widzimy 2 LANDED_TENTATIVE z tego samego triggerId → coś źle).

### 9.4 Per-sender Advance-first compatibility probe (pre-flight)

Codex flag: niektórzy sender'zy (NextBlock, BlockRazor) dokumentują "tip should be first" — może być heurystyka dla detection. AdvanceNonce w naszych tx musi być pierwsza → niekoniecznie kompatybilne z każdym senderem.

**Probe procedure (osobny binary `bin/probe-senders.rs`):**
1. Per włączony sender_id, wyślij 5 tx (single, nie w fan-out): self-transfer + tip + memo + AdvanceNonce na ix[0]
2. Czekaj 60s na observation
3. Jeśli ≥3/5 wylądowało normalnie → sender COMPATIBLE, mark `tip_after_nonce_ok=true` w config
4. Jeśli ≤1/5 wylądowało → sender INCOMPATIBLE, **wyłącz w config** lub flag jako needs manual workaround

Probe musi być uruchomiony przed pierwszym pełnym runem i przy każdej zmianie sender impl.

### 9.4 Sanity checks pierwszego prawdziwego runu

Po pierwszym 1h runie sprawdzamy:
- `outcome == LANDED` count vs `outcome == DEDUPED_BY_NONCE` count: per trigger powinno być **dokładnie 1 LANDED + (N-1) DEDUPED** (gdy nikt nie failuje rate limit)
- Sender attribution: `sender_id` w Memo zgadza się z tip_account_used i sender's tip account list
- `nonce_account_id` rotacja: każdy z 50 użyty mniej więcej tyle samo razy (RR)
- `wall_send_rtt_ns` rozkłady per sender: outliers, p99 detection (broken sender?)

---

## 10. Pre-flight checklist

Przed pierwszym prawdziwym runem:

**Konta i klucze:**
- [ ] Wallet z ~2 SOL balance (1.5 SOL budget + 0.22 SOL nonce rent dla 150 kont + 0.3 SOL buffer)
- [ ] Keypair w `~/.config/solana/dex-bench.json`, chmod 600
- [ ] Nonce keypairs generated → `nonce-keypairs.json`, chmod 600
- [ ] `setup-nonces --count 150` run → 150 kont initialized, verified
- [ ] `nonce-config.json` zapisane

**Sendery (per włączony):**
- [ ] Helius: working (mamy), `?api-key=` opcjonalny dla custom TPS
- [ ] Jito: drop-in, bez signup, test ping
- [ ] AllenHark: drop-in HTTPS, opcjonalnie API key
- [ ] 0slot: sign in + API key, trial week aktywny
- [ ] Nozomi: Discord ticket → API key
- [ ] bloXroute: portal signup → auth header
- [ ] NextBlock: dashboard → API key
- [ ] BlockRazor: register → auth token
- [ ] Triton: $125 deposit lub trial credits
- [ ] Syncro: P2P onboarding → full URL i auth
- [ ] Astralane: sales contact (opcjonalne dla v1)
- [ ] Harmonic: whitelist Airtable (opcjonalne dla v1)

**Infrastruktura:**
- [ ] aws-fra-* machine z Helius RPC + dedicated YS gRPC dostępne
- [ ] jito-shredstream-proxy running na 127.0.0.1:9999
- [ ] Disk space: ~5 GB free dla 24h runu
- [ ] Network: bez konfliktów z innymi benchami na tej samej maszynie

**Config:**
- [ ] `config.json` z listą enabled senders + per-sender tip + endpoint URLs
- [ ] `min_balance_lamports = 1_500_000` w configu
- [ ] `chunk_size_slots = 1000`
- [ ] Budget watcher enabled, interval 50 slotów

**Compatibility probe:**
- [ ] `probe-senders` run dla każdego enabled sendera → ≥3/5 landing rate confirmed
- [ ] Disabled w config jakie sendery nie przeszły probe

**Smoke run (2-3 min):**
- [ ] Wykonano smoke test z 2 senderów + 10 nonces + 30 slotów
- [ ] Parquet schema valid (sprawdzone pyarrow.read_schema)
- [ ] Dedup confirmed (LANDED_TENTATIVE + DEDUPED_TENTATIVE counts make sense per trigger — dokładnie 1 + (N-1))
- [ ] Finality updates emitted po ~30s, CONFIRMED rows widoczne w finality-updates.jsonl
- [ ] Counters bez red flags (zero send_http_error, zero pool_empty)
- [ ] Memo decoder weryfikuje sender_id z landed tx body matches expected sender

**Analytics readiness:**
- [ ] `~/solana-analysis/fan-out/` directory z analysis.py adapted from `~/solana-analysis/tick-trigger/analysis.py`
- [ ] LeaderCache snapshot dla aktualnej epoki (z `solana-leader-map fetch`)

---

## 11. Otwarte luki / przyszłe iteracje

**v1 (this spec):**
- Etap 1: self-transfer benchmark z durable nonce dedup
- Stałe payload (self-tx + tip + memo + AdvanceNonce)
- Per-sender minimum tip, 1 tx/slot

**v2 (nieobjęte tym spec'em):**
- Etap 2: integracja z real swap flow w dex-traderze
- Slippage / economic quality measurement
- Competitive context (inne tx na tej samej puli)
- Dynamic tip bidding (per leader stake bucket)
- Per-sender SLA monitoring → auto-drop sender z benchu jeśli outage

**Nieznane wymagające researchu w v1:**
- Syncro full URL — wymaga onboardingu, blocker
- Astralane API key + pricing — sales-gated, blocker
- Harmonic whitelist — beta, blocker
- Triton FRA endpoint specific — po onboardingu
- Czy fork_tick_overflow z tick-trigger-bench powtórzy się w merged stream (SS+YS)? Hipoteza: mniej, ale do potwierdzenia empirycznie
- Realny rozkład winnera per sender przy nonce dedup — może być counterintuitive (np. sender z najtańszym tipem ale najlepszą drogą może dominować)

**Decyzje odłożone:**
- Anulowanie in-flight wysyłek (v1 NIE): zostawiamy fire-and-forget; tylko Matcher rozwiązuje sibling statuses natychmiast
- Multi-region per sender automatyczne — w v1 user explicit konfiguruje który region jako który sender_id
- Confirmation tracking po stronie senderów które to oferują (np. AllenHark `request_id`) — provider_request_id ZAPISUJEMY w schemie ale nie używamy do confirmation flow w v1

**Codex review findings — przyjęte vs odłożone:**

Przyjęte (zaimplementowane w tym revu spec'u):
- Dwustopniowa rezolucja outcome (tentative + finalized) — §3.4, §7.2, §6
- Pool size 50 → 150 — §3.3 (codex sugerował 300, my idziemy 150 z opcją scale up)
- Rent calc poprawiony — §4 (890_880 → 1_447_680)
- ASCII memo encoding — §3.6, §4.5 (codex flag UTF-8 validation)
- Hard assert ix[0]=AdvanceNonce w tx_builder — §4.5
- Schema +13 kolumn — §6
- Matcher single-owner state machine — §7.2 (eliminate race)
- Per-sender Advance-first probe pre-flight — §9.4
- Nozomi QoS caveat — §5.3

Odłożone (z uzasadnieniem):
- Codex sugerował pool 300 (worst-case finality 90s+); zostajemy 150 jako balance cost/safety, monitorujemy `nonce_stalls` counter, scale w v2 jeśli widzimy problem
- Codex flag "min(SS, YS) is metric, not proof" — zostawiamy min() jako derived kolumnę dla latency analysis, ale dla **correctness analysis** używamy `final_status=CONFIRMED` filtru. To rozwiązuje problem bez wyrzucania min() w ogóle
- Codex flag "Helius dual-route min tip 200_000 lamports vs swqos 5000" — config per-sender pozwala explicit override; default w impl = dual-route min

---

## 12. Struktura katalogów crate'a

```
crates/fan-out-bench/
├── Cargo.toml
├── README.md
├── config.example.json
├── nonce-keypairs.example.json
├── src/
│   ├── main.rs                    — CLI: run | smoke | dry-run
│   ├── lib.rs
│   ├── config.rs
│   ├── schedule.rs
│   ├── preparer.rs
│   ├── pool.rs
│   ├── observer.rs
│   ├── matcher.rs                 — single-owner AttemptState per (trigger_id, sender_id)
│   ├── tx_builder.rs              — central tx composition, hard asserts on layout
│   ├── dispatcher.rs
│   ├── finality_tracker.rs        — getSignatureStatuses(finalized) polling, jsonl emit
│   ├── clock_monitor.rs           — periodic NTP offset measurement
│   ├── runtime.rs
│   ├── budget_watcher.rs
│   ├── nonce/
│   │   ├── mod.rs
│   │   ├── manager.rs             — RR allocator + state machine
│   │   ├── account.rs             — Nonce account parsing
│   │   └── geyser_sub.rs          — YS subscription wrapper
│   ├── sources/
│   │   ├── mod.rs
│   │   ├── merger.rs
│   │   └── (reuse entry-sources crate)
│   ├── senders/
│   │   ├── mod.rs                 — TxSender trait
│   │   ├── helius.rs
│   │   ├── triton.rs
│   │   ├── nozomi.rs
│   │   ├── syncro.rs
│   │   ├── astralane.rs
│   │   ├── slot0.rs
│   │   ├── allenhark.rs
│   │   ├── nextblock.rs
│   │   ├── bloxroute.rs
│   │   ├── blockrazor.rs
│   │   ├── jito.rs
│   │   └── harmonic.rs
│   ├── writer.rs                  — parquet
│   ├── rpc_fallback.rs
│   ├── counters.rs
│   ├── run_meta.rs
│   ├── leader_cache.rs            — reuse z tick-trigger-bench lub solana-leader-map
│   └── bin/
│       ├── setup-nonces.rs
│       ├── teardown-nonces.rs
│       └── probe-senders.rs       — Advance-first compatibility check per sender
└── tests/
    ├── e2e_mock.rs
    ├── nonce_lifecycle.rs
    └── matcher_dedup.rs
```

---

## 13. Lista wszystkich potwierdzonych decyzji (one-liner reference)

1. ✅ Osobny crate `fan-out-bench` (nie rozszerzenie tick-trigger-bench)
2. ✅ Dual entry source: SS + YS, dedup po entry_hash; oba timestamps w parquet, min() jako derived
3. ✅ N senderów konfigurowalnych przez config.json, brak hardcode
4. ✅ Każdy region = osobny sender_id (np. jito-fra, jito-ams)
5. ✅ Per-sender najszybszy protokół: HTTP/gRPC/QUIC/WS mix
6. ✅ QUIC dla AllenHark + NextBlock (quinn crate)
7. ✅ **1 byte ASCII-encoded sender_id** w Memo programie (range '!' do '~', 0-93, UTF-8 safe)
8. ✅ Per-sender tip account z rotacją RR (uniknięcie write CU contention)
9. ✅ Per-sender minimum tip (vendor docs), override w configu
10. ✅ Cadence: 1 tx/slot, 1 random tick (deterministyczny seed)
11. ✅ Randomizowana kolejność wysyłki między senderami per trigger
12. ✅ Schedule chunkowany (1000 slotów per chunk), generowany lazy
13. ✅ Open-ended run: stop na min_balance_lamports default 1.5M
14. ✅ **150 durable nonce accounts** (rent ~0.217 SOL refundable, default; configurable)
15. ✅ Setup/teardown nonces osobnymi binarami; rent fetched via `getMinimumBalanceForRentExemption(80)`
16. ✅ RR allocator nonce per trigger, state machine 4 stany
17. ✅ Brak in-flight cancellation wysyłek; Matcher resolves siblings natychmiast (tentative)
18. ✅ **Dwustopniowa rezolucja outcome:** tentative_outcome (real-time) + final_status (post-finality)
19. ✅ Finality tracker komponent: polling `getSignatureStatuses(finalized)`, emit do `finality-updates.jsonl`
20. ✅ Parquet single file per run, row groups co 32k records
21. ✅ Per-sender circuit breaker (5 errors → 60s cooldown)
22. ✅ **Central `tx_builder::build_variant()`** z hard assert ix[0]=AdvanceNonce, ASCII memo safe
23. ✅ **Matcher single-owner per attempt:** `HashMap<(TriggerId, SenderId), AttemptState>`, no race
24. ✅ Schema rozbudowana o 13 kolumn: ss/ys timestamps osobno, commitment level, RPC err, provider request_id, endpoint/protocol/auth_tier, rate_limit_state, prepared_at, message_hash, nonce_update timing, clock_offset
25. ✅ Pre-flight `probe-senders` per sender — Advance-first compatibility check przed pierwszym runem
26. ✅ Nozomi zachowany w v1 mimo QoS penalty bias, post-hoc analiza standalone

---

**Koniec spec'u.** Następny krok: pełen implementation plan przez `writing-plans` skill po zatwierdzeniu spec'u przez usera.
