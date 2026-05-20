# tick-trigger-fan-out-bench

Modułowa przebudowa benchu od zera. Każda warstwa (faza) jest:

1. **Niezależnie uruchamialna end-to-end** — własna binarka `phaseN_*`.
2. **Metrykowana** — counters → snapshot JSON + periodyczne log-line summaries.
3. **Testowalna** — unit + integration testy.
4. **Komponowalna** — output kolejnej fazy = input następnej (kanały
   `crossbeam_channel`).

## Faza 1 — weryfikacja źródeł danych

```text
  ShredStream gRPC ──┐
                     ├─▶ entry-merger ──▶ ordering-tracker ──▶ metrics
  Yellowstone gRPC ──┘   (one entry =
                          one emission,
                          first-seen wins,
                          latency stats
                          internal)
```

**Cel:** zanim zbudujemy cokolwiek wyżej, upewniamy się że stream entries jest
rzetelny. Każde unikalne `(slot, entry_hash)` przechodzi **dokładnie raz**
przez kanał wyjściowy — downstream ma pojedynczy unified stream. Pod spodem
mierzymy KTO i o ILE wyprzedził, ale dane wypływają jako jeden strumień.

### Co mierzymy

Merger:
- `ss_received`, `ys_received` — surowy receive per źródło
- `ss_first`, `ys_first` — kto pierwszy widział unikalne entry (= kto wygrał)
- `confirmed_by_both` — ile unikalnych entries potwierdziło drugie źródło
- `confirm_latency_{sum,min,max}_ns` — rozkład inter-source latency
  (gdy oba źródła widzą entry, mierzymy o ile drugie się spóźniło)
- `duplicates` — same-source replay lub 3-ie+ przybycie (powinno być ~0)

Ordering tracker (per sealed slot):
- `entries_seen`, `max_index` — wielkość slotu
- `out_of_order_count` — ile entries przybyło PO entry z wyższym indexem
- `max_backward_gap` — największy "cofnięcie" (max_idx_at_arrival - this_idx)
- `missing_indices` — indeksy < max_index których nigdy nie widzieliśmy
- `last_entry_was_tick` — czy slot kończy się tickiem (powinien zawsze)

Aggregaty:
- `slots_fully_ordered` / `slots_sealed` — frakcja idealnych slotów
- `total_out_of_order` — suma backwards arrivals
- `tick_ending_rate` — frakcja slotów kończących się tickiem

### Jak uruchomić

```bash
cargo build --release -p tick-trigger-fan-out-bench

./target/release/phase1_observe \
  --ss-url http://127.0.0.1:9999 \
  --ys-url https://your-helius.com:2053 \
  --ys-token <UUID> \
  --duration 60s \
  --output runs/phase1-$(date +%Y%m%d-%H%M%S).json
```

Wypisze co 5s one-line summary, na końcu pełen raport (per-slot detail w JSON).

### Co interpretować

**Healthy mainnet (oczekiwane):**
- `tick_ending_rate ≈ 100%` — każdy slot kończy się tickiem 64
- `slots_fully_ordered > 95%` — większość slotów bez przeplotów
- `both_confirm_rate > 95%` — SS i YS powinny widzieć te same entries
- `total_missing_indices ≈ 0` — żadnych dziur w środku slotu
- `avg_entries_per_slot ≈ 100-300` — typowa mainnet aktywność

**Sygnały alarmowe:**
- `tick_ending_rate << 100%` → źródło traci ostatnie ticki slotu (gubi
  ostatnie shred'y FEC). To NIE pozwala policzyć durable-nonce-blockhash
  lokalnie — patrz dalsze fazy.
- `avg_out_of_order_per_slot >> 1` → entries naprawdę przybywają reordered.
  Naive "last write wins" cache hashy będzie błędna; wymagany index-aware
  store.
- `slots_with_gaps > 0` → niektórych entries nigdy nie widzimy. Albo
  źródło drop'uje, albo dedup window jest za krótki (raczej nie).
- `duplicates >> 0` → bug po stronie źródła lub mergera.

### Plan kolejnych faz

- **Faza 2:** PoH tick tracking + schedule firing (przeniesienie observer-a)
- **Faza 3:** sender layer (multi-vendor fan-out, fresh blockhash mode)
- **Faza 4:** parquet writer + finality tracker
- **Faza 5 (opt):** durable nonce mode z chain-hash fallback

Każda faza dostanie własną binarkę i własny JSON output, kompatybilną z
poprzednią warstwą.
