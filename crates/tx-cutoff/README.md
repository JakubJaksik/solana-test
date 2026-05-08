# tx-cutoff

Transaction inclusion cutoff measurement tool dla EVM chainów. Mierzy najpóźniejszy
moment (w ms po `block.timestamp`) kiedy wysłana tx nadal trafia w następny blok.

## Wymagania

- Rust 2024 (stable via rustup) — 1.85+
- Dostęp do RPC: WebSocket (`newHeads` subscription) + HTTP (`eth_sendRawTransaction`)
- Kilka walletów z ETH + tokenami (WETH/USDC na Base)

## Build

### Dev (natywnie)

    cargo build --release
    # → target/release/tx-cutoff

### Linux static (serwer — x86_64)

    just build-linux-x64
    # → target/x86_64-unknown-linux-musl/release/tx-cutoff

### Linux static (serwer — ARM64 / Graviton)

    just build-linux-arm64
    # → target/aarch64-unknown-linux-musl/release/tx-cutoff

## Konfiguracja

Skopiuj `config.example.json` → `config.json`, uzupełnij:

- RPC WebSocket + HTTP URLe
- Private keys walletów (inline w `wallets[]`)
- Timing (`start_ms`, `end_ms`, `step_ms`, `samples_per_wallet_per_slot`)
- Gas (`max_priority_fee_gwei`, `max_fee_multiplier`)
- Adresy tokenów i routera dla swapu

**BEZPIECZEŃSTWO:**

    chmod 600 config.json

`.gitignore` wyklucza `config.json` z commitów.

### Przykład konfiguracji Base

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
  ...
}
```

Pełny schemat w `config.example.json`.

## Uruchomienie

    ./tx-cutoff --config config.json

Flagi:

- `--yes` — pomija pre-flight confirmation prompt (unattended mode)
- `--output-dir <PATH>` — override `output.dir` z configa
- `--log-level <LEVEL>` — `trace|debug|info|warn|error`

## Deployment na serwer (AWS EC2)

### Amazon Linux 2023 / Ubuntu 22.04+

Build lokalnie:

    just build-linux-x64

Deploy:

    scp target/x86_64-unknown-linux-musl/release/tx-cutoff <user>@<server>:/opt/tx-cutoff/
    scp config.json <user>@<server>:/opt/tx-cutoff/

Na serwerze:

    cd /opt/tx-cutoff
    chmod 600 config.json
    ./tx-cutoff --config config.json
    # (lub w tmux/screen dla długich runów)

### Rekomendowana instancja

- `m7i.2xlarge` (8 vCPU Intel x86_64) — dla ≤5 walletów
- `m7g.2xlarge` (8 vCPU Graviton ARM64) — tańsze
- **`xlarge` (4 vCPU)** zadziała ale przy >2 walletach wake jitter rośnie (patrz spec)

## Jak to działa

1. **Pre-flight** — sprawdza RPC, wallety, allowance (auto-approve), robi kalibracyjny swap (pomiar gas), projektuje koszt runu, pyta o potwierdzenie.
2. **Main loop** — subskrybuje `newHeads`, dla każdego bloku:
   - Oblicza `target_time = block.timestamp + slot_ms` (deterministic slot per block)
   - Pre-signuje EIP-1559 tx per wallet (swap ping-pong)
   - Spawnuje task per wallet z hybrid sleep (tokio + busy-wait) aż do `target_time`
   - Wysyła `eth_sendRawTransaction` i rejestruje timing
   - Tracker obserwuje kolejne bloki i klasyfikuje inclusion: target / late / dropped
3. **Report** — po zakończeniu planu: finalny stdout raport + JSONL per-tx + CSV per-slot + markdown.

## Output

Każdy run tworzy katalog `runs/YYYY-MM-DD-HHMMSS/` zawierający:

- `config.snapshot.json` — użyty config (klucze redacted)
- `tx_log.jsonl` — per-tx log (dwuetapowy: Pending po send, potem Target/Late/Dropped po klasyfikacji)
- `summary.csv` — per-slot aggregate dla analityki
- `report.md` — markdown raport z krzywą cutoff + percentile breakpoints

## TDD / testy

    just test
    just check  # fmt + clippy + test

Wszystkie moduły (config, scheduler, time, wallet, swap, rpc, tracker, report, preflight, engine) mają integration testy w `tests/*_test.rs`. Aktualnie ~50 testów.

## Spec

Pełna specyfikacja architektury + decyzje projektowe: [`docs/superpowers/specs/2026-04-22-tx-inclusion-cutoff-design.md`](docs/superpowers/specs/2026-04-22-tx-inclusion-cutoff-design.md)

Plan implementacji (task-by-task): [`docs/superpowers/plans/2026-04-22-tx-cutoff.md`](docs/superpowers/plans/2026-04-22-tx-cutoff.md)

## Known limitations

1. **Base block.timestamp ma sekundową rozdzielczość** — wprowadza ±500ms jitter między real-time a timestamp. Uśredniane przez 20 sampli per slot. Na ETH mainnet ten problem znika (deterministyczne 12s slot clock).
2. **Nonce gap cascading** — jeśli tx przestają wchodzić (slot > cutoff), kolejne tx z tego samego walleta blokują się w mempool aż stare się sklaruje (timeout ~1-2 min). Widoczne w JSONL jako dropped.
3. **Brak WS fallback / RPC redundancy** — pojedynczy RPC, abort jeśli padnie (intencja: clean measurement, nie silent failover).
4. **Brak replace-by-fee** dla stuck tx — missed = counted as dropped, continue.
