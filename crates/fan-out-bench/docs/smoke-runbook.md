# fan-out-bench — smoke test runbook

Pierwsze uruchomienie end-to-end na mainnet. Cel: zwalidować że pipeline produkuje parquet z sensownymi LANDED/DEDUPED countami. NIE odpalaj z pełną pulą 150 nonces i wszystkimi senderami na pierwszy raz — zacznij mały.

## Prerequisites

- `setup_nonces` wykonany dla małej puli na początek (np. N=5)
- Wallet z ~0.05 SOL operational budget (poza locked rent)
- Helius RPC URL z dostatecznym credit
- Jito Shredstream proxy uruchomiony na `127.0.0.1:9999`
- Helius dedicated node Yellowstone gRPC URL (lub fallback do public jeśli dostępny)

## Smoke config

Skopiuj `config.example.json` do `smoke-config.json` i dostosuj:

- `nonce.pool_size: 5` (zgodne z tym ile setup'owałeś)
- `run.chunk_size_slots: 30` (krótki run)
- `run.min_balance_lamports: 1500000`
- `senders`: tylko `helius` + `jito-fra-tx` (2 senderów dla minimal blast radius)
- `run.wallet_keypair_path`: ścieżka do walletu
- `sources.shredstream_grpc_url`: `http://127.0.0.1:9999`
- `sources.yellowstone_grpc_url`: dedicated URL
- `sources.yellowstone_auth_token`: token jeśli wymagany
- `sources.helius_rpc_url`: Helius RPC URL

## Setup nonce pool (one-time)

```bash
cargo build --release -p fan-out-bench
./target/release/setup_nonces \
  --rpc-url <HELIUS_RPC_URL> \
  --wallet ~/.config/solana/dex-bench.json \
  --count 5 \
  --output-keypairs nonce-keypairs.json \
  --output-config nonce-config.json
```

Powinno zalockować ~0.0072 SOL rent (5 × 0.00144768 SOL).

## Run

```bash
./target/release/run --config smoke-config.json
```

Obserwuj logi przez ~2 min. Potem Ctrl-C.

## Co powinno się dziać

- Co 5 sekund: counter snapshot log
- Output `runs/<timestamp>/tx-events.parquet` zaczyna się zapełniać po pierwszych triggerach
- `runs/<timestamp>/finality-updates.jsonl` zapełnia się po ~30s od pierwszego landingu (finality tracker poll interval)
- W trakcie: `pool_empty`, `send_http_error`, `send_throttled_429`, `finality_confirmed` w logach

## Co sprawdzić w output

Po Ctrl-C i shutdown:

```bash
# Parquet — ile rows i jakie outcomes
python3 -c "
import pyarrow.parquet as pq
import collections
t = pq.read_table('runs/<timestamp>/tx-events.parquet')
print('total rows:', t.num_rows)
print('outcomes:', collections.Counter(t['tentative_outcome'].to_pylist()))
print('senders:', collections.Counter(t['sender_name'].to_pylist()))
"

# Finality updates
wc -l runs/<timestamp>/finality-updates.jsonl
head runs/<timestamp>/finality-updates.jsonl
```

Oczekiwane:
- `total_rows`: ~30 slots × ~3% trigger rate × 2 senders ≈ 50-150 (zależy od ile tx faktycznie wyszło)
- `LANDED_TENTATIVE`: ~1/3 wszystkich (dedup, 1 winner per trigger)
- `DEDUPED_TENTATIVE`: ~2/3 (sibling losers)
- `SEND_ERROR`: jeśli sender 429-uje
- `UNKNOWN_PENDING`: jeśli deadline minął bez landingu

## Co może pójść źle

| Problem | Symptom | Fix |
|---|---|---|
| Pool empty częste | `pool_empty > 0` rośnie | N=5 nonces za mało, zwiększ pool albo zmniejsz cadence |
| 429 z Helius | `send_throttled_429` rośnie | Default 50 TPS powinien wystarczyć dla N=5; jeśli się dzieje, masz inny problem |
| Jito drops | UNKNOWN_PENDING dla Jito | Jito default 1 rps/IP/region — to normal; możemy też testować z `jito-fra-bundle` (Plan 6) |
| 0 triggerów | `schedule_contains_true: 0` | Observer dostał pusty schedule. Sprawdź czy schedule-bridge thread działa (logs) |
| 0 entries observed | brak żadnego ruchu | SS/YS nie connectują — sprawdź gRPC endpoints, ping |
| Budget watcher stop natychmiast | "balance below threshold" | Twój wallet ma mniej niż `min_balance + nonce_rent`. Dopełnij wallet. |

## Teardown

```bash
./target/release/teardown_nonces \
  --rpc-url <HELIUS_RPC_URL> \
  --wallet ~/.config/solana/dex-bench.json \
  --keypairs nonce-keypairs.json
```

Refunduje locked rent (~0.0072 SOL przy N=5).
