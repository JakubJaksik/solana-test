# Entry-comparator runbook (Etap 0 — YS vs SS)

**Cel:** Uruchomić test porównawczy Helius Yellowstone gRPC vs Jito ShredStream
i wygenerować wykresy / raport. Time-cost end-to-end: ~30 min pierwszy raz,
~5 min każdy kolejny run.

**Powiązane:**
- Spec: `../specs/2026-05-08-shredstream-vs-yellowstone-entry-comparison-design.md`
- Plan: `../plans/2026-05-08-entry-comparator-implementation.md`

---

## Inwentaryzacja maszyn

| Maszyna | Rola | Hostname |
|---|---|---|
| Laptop | dev + analiza | `LPLWAR185` |
| Bastion | jump host | `aws-fra-proxy1` |
| Prod-like | testowanie | `aws-fra35-defi-external-test1` (AWS Frankfurt, 8 vCPU AMD EPYC, 15 GB RAM) |

SSH: laptop → bastion (OTP) → prod-like (key forward).

`~/.ssh/config` na laptopie (jednorazowy setup):
```
Host aws-fra35-defi-external-test1
    User jjaksik
    ProxyJump aws-fra-proxy1
```

---

## Faza 0 — Setup jednorazowy

### 0a. Repo na prod-like (przez agent forwarding, klucz na laptopie)

```bash
# Z laptopa, w sesji z agent forwardingiem:
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/id_ed25519_github   # albo właściwy klucz
ssh -A aws-fra35-defi-external-test1

# Na prod-like:
git clone git@github.com:<your>/my-scripts.git ~/solana-test
cd ~/solana-test
```

### 0b. System deps na prod-like

```bash
# Rust 1.85+
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.85.0
source $HOME/.cargo/env

# build essentials
sudo apt update
sudo apt install -y build-essential libclang-dev clang protobuf-compiler \
                    pkg-config libssl-dev libudev-dev cmake numactl
```

### 0c. Build entry-comparator (~12-15 min pierwszy raz)

```bash
cd ~/solana-test
RUSTFLAGS="-C target-cpu=native" cargo build --release -p entry-comparator -j 4
ls -lh target/release/entry-comparator
target/release/entry-comparator --help
```

### 0d. Helius credentials

```bash
cat > ~/.entry-comparator.env <<'EOF'
HELIUS_GRPC_URL=https://YOUR-DEDICATED-NODE.helius-rpc.com:2053
HELIUS_GRPC_TOKEN=YOUR_HELIUS_TOKEN
SOLANA_RPC_URL=https://api.mainnet-beta.solana.com
EOF
chmod 600 ~/.entry-comparator.env
```

> URL i token wyciągasz z konfiguracji `dex-radar` / `dex-trader` — tych samych
> używamy. URL może mieć port `:2053` dla gRPC lub inny zdefiniowany przez Helius.

### 0e. Jito ShredStream proxy — clone + build

```bash
cd ~
git clone https://github.com/jito-labs/shredstream-proxy.git
cd shredstream-proxy
git submodule update --init --recursive   # KRYTYCZNE — proto submoduły
cargo build --release -p jito-shredstream-proxy
ls -lh target/release/jito-shredstream-proxy
```

### 0f. Jito keypair

Wrzuć keypair (od onboardingu Jito whitelist) na prod-like:

```bash
# Z laptopa:
scp ~/path/to/jito-keypair.json aws-fra35-defi-external-test1:~/jito-keypair.json

# Na prod-like:
chmod 600 ~/jito-keypair.json
```

### 0g. Sanity outbound do Jito Block Engine

```bash
curl -sI https://mainnet.block-engine.jito.wtf/ 2>&1 | head -3
# Oczekiwane: HTTP/2 200 lub redirect — byle nie connection refused
```

---

## Faza 1 — Uruchomienie testu

### 1a. Uruchom Jito proxy (terminal A — zostaw aktywny przez cały test)

```bash
ssh -A aws-fra35-defi-external-test1
cd ~/shredstream-proxy

RUST_LOG=info ./target/release/jito-shredstream-proxy shredstream \
  --block-engine-url https://mainnet.block-engine.jito.wtf \
  --auth-keypair ~/jito-keypair.json \
  --desired-regions frankfurt \
  --src-bind-port 20000 \
  --dest-ip-ports 127.0.0.1:8001 \
  --grpc-service-port 9999
```

Logi do potwierdzenia:
- `Retrieved public ip: X.X.X.X`
- `Sending heartbeat every 40s.`
- `Shredstream started, listening on 0.0.0.0:20000/udp.`
- `shredstream_proxy-listen_thread packets_count=10000+i` co sekundę

Jeśli `packets_count` zostaje 0 — firewall blokuje inbound UDP 20000. Otwórz tam.
(Na naszym hoście AWS było OK domyślnie.)

### 1b. Uruchom entry-comparator (terminal B)

```bash
ssh -A aws-fra35-defi-external-test1
cd ~/solana-test
mkdir -p ~/runs
set -a; source ~/.entry-comparator.env; set +a

# weryfikacja env (TOKEN zaślepiony)
env | grep -E "HELIUS|SOLANA" | sed 's/TOKEN=.*/TOKEN=***/'

RUST_LOG=info ./target/release/entry-comparator run \
  --output-dir ~/runs \
  --duration 15m \
  --core-pinning ys=2,ss_rx=3,deshred=4,corr=5,writer=6
```

> `--duration` — zmieniaj wg potrzeby (5m sanity, 15m, 2h, lub `--duration 1d`
> na całą epokę). Defaults używają **gRPC mode** (`--shredstream-mode grpc`)
> z proxy na `127.0.0.1:9999`. Dla legacy raw-UDP użyj `--shredstream-mode udp`.

Logi do potwierdzenia:
- `run directory ready run_dir="..."`
- `fetched current slot current_slot=N epoch_at_start=N`
- `starting ShredStream gRPC source endpoint="http://127.0.0.1:9999"`
- `shredstream grpc subscription open`
- `yellowstone entry subscription open`
- `comparator running`

Po czasie `duration` zobaczysz `shutdown complete; Parquet finalized`.

### 1c. Quick verify (po zakończeniu)

```bash
RUN_DIR=$(ls -1d ~/runs/*/ | tail -1)
./target/release/entry-comparator report \
  --input-dir "$RUN_DIR" --output "$RUN_DIR/report.md"
cat "$RUN_DIR/report.md"

# anomalie (powinny być znikome)
grep -oE '"counter":"[^"]+"' "$RUN_DIR/anomalies.jsonl" | sort | uniq -c | sort -rn

ls -lh "$RUN_DIR"
```

Spodziewane:
- `BOTH ≥ 99%` z `Total`
- `YS_ONLY`, `SS_ONLY` < 1% każdy
- `Hash mismatches: 0`
- `SS earlier than YS: 99%+ samples`
- `anomalies.jsonl`: max kilka set per godzinę (`ss_obs_channel_full`,
  `ys_reconnects`)

Jeśli nie pasuje — zob. **Troubleshooting** na dole.

---

## Faza 2 — Pobranie danych na laptop

### 2a. Spakuj run

```bash
# Na prod-like:
RUN_DIR=$(ls -1d ~/runs/*/ | tail -1)
RUN_NAME=$(basename "$RUN_DIR")
tar czf ~/run.tar.gz -C "$RUN_DIR" .
ls -lh ~/run.tar.gz
```

### 2b. Pobierz przez bastion (ProxyJump)

```bash
# Na laptopie:
mkdir -p ~/solana-analysis/runs/<RUN_NAME>
scp aws-fra35-defi-external-test1:~/run.tar.gz /tmp/
cd ~/solana-analysis/runs/<RUN_NAME>   # zastąp <RUN_NAME> rzeczywistym timestamp'em
tar xzf /tmp/run.tar.gz
ls
# diff.parquet, run-meta.json, leader-schedule.json, anomalies.jsonl
```

---

## Faza 3 — Walidator geo map

### 3a. solana-leader-map setup (tylko pierwszy raz)

```bash
cd ~/Repos/my-scripts
cat crates/solana-leader-map/config.example.json
# skopiuj jako config.json i wypełnij (validators.app API token, RPC URL)
cp crates/solana-leader-map/config.example.json crates/solana-leader-map/config.json
nano crates/solana-leader-map/config.json
```

### 3b. Fetch + export dla bieżącej epoki

```bash
cd ~/Repos/my-scripts/crates/solana-leader-map
cargo run --release -p solana-leader-map -- fetch

# export do JSON pod analizę (zmień 969 na bieżącą epokę testu)
cargo run --release -p solana-leader-map -- export --epoch 969 \
  > ~/solana-analysis/validators-epoch-969.json
head -c 300 ~/solana-analysis/validators-epoch-969.json
```

> Jeśli test był na epoce N, użyj `--epoch N`. Geo walidatorów jest stabilne
> między epokami, więc nawet ±1 epoki różnicy nie psuje analizy.

---

## Faza 4 — Analiza danych (Python)

### 4a. venv + deps (pierwszy raz)

```bash
# Na laptopie:
python3 -m venv ~/solana-analysis/venv
source ~/solana-analysis/venv/bin/activate
pip install pandas matplotlib seaborn duckdb pyarrow base58
```

W kolejnych sesjach:
```bash
source ~/solana-analysis/venv/bin/activate
```

### 4b. Skrypt analizy

Plik `~/solana-analysis/analysis.py` (zachowany w tym repo? jeśli nie — przepisz
ze runbook'a z 2026-05-11 lub patrz Git history).

Edytuj na początku skryptu ścieżki:
```python
RUN_DIR = Path("~/solana-analysis/runs/<RUN_NAME>").expanduser()
VALIDATORS_JSON = Path("~/solana-analysis/validators-epoch-<N>.json").expanduser()
OUT_DIR = Path("~/solana-analysis/plots/<RUN_NAME>").expanduser()
```

Uruchom:
```bash
python ~/solana-analysis/analysis.py
```

Output: katalog `OUT_DIR` z PNG-ami:
- `01-global-cdf.png`
- `02-per-continent-violin.png`
- `03-per-country-hist.png`
- `04-per-validator-box.png`
- `05-missing-breakdown.png`
- `06-bars-continent-ss-vs-ys.png` (jeśli dorzucone)
- `07-bars-country-ss-vs-ys.png` (jeśli dorzucone)
- `08-percentile-continent.png` (p1/p10/p50/p90/p99)
- `09-percentile-country.png`
- `10-median-continent.png`
- `11-median-country.png`
- `summary-continent.csv`, `summary-country.csv`

Plus na stdout idzie summary table do wklejenia w raporcie.

---

## Faza 5 — Raport w Notion

### 5a. Struktura strony

```
## TESTY

### ETAP 0: Porównanie źródeł danych

Porównanie Jito ShredStream vs Helius Dedicated Node gRPC

| Run | Epoch | Duration | Samples | Match% | SS p50 | SS p99 | Worst |
|-----|-------|----------|---------|--------|--------|--------|-------|
| #1  | 969   | 15 min   | 1.5M    | 99.78% | 6.93   | 28.0   | -17.0 |
| #2  | 970   | 2h       | ?       | ?      | ?      | ?      | ?     |

▶ Run #1 — Epoch 969 · 15 min
▶ Run #2 — Epoch 970 · 2h
```

Każdy `▶` to **toggle** (`/toggle`). Wewnątrz:

```
💡 Callout — Metadata
| Pole          | Wartość                       |
|---------------|-------------------------------|
| Epoch         | N                             |
| Duration      | Xm/h                          |
| Date          | YYYY-MM-DD HH:MM UTC          |
| Host          | aws-fra35-defi-external-test1 |
| Region        | Frankfurt (eu-central-1)      |
| Samples       | N entries                     |
| Match rate    | X.XX%                         |
| SS p50        | X.XX ms                       |

📌 Callout — TL;DR (kolor zielony)
- ShredStream szybsze w X% przypadków
- Median advantage: X ms
- Worst case: Y ms

▼ Wykresy globalne
  [01-global-cdf.png]
  [05-missing-breakdown.png]

▼ Per kontynent
  [02-per-continent-violin.png]
  [10-median-continent.png]
  [08-percentile-continent.png]

▼ Per państwo (top 10)
  [03-per-country-hist.png]
  [11-median-country.png]
  [09-percentile-country.png]

▼ Per validator (top 20)
  [04-per-validator-box.png]

▼ Obserwacje
  - ...
  - ...
```

### 5b. Skalowanie na kolejne runy

Kliknij prawym → Duplicate toggle. Zmień: tytuł, metadata, podmień wykresy.
Dorzucaj wiersz do sumarycznej tabeli na górze.

---

## Troubleshooting

### Proxy: `packets_count=0`
Firewall blokuje inbound UDP `--src-bind-port` (default 20000). Sprawdź security group / iptables. Z Jito relayerów (region Frankfurt) musi dochodzić.

### entry-comparator: `gRPC status: code: 'Unauthenticated'`
Brak `HELIUS_GRPC_TOKEN` w env. Sprawdź `~/.entry-comparator.env`, sourcuj
przez `set -a; source ...; set +a`.

### Report: `Invalid Parquet file. Corrupt footer`
Process killed przed flushem. Sprawdź czy w logach było `shutdown complete;
Parquet finalized`. Jeśli nie — bug shutdown / Ctrl+C podczas runu. Dane stracone.

### Dużo YS_ONLY (>10%)
Tryb gRPC: proxy się rozłączył albo gubił FEC sety. Sprawdź logi proxy
(`shredstream_proxy-listen_thread packets_count=` powinno być żywe).
Tryb UDP: brakuje FEC recovery (known limitation). Switch na gRPC mode.

### `ss_obs_channel_full` rośnie
Channel za mały na rate. `--channel-capacity 262144` w entry-comparator
(domyślnie 65536).

### `ys_channel_full` rośnie
Same. Plus sprawdź czy pinning Core ys (=2 w defaults) nie konkuruje z innym
procesem. `htop` → identify.

### Analysis Python: `error: externally-managed-environment`
Aktywuj venv:
```bash
source ~/solana-analysis/venv/bin/activate
```

---

## Cleanup po runie (opcjonalnie)

```bash
# Prod-like — usuń stare runy żeby nie zalegały:
rm -rf ~/runs/<OLD_RUN_NAME>

# Lub bulk usunięcie starszych niż 7 dni:
find ~/runs -maxdepth 1 -type d -mtime +7 -name "20*" -exec rm -rf {} \;
```

---

## Wersja runbooka

- **2026-05-11 v1** — pierwszy zapis. Etap 0 z gRPC mode jako primary,
  legacy UDP mode dostępny przez `--shredstream-mode udp`.
