# solana-leader-map

Per-epoch mapa **Solana validator → lokalizacja geograficzna**. Krzyżuje `getLeaderSchedule` z dataset'em validators.app, zapisuje snapshot do JSON na epoch (~2 dni TTL), wystawia agregaty per kraj/region.

## Po co to

Dla HFT na Solanie warto wiedzieć **gdzie fizycznie jest następny leader** — bo to determinuje:
- czy fast-sender po waszej stronie jest blisko (intra-metro RTT vs cross-region 100–230 ms);
- czy strategia powinna być aggressive na tym slot-window czy raczej skip (jeśli leader jest "daleko");
- jaki rozkład DC ma cała sieć w danym epoch'u (dla sanity-check baseline).

Dataset jest static per epoch — leader schedule fixuje się na cały epoch (~432K slotów ≈ 2 dni). Fetchujesz raz, używasz przez 2 dni.

## Wymagania

- **Rust 2024** stable (1.85+).
- Konto na **validators.app** (darmowe, https://www.validators.app/users/sign_up) → Settings → API Tokens → wygeneruj token.
- Dostęp do **HTTP RPC Solany** — wasz dedicated node Heliusa (przez proxy `sol3:8899`) lub publiczny `https://api.mainnet-beta.solana.com`.

## Setup

```bash
# z root workspace:
cargo build --release -p solana-leader-map
# binary: target/release/solana-leader-map

# config:
cd crates/solana-leader-map/
cp config.example.json config.json
chmod 600 config.json
# edit: wklej api_token i URL waszego RPC
```

`config.json` jest w `.gitignore` (workspace-level).

## Użycie

```bash
# 1. Pobierz aktualny epoch (validators.app + getLeaderSchedule)
target/release/solana-leader-map fetch

# (Cache w runs/leader-map-epoch-{N}.json — refetch co ~2 dni gdy epoch się zmieni.
#  --force żeby refetchować nawet gdy cache istnieje.)

# 2. Tabela kraj × % slotów × % stake'u × liczba validatorów
target/release/solana-leader-map summary

# 3. Co ten konkretny slot
target/release/solana-leader-map at 251234567

# 4. Range slotów (inclusive)
target/release/solana-leader-map slots 251234500..251234520

# 5. Surowy JSON cały snapshot (do skarmienia innym narzędziom)
target/release/solana-leader-map export > snapshot.json
```

Wszystkie komendy poza `fetch` mają opcjonalny `--epoch <N>` jeśli chcesz starszy cached epoch.

### Wybrane opcje
- `-c <path>` lub `--config <path>` — alternatywny config (default: `./config.json`).
- `RUST_LOG=debug` — verbose tracing (np. RPC calls, parse counts).

## Format cache

`runs/leader-map-epoch-{N}.json`:

```json
{
  "fetched_at": "2026-04-27T...",
  "epoch": { "epoch": 695, "absolute_slot": ..., "slot_index": ..., "slots_in_epoch": 432000 },
  "validators": [
    { "identity": "...", "name": "Helius", "country_code": "DE", "data_center_key": "Hetzner-DE-FRA", "active_stake_lamports": 14000000000000000, ... },
    ...
  ],
  "schedule": { "<identity>": [slot_idx_relative, ...], ... }
}
```

Wszystko co potrzebne do reanalizy ex-post — slot map odbudowujesz `aggregate::build_slot_map` z biblioteki.

## Pułapki które trzeba znać

1. **IP geolocation jest niedoskonały dla cloud/VPS.** Validator na AWS `eu-central-1` może być oznaczony jako "US" zamiast "DE" jeśli ASN jest US-rejestrowany. Validators.app robi to lepiej niż większość (MaxMind + manual overrides), ale weryfikuj outliers.
2. **Stake weighted ≠ slot weighted w pojedynczym oknie.** Per epoch średnio dystrybucja zgadza się z stake'em, ale w konkretnym 4-slot oknie może być 4× pod rząd ten sam validator. `summary` daje ci średnią epoch'ową, `slots`/`at` daje ci konkret per-slot.
3. **`country_code: "??"`** to bucket "validator nieznany validators.app" (występuje w schedule ale nie ma go w geo-dataset). Powinien być <2-3% slotów. Jeśli więcej — sprawdź czy validators.app ma świeże dane.
4. **Refresh per epoch.** Schedule zmienia się co ~2 dni. Ten skrypt nie auto-refetchuje — dodaj cron / systemd-timer co 24h jeśli chcesz live data.

## Architektura (dla rozszerzeń)

```
src/
  main.rs           — entry, tracing setup
  cli.rs            — clap CLI + run dispatcher
  config.rs         — load config.json
  validators_app.rs — REST client validators.app
  solana_rpc.rs     — JSON-RPC client (getEpochInfo, getLeaderSchedule)
  domain.rs         — typy: ValidatorInfo, EpochInfo, EpochSnapshot, SlotEntry, EpochSummary
  aggregate.rs      — slot-map + summary logic + unit tests
  cache.rs          — read/write runs/leader-map-epoch-{N}.json
  output.rs         — pretty CLI tables (comfy-table)
  lib.rs            — re-export modułów
tests/
  (unit testy są w aggregate.rs i cli.rs przy pomocy `#[cfg(test)] mod tests`)
```

Do dorzucenia w przyszłości (otwarte):
- Subscribe na `slotsUpdates` przez gRPC (Yellowstone) — live mapa zamiast static-per-epoch.
- IP geolocation z MaxMind GeoIP2 jako fallback gdy validators.app brakuje rekordu.
- Eksport do Prometheusa (per-country slot %).
- Korelacja z waszym dex-trader history-module — "ile % slotów landing rate per kraj leadera".
