# my-scripts

Cargo workspace z narzędziami pomiarowymi dla DEX-trading research.

## Crates

| Crate | Cel | Status |
|---|---|---|
| [`crates/tx-cutoff`](./crates/tx-cutoff/) | Mierzy najpóźniejszy moment (ms po `block.timestamp`) gdy tx EVM nadal trafia w następny blok. | działający |
| [`crates/solana-leader-map`](./crates/solana-leader-map/) | Per-epoch mapa Solana validator → lokalizacja. Cross-reference z leader schedule. Agregaty per kraj/region/DC. JSON snapshot per epoch. | działający |

## Dlaczego workspace

Wspólne deps (`tokio`, `reqwest`, `serde`, `clap`, `tracing`, `chrono`) pinowane raz w `Cargo.toml` (sekcja `[workspace.dependencies]`). Wspólny `Cargo.lock` → szybsze buildy między crate-ami.

## Wymagania

- Rust 2024 stable (1.85+) — patrz `rust-toolchain.toml`.
- Per-crate dodatkowe wymagania w ich README.

## Build

    # cały workspace
    cargo build --release

    # konkretny crate
    cargo build --release -p tx-cutoff
    cargo build --release -p solana-leader-map

## Test

    cargo test                        # wszystko
    cargo test -p solana-leader-map   # konkretny crate

## Dodawanie nowego skryptu

```bash
mkdir -p crates/<nazwa>/src
# w Cargo.toml workspace dopisz do `members`:
#   "crates/<nazwa>",
# w crates/<nazwa>/Cargo.toml użyj `edition.workspace = true` itp.
# wspólne deps deklaruj jako `tokio.workspace = true` zamiast wersjonować osobno
```

## Just commands

    just build              # cargo build --release (cały workspace)
    just build-linux-x64
    just build-linux-arm64
    just test
    just lint               # clippy --all-targets -D warnings
    just fmt
    just check              # fmt + lint + test

Per-crate komendy (np. configi do tx-cutoff vs solana-leader-map) — uruchamiaj `cargo run -p <crate> -- <args>` ręcznie, albo dodaj recipe per crate w przyszłości.
