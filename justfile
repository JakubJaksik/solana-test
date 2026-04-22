default:
    @just --list

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

test-verbose:
    cargo test -- --nocapture

fmt:
    cargo fmt

lint:
    cargo clippy --all-targets -- -D warnings

check: fmt lint test
