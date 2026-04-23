use anyhow::{Context, Result};
use chrono::Local;
use clap::Parser;
use std::path::PathBuf;
use std::sync::Arc;
use tracing_subscriber::EnvFilter;

use tx_cutoff::config::Config;
use tx_cutoff::engine::{self, EngineContext};
use tx_cutoff::preflight;
use tx_cutoff::rpc::HttpRpcClient;
use tx_cutoff::scheduler::{Schedule, SchedulerConfig};
use tx_cutoff::swap::{PingPongState, SwapEncoder};
use tx_cutoff::wallet::Wallet;

#[derive(Parser, Debug)]
#[command(version, about = "Transaction inclusion cutoff measurement")]
struct Cli {
    #[arg(long, default_value = "config.json")]
    config: PathBuf,

    #[arg(long, help = "Skip pre-flight confirmation prompt")]
    yes: bool,

    #[arg(long, help = "Override output dir")]
    output_dir: Option<PathBuf>,

    #[arg(long, default_value = "info")]
    log_level: String,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Logging
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&cli.log_level));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();

    // Load config
    let cfg = Config::load(&cli.config)
        .with_context(|| format!("loading config from {:?}", cli.config))?;
    let cfg = Arc::new(cfg);

    // Output dir
    let base_dir = cli
        .output_dir
        .clone()
        .unwrap_or_else(|| PathBuf::from(&cfg.output.dir));
    let run_id = Local::now().format("%Y-%m-%d-%H%M%S").to_string();
    let output_dir = base_dir.join(&run_id);
    std::fs::create_dir_all(&output_dir)
        .with_context(|| format!("creating output dir {:?}", output_dir))?;

    // Redacted config snapshot
    let snap = cfg.to_snapshot();
    std::fs::write(
        output_dir.join("config.snapshot.json"),
        serde_json::to_string_pretty(&snap)?,
    )?;

    // Build runtimes
    let main_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .thread_name("main-rt")
        .build()?;

    let send_worker_count = cfg.send.resolved_worker_threads(cfg.wallets.len());
    let _send_rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(send_worker_count)
        .enable_all()
        .thread_name("send-rt")
        .on_thread_start(|| {
            use std::sync::atomic::{AtomicUsize, Ordering};
            static COUNTER: AtomicUsize = AtomicUsize::new(0);
            if let Some(core_ids) = core_affinity::get_core_ids()
                && !core_ids.is_empty()
            {
                let idx = COUNTER.fetch_add(1, Ordering::SeqCst) % core_ids.len();
                let _ = core_affinity::set_for_current(core_ids[idx]);
            }
        })
        .build()?;

    main_rt.block_on(async {
        let http =
            Arc::new(HttpRpcClient::new(&cfg.chain.rpc_http).context("read HTTP RPC client init")?);
        // Optional dedicated send endpoint (np. sequencer.base.org dla Base).
        // Jeśli config nie ma rpc_http_send — reuse read client.
        let send_http: Arc<HttpRpcClient> = if cfg.chain.rpc_http_send.is_some() {
            Arc::new(HttpRpcClient::new(cfg.chain.send_url()).context("send HTTP RPC client init")?)
        } else {
            http.clone()
        };
        // Optional bundle endpoint (np. rpc.beaverbuild.org na ETH). Jeśli ustawiony,
        // engine main loop używa eth_sendBundle zamiast eth_sendRawTransaction.
        let bundle_http: Option<Arc<HttpRpcClient>> = match &cfg.chain.bundle_url {
            Some(url) => Some(Arc::new(
                HttpRpcClient::new(url).context("bundle HTTP RPC client init")?,
            )),
            None => None,
        };

        let mut wallets = Vec::with_capacity(cfg.wallets.len());
        for wc in &cfg.wallets {
            wallets.push(Wallet::from_config(wc).context("wallet load")?);
        }

        let outcome = preflight::run(&cfg, &http, &send_http, &wallets, cli.yes)
            .await
            .context("pre-flight failed")?;

        // Build encoders + ping-pong per wallet (initial dir based on current balances)
        let mut encoders = Vec::new();
        let token_a: alloy::primitives::Address = cfg.swap.token_a.parse()?;
        let token_b: alloy::primitives::Address = cfg.swap.token_b.parse()?;
        for w in &wallets {
            let enc = SwapEncoder::new(&cfg.swap, w.address()).context("swap encoder")?;
            let bal_a = preflight::erc20_balance(&http, token_a, w.address()).await?;
            let bal_b = preflight::erc20_balance(&http, token_b, w.address()).await?;
            let state = PingPongState::initialize(bal_a, bal_b);
            encoders.push((w.label().to_string(), enc, state));
        }

        let schedule = Arc::new(
            Schedule::new(SchedulerConfig {
                start_ms: cfg.timing.start_ms,
                end_ms: cfg.timing.end_ms,
                step_ms: cfg.timing.step_ms,
                samples_per_wallet_per_slot: cfg.timing.samples_per_wallet_per_slot,
            })
            .context("schedule")?,
        );

        let ctx = EngineContext {
            cfg: cfg.clone(),
            http,
            send_http,
            bundle_http,
            wallets: Arc::new(wallets),
            encoders: Arc::new(encoders),
            schedule,
            gas_limits: Arc::new(outcome.gas_limits),
            output_dir,
        };

        engine::run(ctx).await
    })
}
