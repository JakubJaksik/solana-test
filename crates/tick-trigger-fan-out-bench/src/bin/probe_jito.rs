//! Diagnostic: subscribe to Jito's `SubscribeBundleResults` BEFORE sending,
//! then submit one minimal fresh-blockhash bundle and print the real rejection
//! reason streamed back.

use anyhow::{Context, Result};
use clap::Parser;
use serde_json::{json, Value};
use solana_client::rpc_client::RpcClient;
use solana_commitment_config::CommitmentConfig;
use solana_sdk::signer::Signer;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tick_trigger_fan_out_bench::config::{Config, SenderKind};
use tick_trigger_fan_out_bench::tip_accounts::{tip_accounts_for, TipAccountRotator};
use tick_trigger_fan_out_bench::tx_builder::{self, BuildParams};
use tick_trigger_fan_out_bench::wallet;

use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::Request;

use tick_trigger_fan_out_bench::senders::jito::proto::searcher::searcher_service_client::SearcherServiceClient;
use tick_trigger_fan_out_bench::senders::jito::proto::searcher::{
    SendBundleRequest as PbSendBundleRequest, SubscribeBundleResultsRequest,
    NextScheduledLeaderRequest,
};
use tick_trigger_fan_out_bench::senders::jito::proto::bundle::{Bundle as PbBundle, BundleResult};
use tick_trigger_fan_out_bench::senders::jito::proto::packet::Packet as PbPacket;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    config: PathBuf,
    #[arg(long, default_value = "frankfurt")]
    region: String,
    #[arg(long, default_value_t = 200_000_u64)]
    tip_lamports: u64,
    #[arg(long)]
    rpc_url: Option<String>,
    #[arg(long, default_value_t = 60)]
    wait_secs: u64,
    /// (legacy) Raw value to send as `x-jito-auth` metadata. Almost certainly
    /// NOT what Jito wants — use `--auth-keypair` instead for the real
    /// challenge-response gRPC flow.
    #[arg(long)]
    jito_auth: Option<String>,
    /// Path to the keypair JSON for the pubkey registered with Jito (i.e.
    /// you sent Jito the corresponding public key). The auth flow signs a
    /// challenge with this keypair to obtain an access token, then sends
    /// `authorization: Bearer <token>` on all SearcherService calls.
    #[arg(long)]
    auth_keypair: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();
    let cfg = Config::load(&args.config)?;
    let rpc_url = args.rpc_url.clone().unwrap_or_else(|| cfg.rpc.url.clone());

    let keypair = Arc::new(wallet::load_keypair(&cfg.wallet.keypair_path)?);
    println!("payer: {}", keypair.pubkey());
    println!("tip lamports: {}", args.tip_lamports);

    let rotator = TipAccountRotator::new(tip_accounts_for(SenderKind::Jito).to_vec());
    let tip_account = rotator.next().ok_or_else(|| anyhow::anyhow!("no jito tip accounts"))?;
    println!("tip account: {}", tip_account);

    let rpc = RpcClient::new_with_commitment(rpc_url.clone(), CommitmentConfig::confirmed());
    let fresh_bh = rpc.get_latest_blockhash().context("get_latest_blockhash")?;
    println!("fresh blockhash: {}", fresh_bh);

    let built = tx_builder::build(BuildParams {
        payer: &keypair,
        blockhash: fresh_bh,
        sender_id: 99,
        trigger_id: 0xDEADBEEFu64,
        tip_account: Some(tip_account),
        tip_lamports: args.tip_lamports,
        nonce: None,
        tx_cfg: &cfg.tx,
        fund_tipper: None,
    });
    println!("tx signature: {} ({} ixs)", built.signature, built.tx.message.instructions.len());

    let host = format!("{}.mainnet.block-engine.jito.wtf", args.region);
    let endpoint_url = format!("https://{}:443", host);
    println!("\nconnecting to: {}", endpoint_url);
    let channel: Channel = Endpoint::from_shared(endpoint_url)?
        .tls_config(ClientTlsConfig::new().domain_name(host.clone()).with_native_roots())?
        .timeout(Duration::from_secs(10))
        .connect()
        .await
        .context("connect to Jito gRPC")?;

    let mut client = SearcherServiceClient::new(channel);

    // Resolve auth: prefer challenge-response with --auth-keypair, fall back
    // to legacy raw x-jito-auth header if --jito-auth provided (almost
    // certainly won't work but kept as a sanity comparison).
    let bearer_token: Option<String> = if let Some(kp_path) = &args.auth_keypair {
        let auth_kp = wallet::load_keypair(kp_path)
            .with_context(|| format!("load auth keypair {:?}", kp_path))?;
        println!("auth keypair pubkey: {}", auth_kp.pubkey());
        let host_for_auth = host.clone();
        let token = tick_trigger_fan_out_bench::senders::jito::auth::obtain_access_token(
            &format!("https://{}:443", host_for_auth),
            &host_for_auth,
            &auth_kp,
        )
        .await
        .context("obtain Jito access token")?;
        println!("auth: obtained Bearer token (len={})", token.len());
        Some(token)
    } else {
        None
    };

    fn req_with_auth<M>(
        body: M,
        bearer: Option<&String>,
        legacy_xauth: Option<&String>,
    ) -> Request<M> {
        let mut req = Request::new(body);
        if let Some(token) = bearer {
            let val: tonic::metadata::MetadataValue<_> = format!("Bearer {}", token)
                .parse()
                .expect("invalid authorization value");
            req.metadata_mut().insert("authorization", val);
        }
        if let Some(a) = legacy_xauth {
            let val: tonic::metadata::MetadataValue<_> =
                a.parse().expect("invalid x-jito-auth value");
            req.metadata_mut().insert("x-jito-auth", val);
        }
        req
    }

    println!("subscribing to SubscribeBundleResults...");
    let mut stream = client
        .subscribe_bundle_results(req_with_auth(
            SubscribeBundleResultsRequest {},
            bearer_token.as_ref(),
            args.jito_auth.as_ref(),
        ))
        .await
        .context("subscribe_bundle_results")?
        .into_inner();
    println!("subscription open. sending bundle...\n");

    let raw_bytes = bincode::serialize(&built.tx).unwrap();
    let pkt = PbPacket { data: raw_bytes, meta: None };
    let bundle = PbBundle { header: None, packets: vec![pkt] };
    let send_resp = client
        .send_bundle(req_with_auth(
            PbSendBundleRequest { bundle: Some(bundle) },
            bearer_token.as_ref(),
            args.jito_auth.as_ref(),
        ))
        .await
        .context("send_bundle")?
        .into_inner();
    let bundle_uuid = send_resp.uuid;
    println!("✓ Jito accepted, bundle_uuid: {}", bundle_uuid);
    println!("\nstreaming bundle results (up to {}s)...\n", args.wait_secs);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(args.wait_secs);
    let mut got_result = false;
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() { break; }
        match tokio::time::timeout(remaining, stream.message()).await {
            Ok(Ok(Some(result))) => {
                print_result(&result, &bundle_uuid);
                if result.bundle_id == bundle_uuid {
                    got_result = true;
                    if is_terminal(&result) { break; }
                }
            }
            Ok(Ok(None)) => { println!("[stream] closed by server"); break; }
            Ok(Err(e)) => { println!("[stream] error: {}", e); break; }
            Err(_) => break,
        }
    }
    if !got_result {
        println!("\n⚠ Never received a BundleResult for our bundle_id={}", bundle_uuid);
        println!("  Strong signal: Jito didn't track / forward our bundle.");
    }

    let next_leader = client
        .get_next_scheduled_leader(req_with_auth(
            NextScheduledLeaderRequest { regions: vec![] },
            bearer_token.as_ref(),
            args.jito_auth.as_ref(),
        ))
        .await
        .ok();
    if let Some(resp) = next_leader {
        let r = resp.into_inner();
        println!(
            "\nnext scheduled Jito leader: slot={} identity={} region={} (current_slot={})",
            r.next_leader_slot, r.next_leader_identity, r.next_leader_region, r.current_slot,
        );
    }

    println!("\n--- For comparison: getInflightBundleStatuses ---");
    let client_http = reqwest::Client::new();
    let body = json!({
        "jsonrpc":"2.0","id":1,"method":"getInflightBundleStatuses","params":[[bundle_uuid.clone()]]
    });
    let r = client_http
        .post("https://mainnet.block-engine.jito.wtf/api/v1/getInflightBundleStatuses")
        .header("Content-Type", "application/json")
        .body(body.to_string())
        .send()
        .await?;
    let v: Value = r.json().await?;
    println!("{}", serde_json::to_string_pretty(&v)?);

    Ok(())
}

fn is_terminal(r: &BundleResult) -> bool {
    use tick_trigger_fan_out_bench::senders::jito::proto::bundle::bundle_result::Result as R;
    matches!(
        r.result,
        Some(R::Rejected(_)) | Some(R::Finalized(_)) | Some(R::Processed(_)) | Some(R::Dropped(_))
    )
}

fn print_result(r: &BundleResult, our_uuid: &str) {
    use tick_trigger_fan_out_bench::senders::jito::proto::bundle::bundle_result::Result as R;
    use tick_trigger_fan_out_bench::senders::jito::proto::bundle::rejected::Reason;
    let ours = if r.bundle_id == our_uuid { " *** OUR BUNDLE ***" } else { "" };
    let stamp = chrono::Utc::now().format("%H:%M:%S%.3f");
    let short = &r.bundle_id[..16.min(r.bundle_id.len())];
    match &r.result {
        Some(R::Accepted(a)) => println!(
            "[{}] {} ACCEPTED slot={} validator={}{}",
            stamp, short, a.slot, a.validator_identity, ours
        ),
        Some(R::Rejected(rej)) => {
            let reason = match &rej.reason {
                Some(Reason::StateAuctionBidRejected(x)) => format!(
                    "state_auction_bid_rejected (auction_id={}, bid={}, msg={:?})",
                    x.auction_id, x.simulated_bid_lamports, x.msg
                ),
                Some(Reason::WinningBatchBidRejected(x)) => format!(
                    "winning_batch_bid_rejected (auction_id={}, bid={}, msg={:?})",
                    x.auction_id, x.simulated_bid_lamports, x.msg
                ),
                Some(Reason::SimulationFailure(x)) => format!(
                    "simulation_failure (tx_sig={}, msg={:?})", x.tx_signature, x.msg
                ),
                Some(Reason::InternalError(x)) => format!("internal_error (msg={})", x.msg),
                Some(Reason::DroppedBundle(x)) => format!("dropped_bundle (msg={})", x.msg),
                None => "unknown rejection".into(),
            };
            println!("[{}] {} REJECTED: {}{}", stamp, short, reason, ours);
        }
        Some(R::Finalized(_)) => println!("[{}] {} FINALIZED{}", stamp, short, ours),
        Some(R::Processed(p)) => println!(
            "[{}] {} PROCESSED slot={} validator={} idx={}{}",
            stamp, short, p.slot, p.validator_identity, p.bundle_index, ours
        ),
        Some(R::Dropped(d)) => println!("[{}] {} DROPPED reason={:?}{}", stamp, short, d.reason, ours),
        None => println!("[{}] {} (empty result){}", stamp, short, ours),
    }
}
