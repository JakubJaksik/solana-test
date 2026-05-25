//! Jito Block Engine searcher authentication.
//!
//! Implements the 2-step challenge-response gRPC flow against `AuthService`:
//!   1. `GenerateAuthChallenge(role=Searcher, pubkey)` → returns `challenge`.
//!   2. Sign `"{pubkey_base58}-{challenge}"` with the searcher keypair (ed25519).
//!   3. `GenerateAuthTokens(challenge, pubkey, signed_bytes)` → returns
//!      `(access_token, refresh_token)`. The access token is a JWT-ish opaque
//!      string with an expiration; subsequent gRPC calls must include it as
//!      `authorization: Bearer <access_token>` metadata.
//!
//! This module performs steps 1-3 once and returns the access token string.
//! Token refresh (`RefreshAccessToken`) is not yet wired here — for the
//! diagnostic probe we issue a fresh challenge each invocation.

use super::proto::auth::auth_service_client::AuthServiceClient;
use super::proto::auth::{
    GenerateAuthChallengeRequest, GenerateAuthTokensRequest, Role,
};
use anyhow::{Context, Result};
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use std::net::IpAddr;
use std::time::Duration;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

/// Connect to a Jito region's gRPC, run challenge-response, return the
/// access token string ready to be embedded in `authorization: Bearer <...>`
/// metadata on subsequent `SearcherService` requests.
///
/// `endpoint_url` must be the full `https://<host>:443` form.
///
/// `local_address` pins the outbound TCP socket to a specific local IP. On
/// AWS where NAT rotates EIPs per new TCP connection, this is CRITICAL —
/// without it the two gRPC calls (challenge + tokens) may egress from
/// different public IPs, hit different backend instances of Jito's auth
/// service (each with its own challenge cache), and the tokens call returns
/// "challenge not found".
pub async fn obtain_access_token(
    endpoint_url: &str,
    host: &str,
    keypair: &Keypair,
    local_address: Option<IpAddr>,
) -> Result<String> {
    let tls = ClientTlsConfig::new().domain_name(host).with_native_roots();
    let mut ep = Endpoint::from_shared(endpoint_url.to_string())?
        .tls_config(tls)?
        .timeout(Duration::from_secs(10))
        .tcp_keepalive(Some(Duration::from_secs(30)))
        .http2_keep_alive_interval(Duration::from_secs(20));
    if let Some(addr) = local_address {
        ep = ep.local_address(Some(addr));
        eprintln!("auth: bound to local IP {}", addr);
    }
    let channel: Channel = ep
        .connect()
        .await
        .with_context(|| format!("connect to Jito AuthService at {endpoint_url}"))?;

    let mut client = AuthServiceClient::new(channel);

    // Step 1: generate challenge.
    let pubkey = keypair.pubkey();
    let pubkey_bytes = pubkey.to_bytes().to_vec();
    eprintln!(
        "auth: GenerateAuthChallenge(role=Searcher, pubkey={} [{} bytes])",
        pubkey,
        pubkey_bytes.len()
    );
    let challenge_resp = client
        .generate_auth_challenge(GenerateAuthChallengeRequest {
            role: Role::Searcher as i32,
            pubkey: pubkey_bytes.clone(),
        })
        .await
        .context("generate_auth_challenge")?
        .into_inner();
    let challenge = challenge_resp.challenge;
    eprintln!(
        "auth: challenge received (len={}): {:?}",
        challenge.len(),
        challenge
    );

    // Step 2: sign "{pubkey_base58}-{challenge}".
    let message = format!("{}-{}", pubkey, challenge);
    eprintln!("auth: signing message (len={}): {:?}", message.len(), message);
    let signed = keypair.sign_message(message.as_bytes());
    let signed_bytes = signed.as_ref().to_vec();
    eprintln!("auth: signature ({} bytes)", signed_bytes.len());

    // Step 3: exchange signature for tokens. Retry up to 3 times because
    // Envoy in front of Jito's auth service may load-balance requests
    // across backend instances, and the challenge cache is per-instance.
    // Each retry has a fresh chance of landing on the instance that issued
    // our challenge.
    eprintln!("auth: GenerateAuthTokens(...)");
    let mut last_err: Option<tonic::Status> = None;
    let mut tokens_resp = None;
    for attempt in 1..=3 {
        let result = client
            .generate_auth_tokens(GenerateAuthTokensRequest {
                challenge: challenge.clone(),
                client_pubkey: pubkey_bytes.clone(),
                signed_challenge: signed_bytes.clone(),
            })
            .await;
        match result {
            Ok(r) => {
                tokens_resp = Some(r.into_inner());
                break;
            }
            Err(s) => {
                eprintln!(
                    "auth: GenerateAuthTokens attempt {}/3 failed: {} {}",
                    attempt,
                    s.code(),
                    s.message()
                );
                last_err = Some(s);
                if attempt < 3 {
                    tokio::time::sleep(Duration::from_millis(150)).await;
                }
            }
        }
    }
    let tokens_resp = match tokens_resp {
        Some(t) => t,
        None => {
            let err = last_err.unwrap();
            return Err(anyhow::anyhow!("generate_auth_tokens: {}", err));
        }
    };

    let access_token = tokens_resp
        .access_token
        .context("no access_token in response")?
        .value;
    Ok(access_token)
}
