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
use std::time::Duration;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

/// Connect to a Jito region's gRPC, run challenge-response, return the
/// access token string ready to be embedded in `authorization: Bearer <...>`
/// metadata on subsequent `SearcherService` requests.
///
/// `endpoint_url` must be the full `https://<host>:443` form.
pub async fn obtain_access_token(
    endpoint_url: &str,
    host: &str,
    keypair: &Keypair,
) -> Result<String> {
    let tls = ClientTlsConfig::new().domain_name(host).with_native_roots();
    let channel: Channel = Endpoint::from_shared(endpoint_url.to_string())?
        .tls_config(tls)?
        .timeout(Duration::from_secs(10))
        .connect()
        .await
        .with_context(|| format!("connect to Jito AuthService at {endpoint_url}"))?;

    let mut client = AuthServiceClient::new(channel);

    // Step 1: generate challenge.
    let pubkey = keypair.pubkey();
    let challenge_resp = client
        .generate_auth_challenge(GenerateAuthChallengeRequest {
            role: Role::Searcher as i32,
            pubkey: pubkey.to_bytes().to_vec(),
        })
        .await
        .context("generate_auth_challenge")?
        .into_inner();
    let challenge = challenge_resp.challenge;

    // Step 2: sign "{pubkey_base58}-{challenge}".
    let message = format!("{}-{}", pubkey, challenge);
    let signed = keypair.sign_message(message.as_bytes());

    // Step 3: exchange signature for tokens.
    let tokens_resp = client
        .generate_auth_tokens(GenerateAuthTokensRequest {
            challenge,
            client_pubkey: pubkey.to_bytes().to_vec(),
            signed_challenge: signed.as_ref().to_vec(),
        })
        .await
        .context("generate_auth_tokens")?
        .into_inner();

    let access_token = tokens_resp
        .access_token
        .context("no access_token in response")?
        .value;
    Ok(access_token)
}
