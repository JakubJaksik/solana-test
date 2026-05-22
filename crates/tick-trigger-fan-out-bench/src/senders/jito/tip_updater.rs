//! Background poller for Jito's tip floor API.
//!
//! Every `refresh_interval` GETs `https://bundles.jito.wtf/api/v1/bundles/tip_floor`,
//! log-interpolates the configured percentile, clamps to `[floor, ceiling]`,
//! and stores the result in an `AtomicU64` shared with the sender.
//! On network error, retains the previous value.

use serde::Deserialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

pub const JITO_TIP_AMOUNTS_URL: &str = "https://bundles.jito.wtf/api/v1/bundles/tip_floor";

#[derive(Debug, Deserialize, Clone)]
pub struct TipAmounts {
    pub landed_tips_25th_percentile: f64,
    pub landed_tips_50th_percentile: f64,
    pub landed_tips_75th_percentile: f64,
    pub landed_tips_95th_percentile: f64,
    pub landed_tips_99th_percentile: f64,
}

pub struct JitoTipUpdater {
    pub current_lamports: Arc<AtomicU64>,
    pub percentile: u32,
    pub floor_lamports: u64,
    pub ceiling_lamports: u64,
    pub refresh_interval: Duration,
}

impl JitoTipUpdater {
    pub fn new(percentile: u32, floor_lamports: u64, ceiling_lamports: u64, refresh_interval_ms: u64) -> Self {
        Self {
            current_lamports: Arc::new(AtomicU64::new(floor_lamports)),
            percentile,
            floor_lamports,
            ceiling_lamports,
            refresh_interval: Duration::from_millis(refresh_interval_ms),
        }
    }

    /// Spawn the background poller on the given Tokio runtime handle.
    /// Returns immediately. Loop exits when `stop` flips to true.
    pub fn spawn(
        self,
        handle: &tokio::runtime::Handle,
        stop: Arc<std::sync::atomic::AtomicBool>,
    ) -> tokio::task::JoinHandle<()> {
        let current = self.current_lamports.clone();
        let percentile = self.percentile;
        let floor = self.floor_lamports;
        let ceiling = self.ceiling_lamports;
        let interval = self.refresh_interval;
        handle.spawn(async move {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("reqwest client");
            loop {
                if stop.load(Ordering::Relaxed) { break; }
                match fetch_and_compute(&client, percentile, floor, ceiling).await {
                    Ok(v) => {
                        let prev = current.swap(v, Ordering::Relaxed);
                        if prev != v {
                            tracing::info!(prev_lamports = prev, new_lamports = v, percentile, "jito tip floor updated");
                        }
                    }
                    Err(e) => tracing::warn!(error = %e, "jito tip floor fetch failed; retaining previous value"),
                }
                tokio::time::sleep(interval).await;
            }
        })
    }
}

async fn fetch_and_compute(
    client: &reqwest::Client,
    percentile: u32,
    floor_lamports: u64,
    ceiling_lamports: u64,
) -> Result<u64, String> {
    let raw: Vec<TipAmounts> = client
        .get(JITO_TIP_AMOUNTS_URL)
        .send()
        .await
        .map_err(|e| e.to_string())?
        .json()
        .await
        .map_err(|e| e.to_string())?;
    let amounts = raw.into_iter().next().ok_or("empty tip floor response")?;
    let tip_sol = log_interpolate_percentile(&amounts, percentile as f64);
    let tip_lamports = (tip_sol * 1_000_000_000.0) as u64;
    Ok(tip_lamports.clamp(floor_lamports, ceiling_lamports))
}

/// Log-linear interpolation across the 5 percentile points. Mirrors the
/// `JitoTipUpdater.findTipValueForPercentile` algorithm in dex-trader.
pub fn log_interpolate_percentile(amounts: &TipAmounts, target_percentile: f64) -> f64 {
    let points = [
        (25.0_f64, amounts.landed_tips_25th_percentile.ln()),
        (50.0, amounts.landed_tips_50th_percentile.ln()),
        (75.0, amounts.landed_tips_75th_percentile.ln()),
        (95.0, amounts.landed_tips_95th_percentile.ln()),
        (99.0, amounts.landed_tips_99th_percentile.ln()),
    ];
    let (lower, upper) = surrounding_pair(&points, target_percentile);
    let log_tip = lower.1 + ((target_percentile - lower.0) * (upper.1 - lower.1)) / (upper.0 - lower.0);
    log_tip.exp()
}

fn surrounding_pair(points: &[(f64, f64)], target: f64) -> ((f64, f64), (f64, f64)) {
    for i in 0..points.len() - 1 {
        if target >= points[i].0 && target <= points[i + 1].0 {
            return (points[i], points[i + 1]);
        }
    }
    if target < points[0].0 { return (points[0], points[1]); }
    let last = points.len() - 1;
    (points[last - 1], points[last])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn amounts() -> TipAmounts {
        TipAmounts {
            landed_tips_25th_percentile: 0.000_010,
            landed_tips_50th_percentile: 0.000_025,
            landed_tips_75th_percentile: 0.000_060,
            landed_tips_95th_percentile: 0.000_500,
            landed_tips_99th_percentile: 0.002_000,
        }
    }

    #[test]
    fn percentile_at_known_point_returns_known_value() {
        let v = log_interpolate_percentile(&amounts(), 50.0);
        let lamports = (v * 1_000_000_000.0) as u64;
        assert!(lamports >= 24_900 && lamports <= 25_100, "50th percentile must be ~25k, got {}", lamports);
    }

    #[test]
    fn percentile_below_25_clamps_to_lower_bracket() {
        let v = log_interpolate_percentile(&amounts(), 10.0);
        assert!(v > 0.0);
    }

    #[test]
    fn percentile_above_99_clamps_to_upper_bracket() {
        let v = log_interpolate_percentile(&amounts(), 100.0);
        assert!(v > 0.0);
    }

    #[test]
    fn updater_stores_floor_on_init() {
        let u = JitoTipUpdater::new(75, 15_000, 2_000_000, 30_000);
        assert_eq!(u.current_lamports.load(Ordering::Relaxed), 15_000);
    }

    #[test]
    fn updater_holds_correct_config_values() {
        let u = JitoTipUpdater::new(75, 15_000, 2_000_000, 30_000);
        assert_eq!(u.percentile, 75);
        assert_eq!(u.floor_lamports, 15_000);
        assert_eq!(u.ceiling_lamports, 2_000_000);
        assert_eq!(u.refresh_interval, Duration::from_millis(30_000));
    }
}
