use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tx_cutoff::time::{hybrid_sleep_until, now_unix_ms, target_instant_from_unix_ms};

#[tokio::test]
async fn hybrid_sleep_wakes_at_or_after_target() {
    let target = Instant::now() + Duration::from_millis(20);
    hybrid_sleep_until(target).await;
    let awoke = Instant::now();
    assert!(
        awoke >= target,
        "awoke at {:?}, target was {:?}",
        awoke,
        target
    );
}

#[tokio::test]
async fn hybrid_sleep_wake_jitter_below_one_ms() {
    // Repeat 20x to smooth out noise; p95 < 1 ms on idle runtime
    let mut max_jitter_us: u128 = 0;
    for _ in 0..20 {
        let target = Instant::now() + Duration::from_millis(10);
        hybrid_sleep_until(target).await;
        let jitter = Instant::now().saturating_duration_since(target).as_micros();
        max_jitter_us = max_jitter_us.max(jitter);
    }
    assert!(
        max_jitter_us < 1000,
        "max wake jitter was {} us",
        max_jitter_us
    );
}

#[tokio::test]
async fn hybrid_sleep_returns_immediately_for_past_target() {
    let target = Instant::now() - Duration::from_millis(100);
    let before = Instant::now();
    hybrid_sleep_until(target).await;
    let elapsed = before.elapsed();
    assert!(
        elapsed < Duration::from_millis(2),
        "unexpected delay: {:?}",
        elapsed
    );
}

#[test]
fn now_unix_ms_is_close_to_system_time() {
    let ours = now_unix_ms();
    let expected = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    assert!(
        (ours - expected).abs() < 50,
        "delta {} ms",
        (ours - expected).abs()
    );
}

#[test]
fn target_instant_computes_correct_offset() {
    let now_instant = Instant::now();
    let now_ms = now_unix_ms();
    let target_unix_ms = now_ms + 100;
    let target = target_instant_from_unix_ms(now_instant, now_ms, target_unix_ms);
    let delta = target.saturating_duration_since(now_instant);
    assert!(
        delta >= Duration::from_millis(95) && delta <= Duration::from_millis(105),
        "delta was {:?}",
        delta
    );
}

#[test]
fn target_instant_past_returns_same_instant() {
    let now_instant = Instant::now();
    let now_ms = now_unix_ms();
    let target_unix_ms = now_ms - 500;
    let target = target_instant_from_unix_ms(now_instant, now_ms, target_unix_ms);
    assert_eq!(target, now_instant, "past target should clamp to now");
}
