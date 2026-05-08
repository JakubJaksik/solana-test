//! Monotonic clock helpers + hybrid sleep (tokio + busy-wait).

use std::hint::spin_loop;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Rozmiar okna busy-wait. 2 ms jest kompromisem między precyzją
/// (tokio::sleep ma jitter ~0.5-2 ms) a kosztem CPU.
pub const DEFAULT_SPIN_WINDOW: Duration = Duration::from_millis(2);

/// Zwraca bieżący czas unix w milisekundach.
/// Używany do synchronizacji z `block.timestamp` (które jest sekundowe × 1000).
pub fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_millis() as i64
}

/// Oblicza `target_instant` na podstawie docelowego wall-clock (`target_unix_ms`),
/// przy znanym mapowaniu (`ref_instant`, `ref_unix_ms`) z tej samej chwili.
///
/// Jeśli target jest w przeszłości — zwraca `ref_instant` (callable code
/// powinien rozpoznać tę sytuację i skipnąć wysyłkę).
pub fn target_instant_from_unix_ms(
    ref_instant: Instant,
    ref_unix_ms: i64,
    target_unix_ms: i64,
) -> Instant {
    let offset_ms = target_unix_ms - ref_unix_ms;
    if offset_ms <= 0 {
        ref_instant
    } else {
        ref_instant + Duration::from_millis(offset_ms as u64)
    }
}

/// Hybrid sleep: tokio::sleep do `target - spin_window`, potem busy-wait.
/// Gwarantuje `Instant::now() >= target` po powrocie.
pub async fn hybrid_sleep_until(target: Instant) {
    hybrid_sleep_until_with_window(target, DEFAULT_SPIN_WINDOW).await;
}

/// Wariant z konfigurowalnym rozmiarem busy-wait okna.
pub async fn hybrid_sleep_until_with_window(target: Instant, spin_window: Duration) {
    let now = Instant::now();
    if target <= now {
        return;
    }
    if target > now + spin_window {
        let sleep_dur = target - now - spin_window;
        tokio::time::sleep(sleep_dur).await;
    }
    while Instant::now() < target {
        spin_loop();
    }
}
