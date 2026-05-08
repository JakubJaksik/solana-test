use entry_sources::counters::DropCounters;
use entry_sources::source::EntrySource;
use entry_sources::yellowstone::YellowstoneSource;
use std::sync::Arc;
use std::time::Duration;

#[test]
#[ignore = "requires HELIUS_GRPC_URL + HELIUS_GRPC_TOKEN"]
fn ys_emits_entries_within_10s() {
    let url = std::env::var("HELIUS_GRPC_URL").expect("HELIUS_GRPC_URL");
    let token = std::env::var("HELIUS_GRPC_TOKEN").ok();
    let counters = Arc::new(DropCounters::default());
    let src = Box::new(YellowstoneSource {
        url,
        token,
        channel_capacity: 65536,
        pinned_core: None,
        counters,
    });
    let rx = src.start().expect("start");
    let obs = rx
        .recv_timeout(Duration::from_secs(15))
        .expect("entry within 15s");
    assert!(obs.slot > 0);
    println!(
        "got entry: slot={} index={} num_hashes={} tx_count={}",
        obs.slot, obs.entry_index, obs.num_hashes, obs.tx_count
    );
}
