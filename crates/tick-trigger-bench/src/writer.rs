use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{
    ArrayBuilder, ArrayRef, BooleanBuilder, FixedSizeBinaryBuilder, StringBuilder,
    UInt32Builder, UInt64Builder, UInt8Builder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, select};
use dashmap::DashSet;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use solana_sdk::signature::Signature;
use tracing::{error, info};

use crate::counters::BenchCounters;
use crate::leader_cache::LeaderCache;
use crate::observer::MatchEvent;
use crate::rpc_fallback::FallbackQueue;
use crate::sender::SendEvent;

#[derive(Debug)]
struct PendingRecord {
    send: SendEvent,
    match_event: Option<MatchEvent>,
    inserted_at: Instant,
}

#[derive(Debug)]
pub struct FinalRecord {
    pub send: SendEvent,
    pub match_event: Option<MatchEvent>,
    pub status: &'static str,
}

pub struct WriterConfig {
    pub send_event_rx: Receiver<SendEvent>,
    pub match_rx: Receiver<MatchEvent>,
    pub final_tx: Sender<FinalRecord>,
    pub deadline: Duration,
    pub pinned_core: Option<usize>,
    pub pending_sigs: Arc<DashSet<Signature>>,
    pub counters: Arc<BenchCounters>,
    pub fallback_queue: FallbackQueue,
    pub stop: Arc<std::sync::atomic::AtomicBool>,
}

pub fn spawn_finalizer(cfg: WriterConfig) -> std::io::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new()
        .name("writer-finalizer".into())
        .spawn(move || {
            if let Some(c) = cfg.pinned_core {
                core_affinity::set_for_current(core_affinity::CoreId { id: c });
            }
            finalize_loop(cfg);
        })
}

fn emit_final(
    sig: Signature,
    p: PendingRecord,
    cfg: &WriterConfig,
) {
    let status = if p.match_event.is_some() { "OBSERVED" } else { "UNKNOWN_PENDING" };
    cfg.pending_sigs.remove(&sig);
    if status == "UNKNOWN_PENDING" {
        cfg.fallback_queue.push(sig);
    }
    if cfg.final_tx.try_send(FinalRecord {
        send: p.send,
        match_event: p.match_event,
        status,
    }).is_err() {
        cfg.counters.inc(&cfg.counters.final_queue_full);
    }
}

fn finalize_loop(cfg: WriterConfig) {
    use std::sync::atomic::Ordering;
    let mut map: HashMap<Signature, PendingRecord> = HashMap::with_capacity(8192);
    // Buffer match events that arrived before the corresponding send event.
    let mut early_matches: HashMap<Signature, MatchEvent> = HashMap::with_capacity(256);
    let mut last_sweep = Instant::now();
    loop {
        if cfg.stop.load(Ordering::Relaxed) && map.is_empty() {
            break;
        }
        select! {
            recv(cfg.send_event_rx) -> msg => match msg {
                Ok(ev) => {
                    let sig = ev.signature;
                    let match_event = early_matches.remove(&sig);
                    let complete = match_event.is_some();
                    map.entry(sig).or_insert(PendingRecord {
                        send: ev, match_event, inserted_at: Instant::now(),
                    });
                    if complete {
                        let p = map.remove(&sig).unwrap();
                        cfg.pending_sigs.remove(&sig);
                        if cfg.final_tx.try_send(FinalRecord {
                            send: p.send,
                            match_event: p.match_event,
                            status: "OBSERVED",
                        }).is_err() {
                            cfg.counters.inc(&cfg.counters.final_queue_full);
                        }
                    }
                }
                Err(_) => break,
            },
            recv(cfg.match_rx) -> msg => match msg {
                Ok(ev) => {
                    let sig = ev.signature;
                    if let Some(p) = map.get_mut(&sig) {
                        p.match_event = Some(ev);
                        let p = map.remove(&sig).unwrap();
                        cfg.pending_sigs.remove(&sig);
                        if cfg.final_tx.try_send(FinalRecord {
                            send: p.send,
                            match_event: p.match_event,
                            status: "OBSERVED",
                        }).is_err() {
                            cfg.counters.inc(&cfg.counters.final_queue_full);
                        }
                    } else {
                        // Send event not yet seen; buffer the match
                        early_matches.insert(sig, ev);
                    }
                }
                Err(_) => {}
            },
            default(Duration::from_millis(100)) => {}
        }
        let now = Instant::now();
        if now.duration_since(last_sweep) > Duration::from_millis(500) {
            let stale: Vec<Signature> = map.iter()
                .filter(|(_, p)| now.duration_since(p.inserted_at) > cfg.deadline)
                .map(|(s, _)| *s)
                .collect();
            for sig in stale {
                let p = map.remove(&sig).unwrap();
                emit_final(sig, p, &cfg);
            }
            last_sweep = now;
        }
    }
    info!("writer-finalizer exiting; flushing {} pending records", map.len());
    for (sig, p) in map.drain() {
        emit_final(sig, p, &cfg);
    }
}

// ----------------------- Parquet writer -----------------------

pub struct ParquetWriterConfig {
    pub final_rx: Receiver<FinalRecord>,
    pub output_path: PathBuf,
    pub row_group_size: usize,
    pub flush_interval: Duration,
    pub pinned_core: Option<usize>,
    pub leader_cache: Arc<LeaderCache>,
    pub anchor: Instant,
}

pub fn spawn_parquet(cfg: ParquetWriterConfig) -> std::io::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new()
        .name("parquet-writer".into())
        .spawn(move || {
            if let Some(c) = cfg.pinned_core {
                core_affinity::set_for_current(core_affinity::CoreId { id: c });
            }
            parquet_loop(cfg);
        })
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("schedule_slot", DataType::UInt64, false),
        Field::new("schedule_tick", DataType::UInt8, false),
        Field::new("schedule_leader", DataType::FixedSizeBinary(32), true),
        Field::new("tx_signature", DataType::FixedSizeBinary(64), false),
        Field::new("trigger_observed_at_ns", DataType::UInt64, false),
        Field::new("send_at_ns", DataType::UInt64, false),
        Field::new("response_at_ns", DataType::UInt64, false),
        Field::new("send_error", DataType::Utf8, true),
        Field::new("observed_at_ns", DataType::UInt64, true),
        Field::new("observed_slot", DataType::UInt64, true),
        Field::new("observed_entry_index", DataType::UInt32, true),
        Field::new("observed_tick_in_slot", DataType::UInt8, true),
        Field::new("observed_leader", DataType::FixedSizeBinary(32), true),
        Field::new("tick_delta", DataType::UInt32, true),
        Field::new("slot_delta", DataType::UInt32, true),
        Field::new("time_delta_ns", DataType::UInt64, true),
        Field::new("leader_changed", DataType::Boolean, true),
        Field::new("status", DataType::Utf8, false),
    ]))
}

struct Builders {
    cap: usize,
    schedule_slot: UInt64Builder,
    schedule_tick: UInt8Builder,
    schedule_leader: FixedSizeBinaryBuilder,
    tx_signature: FixedSizeBinaryBuilder,
    trigger_observed_at_ns: UInt64Builder,
    send_at_ns: UInt64Builder,
    response_at_ns: UInt64Builder,
    send_error: StringBuilder,
    observed_at_ns: UInt64Builder,
    observed_slot: UInt64Builder,
    observed_entry_index: UInt32Builder,
    observed_tick_in_slot: UInt8Builder,
    observed_leader: FixedSizeBinaryBuilder,
    tick_delta: UInt32Builder,
    slot_delta: UInt32Builder,
    time_delta_ns: UInt64Builder,
    leader_changed: BooleanBuilder,
    status: StringBuilder,
}

impl Builders {
    fn new(cap: usize) -> Self {
        Self {
            cap,
            schedule_slot: UInt64Builder::with_capacity(cap),
            schedule_tick: UInt8Builder::with_capacity(cap),
            schedule_leader: FixedSizeBinaryBuilder::with_capacity(cap, 32),
            tx_signature: FixedSizeBinaryBuilder::with_capacity(cap, 64),
            trigger_observed_at_ns: UInt64Builder::with_capacity(cap),
            send_at_ns: UInt64Builder::with_capacity(cap),
            response_at_ns: UInt64Builder::with_capacity(cap),
            send_error: StringBuilder::with_capacity(cap, cap * 16),
            observed_at_ns: UInt64Builder::with_capacity(cap),
            observed_slot: UInt64Builder::with_capacity(cap),
            observed_entry_index: UInt32Builder::with_capacity(cap),
            observed_tick_in_slot: UInt8Builder::with_capacity(cap),
            observed_leader: FixedSizeBinaryBuilder::with_capacity(cap, 32),
            tick_delta: UInt32Builder::with_capacity(cap),
            slot_delta: UInt32Builder::with_capacity(cap),
            time_delta_ns: UInt64Builder::with_capacity(cap),
            leader_changed: BooleanBuilder::with_capacity(cap),
            status: StringBuilder::with_capacity(cap, cap * 16),
        }
    }

    fn len(&self) -> usize {
        self.schedule_slot.len()
    }

    fn append(&mut self, r: &FinalRecord, leader_cache: &LeaderCache, anchor: Instant) {
        let s = &r.send;
        self.schedule_slot.append_value(s.schedule_slot);
        self.schedule_tick.append_value(s.schedule_tick);
        match leader_cache.lookup(s.schedule_slot) {
            Some(b) => self.schedule_leader.append_value(b).unwrap(),
            None => self.schedule_leader.append_null(),
        }
        self.tx_signature.append_value(s.signature.as_ref()).unwrap();
        self.trigger_observed_at_ns.append_value(
            s.trigger_observed_at.duration_since(anchor).as_nanos() as u64);
        self.send_at_ns.append_value(s.send_at.duration_since(anchor).as_nanos() as u64);
        self.response_at_ns.append_value(s.response_at.duration_since(anchor).as_nanos() as u64);
        match &s.error {
            Some(e) => self.send_error.append_value(e),
            None => self.send_error.append_null(),
        }
        if let Some(m) = &r.match_event {
            self.observed_at_ns.append_value(m.observed_at.duration_since(anchor).as_nanos() as u64);
            self.observed_slot.append_value(m.observed_slot);
            self.observed_entry_index.append_value(m.observed_entry_index);
            match m.observed_tick_in_slot {
                Some(t) => self.observed_tick_in_slot.append_value(t),
                None => self.observed_tick_in_slot.append_null(),
            }
            match leader_cache.lookup(m.observed_slot) {
                Some(b) => self.observed_leader.append_value(b).unwrap(),
                None => self.observed_leader.append_null(),
            }
            let slot_diff = m.observed_slot.saturating_sub(s.schedule_slot);
            let tick_in_target_frame = m.observed_tick_in_slot
                .map(|t| t as u32 + slot_diff as u32 * 64);
            let tick_delta = tick_in_target_frame.map(|t| t.saturating_sub(s.schedule_tick as u32));
            match tick_delta {
                Some(td) => self.tick_delta.append_value(td),
                None => self.tick_delta.append_null(),
            }
            self.slot_delta.append_value(slot_diff as u32);
            let time_delta = m.observed_at.duration_since(s.send_at).as_nanos() as u64;
            self.time_delta_ns.append_value(time_delta);
            let leader_changed = leader_cache.lookup(s.schedule_slot)
                .zip(leader_cache.lookup(m.observed_slot))
                .map(|(a, b)| a != b);
            match leader_changed {
                Some(b) => self.leader_changed.append_value(b),
                None => self.leader_changed.append_null(),
            }
        } else {
            self.observed_at_ns.append_null();
            self.observed_slot.append_null();
            self.observed_entry_index.append_null();
            self.observed_tick_in_slot.append_null();
            self.observed_leader.append_null();
            self.tick_delta.append_null();
            self.slot_delta.append_null();
            self.time_delta_ns.append_null();
            self.leader_changed.append_null();
        }
        self.status.append_value(r.status);
    }

    fn finish(&mut self, schema: &Arc<Schema>) -> RecordBatch {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(self.schedule_slot.finish()),
            Arc::new(self.schedule_tick.finish()),
            Arc::new(self.schedule_leader.finish()),
            Arc::new(self.tx_signature.finish()),
            Arc::new(self.trigger_observed_at_ns.finish()),
            Arc::new(self.send_at_ns.finish()),
            Arc::new(self.response_at_ns.finish()),
            Arc::new(self.send_error.finish()),
            Arc::new(self.observed_at_ns.finish()),
            Arc::new(self.observed_slot.finish()),
            Arc::new(self.observed_entry_index.finish()),
            Arc::new(self.observed_tick_in_slot.finish()),
            Arc::new(self.observed_leader.finish()),
            Arc::new(self.tick_delta.finish()),
            Arc::new(self.slot_delta.finish()),
            Arc::new(self.time_delta_ns.finish()),
            Arc::new(self.leader_changed.finish()),
            Arc::new(self.status.finish()),
        ];
        RecordBatch::try_new(schema.clone(), arrays).expect("record batch")
    }
}

fn parquet_loop(cfg: ParquetWriterConfig) {
    let file = match File::create(&cfg.output_path) {
        Ok(f) => f,
        Err(e) => { error!(error = %e, "open parquet"); return; }
    };
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .set_max_row_group_size(cfg.row_group_size)
        .build();
    let schema = schema();
    let mut writer = match ArrowWriter::try_new(file, schema.clone(), Some(props)) {
        Ok(w) => w,
        Err(e) => { error!(error = %e, "init parquet"); return; }
    };
    let mut builders = Builders::new(cfg.row_group_size);
    let mut last_flush = Instant::now();

    loop {
        match cfg.final_rx.recv_timeout(cfg.flush_interval) {
            Ok(rec) => {
                builders.append(&rec, &cfg.leader_cache, cfg.anchor);
                if builders.len() >= cfg.row_group_size {
                    flush(&mut writer, &mut builders, &schema);
                    last_flush = Instant::now();
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                if builders.len() > 0 {
                    flush(&mut writer, &mut builders, &schema);
                    last_flush = Instant::now();
                }
            }
            Err(RecvTimeoutError::Disconnected) => {
                if builders.len() > 0 {
                    flush(&mut writer, &mut builders, &schema);
                }
                if let Err(e) = writer.close() { error!(error = %e, "parquet close"); }
                return;
            }
        }
        if builders.len() > 0 && last_flush.elapsed() > cfg.flush_interval {
            flush(&mut writer, &mut builders, &schema);
            last_flush = Instant::now();
        }
    }
}

fn flush(w: &mut ArrowWriter<File>, b: &mut Builders, schema: &Arc<Schema>) {
    let batch = b.finish(schema);
    if let Err(e) = w.write(&batch) { error!(error = %e, "parquet write"); }
    *b = Builders::new(b.cap);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::bounded;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::collections::HashMap;
    use std::sync::atomic::AtomicBool;

    fn dummy_send(sig_byte: u8) -> SendEvent {
        let mut sig_bytes = [0u8; 64];
        sig_bytes[0] = sig_byte;
        SendEvent {
            signature: Signature::from(sig_bytes),
            schedule_slot: 100,
            schedule_tick: 5,
            trigger_observed_at: Instant::now(),
            send_at: Instant::now(),
            response_at: Instant::now(),
            error: None,
        }
    }

    fn make_writer_cfg(
        send_rx: crossbeam_channel::Receiver<SendEvent>,
        match_rx: crossbeam_channel::Receiver<MatchEvent>,
        final_tx: crossbeam_channel::Sender<FinalRecord>,
        deadline: Duration,
        stop: Arc<AtomicBool>,
    ) -> WriterConfig {
        WriterConfig {
            send_event_rx: send_rx,
            match_rx,
            final_tx,
            deadline,
            pinned_core: None,
            pending_sigs: Arc::new(DashSet::new()),
            counters: Arc::new(crate::counters::BenchCounters::default()),
            fallback_queue: crate::rpc_fallback::FallbackQueue::default(),
            stop,
        }
    }

    #[test]
    fn finalizer_pairs_send_and_match() {
        let (send_tx, send_rx) = bounded(16);
        let (match_tx, match_rx) = bounded(16);
        let (final_tx, final_rx) = bounded(16);
        let stop = Arc::new(AtomicBool::new(false));

        let handle = spawn_finalizer(make_writer_cfg(
            send_rx, match_rx, final_tx, Duration::from_secs(5), stop.clone(),
        )).unwrap();

        let s = dummy_send(7);
        let sig = s.signature;
        send_tx.send(s).unwrap();
        match_tx.send(MatchEvent {
            signature: sig, observed_at: Instant::now(),
            observed_slot: 101, observed_entry_index: 3,
            observed_tick_in_slot: Some(10),
        }).unwrap();

        let rec = final_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(rec.status, "OBSERVED");
        assert!(rec.match_event.is_some());

        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        drop(send_tx); drop(match_tx);
        let _ = handle.join();
    }

    #[test]
    fn finalizer_emits_pending_after_deadline() {
        let (send_tx, send_rx) = bounded(16);
        let (_match_tx, match_rx) = bounded::<MatchEvent>(16);
        let (final_tx, final_rx) = bounded(16);
        let stop = Arc::new(AtomicBool::new(false));

        let handle = spawn_finalizer(make_writer_cfg(
            send_rx, match_rx, final_tx, Duration::from_millis(300), stop.clone(),
        )).unwrap();

        send_tx.send(dummy_send(9)).unwrap();
        let rec = final_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(rec.status, "UNKNOWN_PENDING");

        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        drop(send_tx);
        let _ = handle.join();
    }

    #[test]
    fn parquet_writer_writes_valid_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.parquet");
        let (tx, rx) = bounded(16);
        let lc = LeaderCache::from_map(HashMap::new());
        let h = spawn_parquet(ParquetWriterConfig {
            final_rx: rx, output_path: path.clone(),
            row_group_size: 4, flush_interval: Duration::from_millis(50),
            pinned_core: None, leader_cache: lc, anchor: Instant::now(),
        }).unwrap();

        tx.send(FinalRecord {
            send: dummy_send(1), match_event: None, status: "UNKNOWN_PENDING",
        }).unwrap();
        drop(tx);
        h.join().unwrap();

        let file = File::open(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap().build().unwrap();
        let total: usize = reader.map(|r| r.unwrap().num_rows()).sum();
        assert_eq!(total, 1);
    }
}
