use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{
    ArrayBuilder, ArrayRef, BooleanBuilder, FixedSizeBinaryBuilder, ListBuilder, StringBuilder,
    UInt32Builder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use crossbeam_channel::{Receiver, RecvTimeoutError};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use tracing::error;

use crate::diff_record::DiffRecord;

pub struct WriterConfig {
    pub diff_rx: Receiver<DiffRecord>,
    pub output_path: PathBuf,
    pub row_group_size: usize,
    pub flush_interval: Duration,
    pub pinned_core: Option<usize>,
}

pub fn spawn(cfg: WriterConfig) -> std::io::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new()
        .name("writer".into())
        .spawn(move || {
            if let Some(c) = cfg.pinned_core {
                core_affinity::set_for_current(core_affinity::CoreId { id: c });
            }
            run_loop(cfg);
        })
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("slot", DataType::UInt64, false),
        Field::new("entry_index", DataType::UInt32, false),
        Field::new("num_hashes", DataType::UInt64, false),
        Field::new("source", DataType::Utf8, false),
        Field::new("ys_observed_ns", DataType::UInt64, true),
        Field::new("ss_first_shred_ns", DataType::UInt64, true),
        Field::new("ss_fec_complete_ns", DataType::UInt64, true),
        Field::new("ys_hash", DataType::FixedSizeBinary(32), true),
        Field::new("ss_hash", DataType::FixedSizeBinary(32), true),
        Field::new("ys_tx_count", DataType::UInt32, true),
        Field::new("ss_tx_count", DataType::UInt32, true),
        Field::new("hash_match", DataType::Boolean, false),
        Field::new("sig_set_match", DataType::Boolean, true),
        Field::new("leader_pubkey", DataType::FixedSizeBinary(32), true),
        Field::new(
            "ys_signatures",
            DataType::List(Arc::new(Field::new("item", DataType::FixedSizeBinary(64), true))),
            true,
        ),
        Field::new(
            "ss_signatures",
            DataType::List(Arc::new(Field::new("item", DataType::FixedSizeBinary(64), true))),
            true,
        ),
    ]))
}

struct Builders {
    cap: usize,
    slot: UInt64Builder,
    entry_index: UInt32Builder,
    num_hashes: UInt64Builder,
    source: StringBuilder,
    ys_observed_ns: UInt64Builder,
    ss_first_shred_ns: UInt64Builder,
    ss_fec_complete_ns: UInt64Builder,
    ys_hash: FixedSizeBinaryBuilder,
    ss_hash: FixedSizeBinaryBuilder,
    ys_tx_count: UInt32Builder,
    ss_tx_count: UInt32Builder,
    hash_match: BooleanBuilder,
    sig_set_match: BooleanBuilder,
    leader_pubkey: FixedSizeBinaryBuilder,
    ys_signatures: ListBuilder<FixedSizeBinaryBuilder>,
    ss_signatures: ListBuilder<FixedSizeBinaryBuilder>,
}

impl Builders {
    fn new(cap: usize) -> Self {
        Self {
            cap,
            slot: UInt64Builder::with_capacity(cap),
            entry_index: UInt32Builder::with_capacity(cap),
            num_hashes: UInt64Builder::with_capacity(cap),
            source: StringBuilder::with_capacity(cap, cap * 8),
            ys_observed_ns: UInt64Builder::with_capacity(cap),
            ss_first_shred_ns: UInt64Builder::with_capacity(cap),
            ss_fec_complete_ns: UInt64Builder::with_capacity(cap),
            ys_hash: FixedSizeBinaryBuilder::with_capacity(cap, 32),
            ss_hash: FixedSizeBinaryBuilder::with_capacity(cap, 32),
            ys_tx_count: UInt32Builder::with_capacity(cap),
            ss_tx_count: UInt32Builder::with_capacity(cap),
            hash_match: BooleanBuilder::with_capacity(cap),
            sig_set_match: BooleanBuilder::with_capacity(cap),
            leader_pubkey: FixedSizeBinaryBuilder::with_capacity(cap, 32),
            ys_signatures: ListBuilder::new(FixedSizeBinaryBuilder::with_capacity(cap * 4, 64)),
            ss_signatures: ListBuilder::new(FixedSizeBinaryBuilder::with_capacity(cap * 4, 64)),
        }
    }

    fn append(&mut self, r: &DiffRecord) {
        self.slot.append_value(r.slot);
        self.entry_index.append_value(r.entry_index);
        self.num_hashes.append_value(r.num_hashes);
        self.source.append_value(r.source.as_str());

        match r.ys_observed_ns {
            Some(v) => self.ys_observed_ns.append_value(v),
            None => self.ys_observed_ns.append_null(),
        }
        match r.ss_first_shred_ns {
            Some(v) => self.ss_first_shred_ns.append_value(v),
            None => self.ss_first_shred_ns.append_null(),
        }
        match r.ss_fec_complete_ns {
            Some(v) => self.ss_fec_complete_ns.append_value(v),
            None => self.ss_fec_complete_ns.append_null(),
        }

        match &r.ys_hash {
            Some(b) => self.ys_hash.append_value(b).unwrap(),
            None => self.ys_hash.append_null(),
        }
        match &r.ss_hash {
            Some(b) => self.ss_hash.append_value(b).unwrap(),
            None => self.ss_hash.append_null(),
        }

        match r.ys_tx_count {
            Some(v) => self.ys_tx_count.append_value(v),
            None => self.ys_tx_count.append_null(),
        }
        match r.ss_tx_count {
            Some(v) => self.ss_tx_count.append_value(v),
            None => self.ss_tx_count.append_null(),
        }

        self.hash_match.append_value(r.hash_match);
        match r.sig_set_match {
            Some(b) => self.sig_set_match.append_value(b),
            None => self.sig_set_match.append_null(),
        }
        match &r.leader_pubkey {
            Some(b) => self.leader_pubkey.append_value(b).unwrap(),
            None => self.leader_pubkey.append_null(),
        }

        // Lists: append each signature into the inner builder, then call
        // list.append(true) to close the row's list.
        let inner_ys = self.ys_signatures.values();
        for sig in &r.ys_signatures {
            inner_ys.append_value(sig.as_ref()).unwrap();
        }
        self.ys_signatures.append(true);

        let inner_ss = self.ss_signatures.values();
        for sig in &r.ss_signatures {
            inner_ss.append_value(sig.as_ref()).unwrap();
        }
        self.ss_signatures.append(true);
    }

    fn len(&self) -> usize {
        self.slot.len()
    }

    fn finish_batch(&mut self, schema: &Arc<Schema>) -> RecordBatch {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(self.slot.finish()),
            Arc::new(self.entry_index.finish()),
            Arc::new(self.num_hashes.finish()),
            Arc::new(self.source.finish()),
            Arc::new(self.ys_observed_ns.finish()),
            Arc::new(self.ss_first_shred_ns.finish()),
            Arc::new(self.ss_fec_complete_ns.finish()),
            Arc::new(self.ys_hash.finish()),
            Arc::new(self.ss_hash.finish()),
            Arc::new(self.ys_tx_count.finish()),
            Arc::new(self.ss_tx_count.finish()),
            Arc::new(self.hash_match.finish()),
            Arc::new(self.sig_set_match.finish()),
            Arc::new(self.leader_pubkey.finish()),
            Arc::new(self.ys_signatures.finish()),
            Arc::new(self.ss_signatures.finish()),
        ];
        RecordBatch::try_new(schema.clone(), arrays).expect("record batch")
    }
}

fn run_loop(cfg: WriterConfig) {
    let file = match File::create(&cfg.output_path) {
        Ok(f) => f,
        Err(e) => {
            error!(error = %e, "open parquet output");
            return;
        }
    };
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .set_max_row_group_size(cfg.row_group_size)
        .build();
    let schema = schema();
    let mut writer = match ArrowWriter::try_new(file, schema.clone(), Some(props)) {
        Ok(w) => w,
        Err(e) => {
            error!(error = %e, "init parquet writer");
            return;
        }
    };
    let mut builders = Builders::new(cfg.row_group_size);
    let mut last_flush = Instant::now();

    loop {
        match cfg.diff_rx.recv_timeout(cfg.flush_interval) {
            Ok(rec) => {
                builders.append(&rec);
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
                if let Err(e) = writer.close() {
                    error!(error = %e, "parquet close");
                }
                return;
            }
        }
        if builders.len() > 0 && last_flush.elapsed() > cfg.flush_interval {
            flush(&mut writer, &mut builders, &schema);
            last_flush = Instant::now();
        }
    }
}

fn flush(writer: &mut ArrowWriter<File>, b: &mut Builders, schema: &Arc<Schema>) {
    let batch = b.finish_batch(schema);
    if let Err(e) = writer.write(&batch) {
        error!(error = %e, "parquet write");
    }
    *b = Builders::new(b.cap);
}
