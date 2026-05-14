//! Parquet output sink + Arrow schema.

pub mod record;
pub mod schema;
pub mod parquet_sink;

pub use record::FinalRecord;
pub use schema::final_record_schema;
pub use parquet_sink::{ParquetWriterConfig, spawn_parquet};
