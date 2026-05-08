use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use solana_sdk::signature::Signature;

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    Both,
    YsOnly,
    SsOnly,
}

impl Source {
    pub fn as_str(self) -> &'static str {
        match self {
            Source::Both => "BOTH",
            Source::YsOnly => "YS_ONLY",
            Source::SsOnly => "SS_ONLY",
        }
    }
}

/// Per-entry comparison record. ns timestamps are offsets from a process-level anchor
/// (`Instant::elapsed_since_anchor`); the wall-clock anchor is logged once in run-meta.json.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DiffRecord {
    pub slot: u64,
    pub entry_index: u32,
    pub num_hashes: u64,
    pub source: Source,

    pub ys_observed_ns: Option<u64>,
    pub ss_first_shred_ns: Option<u64>,
    pub ss_fec_complete_ns: Option<u64>,

    pub ys_hash: Option<[u8; 32]>,
    pub ss_hash: Option<[u8; 32]>,
    pub ys_tx_count: Option<u32>,
    pub ss_tx_count: Option<u32>,

    pub hash_match: bool,
    pub sig_set_match: Option<bool>,

    pub leader_pubkey: Option<[u8; 32]>,

    #[serde(with = "smallvec_serde")]
    pub ys_signatures: SmallVec<[Signature; 8]>,
    #[serde(with = "smallvec_serde")]
    pub ss_signatures: SmallVec<[Signature; 8]>,
}

mod smallvec_serde {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use smallvec::SmallVec;
    use solana_sdk::signature::Signature;

    pub fn serialize<S: Serializer>(
        v: &SmallVec<[Signature; 8]>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        v.as_slice().serialize(s)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<SmallVec<[Signature; 8]>, D::Error> {
        let v: Vec<Signature> = Vec::deserialize(d)?;
        Ok(v.into_iter().collect())
    }
}
