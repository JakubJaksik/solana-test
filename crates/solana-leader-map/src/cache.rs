use anyhow::{Context, Result};
use std::path::PathBuf;
use tracing::{debug, info};

use crate::domain::EpochSnapshot;

pub struct Cache {
    dir: PathBuf,
}

impl Cache {
    pub fn new(dir: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("failed to create cache dir {}", dir.display()))?;
        Ok(Self { dir })
    }

    pub fn snapshot_path(&self, epoch: u64) -> PathBuf {
        self.dir.join(format!("leader-map-epoch-{}.json", epoch))
    }

    pub fn save(&self, snap: &EpochSnapshot) -> Result<PathBuf> {
        let path = self.snapshot_path(snap.epoch.epoch);
        let json = serde_json::to_string_pretty(snap)
            .context("serialize EpochSnapshot")?;
        std::fs::write(&path, json)
            .with_context(|| format!("write {}", path.display()))?;
        info!(path = %path.display(), "snapshot saved");
        Ok(path)
    }

    pub fn load(&self, epoch: u64) -> Result<EpochSnapshot> {
        let path = self.snapshot_path(epoch);
        let raw = std::fs::read_to_string(&path)
            .with_context(|| format!("read {}", path.display()))?;
        debug!(path = %path.display(), "loading snapshot");
        let snap: EpochSnapshot = serde_json::from_str(&raw)
            .with_context(|| format!("parse {}", path.display()))?;
        Ok(snap)
    }

    pub fn latest_snapshot(&self) -> Result<Option<EpochSnapshot>> {
        let mut newest: Option<(u64, PathBuf)> = None;
        for entry in std::fs::read_dir(&self.dir)
            .with_context(|| format!("read_dir {}", self.dir.display()))?
        {
            let entry = entry?;
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            // matches: leader-map-epoch-{N}.json
            let Some(rest) = name.strip_prefix("leader-map-epoch-") else {
                continue;
            };
            let Some(epoch_str) = rest.strip_suffix(".json") else {
                continue;
            };
            let Ok(epoch) = epoch_str.parse::<u64>() else {
                continue;
            };
            if newest.as_ref().map(|(e, _)| epoch > *e).unwrap_or(true) {
                newest = Some((epoch, path));
            }
        }
        let Some((epoch, _path)) = newest else {
            return Ok(None);
        };
        Ok(Some(self.load(epoch)?))
    }

}
