use crate::observation::{EntryObservation, SourceKind};
use crossbeam_channel::Receiver;

pub trait EntrySource: Send + 'static {
    fn kind(&self) -> SourceKind;
    fn start(self: Box<Self>) -> anyhow::Result<Receiver<EntryObservation>>;
}
