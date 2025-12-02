use std::path::{Path, PathBuf};

use tracing::debug;

/// Writes a copy of a stream of data to a folder.
///
/// Matches behaviour of ODIN /dev/shm writer
pub struct FolderWriter {
    /// The base folder location to write to
    base: PathBuf,
    state: FolderWriterState,
}

/// Keep track of the current writer state
enum FolderWriterState {
    /// Waiting for the header packet of a new capture series
    Waiting,
    /// Currently processing a specific series
    InAcquisition(usize),
    /// We are currently skipping anything matching a specific series
    Skipping(usize),
}

impl FolderWriter {
    pub fn new(output_path: &Path) -> Self {
        FolderWriter {
            base: output_path.to_path_buf(),
            state: FolderWriterState::Waiting,
        }
    }
    pub fn write(&mut self, messages: Vec<Vec<u8>>) {
        debug!(
            "Received: {} messages, sizes: [{}]",
            messages.len(),
            messages
                .iter()
                .map(|m| m.len().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
        match self.state {
            FolderWriterState::Waiting => {
                // Attempt to decode the zeroth packet, which should be a header
            }
            FolderWriterState::InAcquisition(_) => todo!(),
            FolderWriterState::Skipping(_) => todo!(),
        }
    }
    /// Called when the "mirror" state is changed
    ///
    /// Can be used to clean up any opened resources
    pub fn toggle(&self, _to: bool) {}
}
