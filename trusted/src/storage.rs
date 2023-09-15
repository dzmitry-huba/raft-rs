use crate::util::raft::{
    config_state_contains_node, create_raft_config_state, create_raft_snapshot,
    create_raft_snapshot_metadata, get_config_state, get_metadata,
};
use alloc::vec;
use alloc::vec::Vec;
use core::{cell::RefCell, cmp, result::Result};
use hashbrown::HashMap;
use raft::{
    eraftpb::ConfState as RaftConfigState, eraftpb::Entry as RaftEntry,
    eraftpb::HardState as RaftHardState, eraftpb::Snapshot as RaftSnapshot, util::limit_size,
    Error as RaftError, RaftState, Storage, StorageError as RaftStorageError,
};
use slog::{debug, Logger};

fn statisfies_snapshot_request(
    snapshot_index: u64,
    snapshot_config_state: &RaftConfigState,
    peer_id: u64,
    request_index: u64,
) -> bool {
    snapshot_index >= request_index && config_state_contains_node(snapshot_config_state, peer_id)
}

struct MemoryStorageCore {
    logger: Logger,
    state: RaftHardState,
    entries: Vec<RaftEntry>,
    max_snapshot_diff: u64,
    snapshot: RaftSnapshot,
    snapshot_peer_requests: HashMap<u64, u64>,
}

impl MemoryStorageCore {
    fn new(logger: Logger, max_snapshot_diff: u64) -> MemoryStorageCore {
        MemoryStorageCore {
            logger,
            max_snapshot_diff,
            state: RaftHardState::default(),
            entries: Vec::new(),
            snapshot: create_raft_snapshot(
                create_raft_snapshot_metadata(0, 0, create_raft_config_state(vec![])),
                Vec::new(),
            ),
            snapshot_peer_requests: HashMap::new(),
        }
    }

    fn snapshot_index(&self) -> u64 {
        get_metadata(&self.snapshot).index
    }

    fn snapshot_term(&self) -> u64 {
        get_metadata(&self.snapshot).term
    }

    fn config_state(&self) -> &RaftConfigState {
        get_config_state(&self.snapshot)
    }

    fn first_entry_index(&self) -> u64 {
        match self.entries.first() {
            Some(entry) => entry.index,
            None => self.snapshot_index() + 1,
        }
    }

    fn last_entry_index(&self) -> u64 {
        match self.entries.last() {
            Some(entry) => entry.index,
            None => self.snapshot_index(),
        }
    }

    fn set_hard_state(&mut self, state: RaftHardState) {
        self.state = state;
    }

    fn append_entries(&mut self, entries: &[RaftEntry]) -> Result<(), RaftError> {
        debug!(self.logger, "Append, entries: {:?}", entries);

        if entries.is_empty() {
            return Ok(());
        }

        let first_append_index = entries[0].index;

        // Check that new entries do not overwrite previsouly compacted entries.
        if self.first_entry_index() > first_append_index {
            panic!(
                "Overwriting compacted Raft logs, compacted index: {}, append idnex: {}",
                self.first_entry_index() - 1,
                first_append_index,
            );
        }

        // Check that log will remain continuous.
        if self.last_entry_index() + 1 < first_append_index {
            panic!(
                "Creating gap in Raft log, must be continuous, last index: {}, append index: {}",
                self.last_entry_index(),
                first_append_index,
            );
        }

        // Remove all overwritten entries.
        let overwritten_entries = first_append_index - self.first_entry_index();
        self.entries.drain(overwritten_entries as usize..);
        // Append new entries.
        self.entries.extend_from_slice(entries);

        Ok(())
    }

    fn compact_entries(&mut self, compact_index: u64) -> Result<(), RaftError> {
        debug!(self.logger, "Compact, index {}", compact_index);

        if compact_index <= self.first_entry_index() {
            // The log has already been compated, there is nothing to do.
            return Ok(());
        }

        // Check that entries to compact exist.
        if compact_index > self.last_entry_index() + 1 {
            panic!(
                "Compacting beyond available Raft log entries, compact index: {}, last index: {}",
                compact_index,
                self.last_entry_index()
            );
        }

        if let Some(entry) = self.entries.first() {
            let offset = compact_index - entry.index;
            self.entries.drain(..offset as usize);
        }
        Ok(())
    }

    fn apply_snapshot(&mut self, snapshot: RaftSnapshot) -> Result<(), RaftError> {
        debug!(self.logger, "Applying snapshot, snapshot {:?}", snapshot);

        let snapshot_metadata = get_metadata(&snapshot);

        // Handle check for old snapshot being applied.
        if self.first_entry_index() > snapshot_metadata.index {
            return Err(RaftError::Store(RaftStorageError::SnapshotOutOfDate));
        }

        self.state.commit = snapshot_metadata.index;
        self.state.term = cmp::max(self.state.term, snapshot_metadata.term);

        self.entries.clear();
        self.set_snapshot(snapshot);

        Ok(())
    }

    fn create_snapshot(
        &mut self,
        applied_index: u64,
        config_state: RaftConfigState,
        snapshot_data: Vec<u8>,
    ) -> Result<(), RaftError> {
        debug!(
            self.logger,
            "Creating snapshot, applied index: {}, config state {:?}", applied_index, config_state
        );

        if applied_index > self.last_entry_index() {
            panic!(
                "Raft log index is out of bounds, last index: {}, applied index: {}",
                self.last_entry_index(),
                applied_index
            );
        }

        // Handle check for old snapshot being applied.
        if self.first_entry_index() > applied_index {
            return Err(RaftError::Store(RaftStorageError::SnapshotOutOfDate));
        }

        let snapshot = create_raft_snapshot(
            create_raft_snapshot_metadata(
                applied_index,
                self.entry_term(applied_index)?,
                config_state,
            ),
            snapshot_data,
        );

        self.set_snapshot(snapshot);
        self.compact_entries(applied_index)
    }

    fn try_satisfy_request(&mut self, peer_id: u64, request_index: u64) -> Option<RaftSnapshot> {
        // Return snapshot only if it has all requested entries and configuration
        // contains the node requesting snapshot.
        if statisfies_snapshot_request(
            self.snapshot_index(),
            self.config_state(),
            peer_id,
            request_index,
        ) {
            return Some(self.snapshot.clone());
        }
        // Remember that snapshot has been requested and needs to be produced
        // on the next opportunity.
        self.snapshot_peer_requests.insert(peer_id, request_index);
        None
    }

    fn set_snapshot(&mut self, snapshot: RaftSnapshot) {
        self.snapshot = snapshot;
        let snapshot_index = self.snapshot_index();
        let config_state = self.config_state().clone();
        // Remove pending peer requests that are satisfied by given snapshot.
        self.snapshot_peer_requests
            .extract_if(|peer_id, request_index| {
                statisfies_snapshot_request(snapshot_index, &config_state, *peer_id, *request_index)
            })
            .count();
    }

    fn should_snapshot(&self, applied_index: u64, config_state: &RaftConfigState) -> bool {
        // Check if existing snapshot is too old.
        if self.snapshot_index() + self.max_snapshot_diff < applied_index {
            return true;
        }
        // Check if snapshotting at applied index and config state will satisfy any of the
        // pending peer requests.
        self.snapshot_peer_requests
            .iter()
            .any(|(peer_id, request_index)| {
                statisfies_snapshot_request(applied_index, config_state, *peer_id, *request_index)
            })
    }

    fn initial_state(&self) -> Result<RaftState, RaftError> {
        Ok(RaftState {
            hard_state: self.state.clone(),
            conf_state: self.config_state().clone(),
        })
    }

    fn entries(
        &self,
        low_index: u64,
        high_index: u64,
        entries_max_size: impl Into<Option<u64>>,
        _context: raft::GetEntriesContext,
    ) -> Result<Vec<RaftEntry>, RaftError> {
        debug!(
            self.logger,
            "Getting entries, low: {}, high: {}", low_index, high_index
        );

        let entries_max_size = entries_max_size.into();

        if self.entries.is_empty() {
            return Err(RaftError::Store(RaftStorageError::Unavailable));
        }

        if low_index < self.first_entry_index() {
            return Err(RaftError::Store(RaftStorageError::Compacted));
        }

        if high_index > self.last_entry_index() + 1 {
            panic!(
                "Raft log index is out of bounds, last index: {}, high index: {}",
                self.last_entry_index(),
                high_index
            );
        }

        let offset = self.first_entry_index();
        let lo = (low_index - offset) as usize;
        let hi = (high_index - offset) as usize;
        let mut entries_slice = self.entries[lo..hi].to_vec();
        limit_size(&mut entries_slice, entries_max_size);

        Ok(entries_slice)
    }

    fn entry_term(&self, index: u64) -> Result<u64, RaftError> {
        debug!(self.logger, "Getting term, index: {}", index);

        if index == self.snapshot_index() {
            return Ok(self.snapshot_term());
        }

        let offset = self.first_entry_index();
        if index < offset {
            return Err(RaftError::Store(RaftStorageError::Compacted));
        }

        if index > self.last_entry_index() {
            return Err(RaftError::Store(RaftStorageError::Unavailable));
        }

        Ok(self.entries[(index - offset) as usize].term)
    }

    fn snapshot(&mut self, request_index: u64, peer_id: u64) -> Result<RaftSnapshot, RaftError> {
        debug!(
            self.logger,
            "Getting snapshot, request index: {}, peer id {}", request_index, peer_id
        );

        match self.try_satisfy_request(peer_id, request_index) {
            Some(snapshot) => Ok(snapshot),
            None => Err(RaftError::Store(
                RaftStorageError::SnapshotTemporarilyUnavailable,
            )),
        }
    }
}

pub struct MemoryStorage {
    core: RefCell<MemoryStorageCore>,
}

impl MemoryStorage {
    pub fn new(logger: Logger, max_snapshot_diff: u64) -> MemoryStorage {
        MemoryStorage {
            core: RefCell::new(MemoryStorageCore::new(logger, max_snapshot_diff)),
        }
    }

    /// Saves the current Raft hard state.
    pub fn set_hard_state(&mut self, state: RaftHardState) {
        self.core.borrow_mut().set_hard_state(state);
    }

    /// Append the new entries to storage.
    ///
    /// # Panics
    ///
    /// Panics if `entries` contains compacted entries, or there's a gap between `entries`
    /// and the last received entry in the storage.
    pub fn append_entries(&mut self, entries: &[RaftEntry]) -> Result<(), RaftError> {
        self.core.borrow_mut().append_entries(entries)
    }

    /// Overwrites the contents of this Storage object with those of the given snapshot.
    pub fn apply_snapshot(&mut self, snapshot: RaftSnapshot) -> Result<(), RaftError> {
        self.core.borrow_mut().apply_snapshot(snapshot)
    }

    /// CreateSnapshot makes a snapshot which can be retrieved with Snapshot() and
    /// can be used to reconstruct the state at that point.
    ///
    /// If any configuration changes have been made since the last compaction,
    /// the result of the last ApplyConfChange must be passed in.
    pub fn create_snapshot(
        &mut self,
        applied_index: u64,
        config_state: RaftConfigState,
        snapshot_data: Vec<u8>,
    ) -> Result<(), RaftError> {
        self.core
            .borrow_mut()
            .create_snapshot(applied_index, config_state, snapshot_data)
    }

    /// Checks if snapshotting at given index is desirable.
    pub fn should_snapshot(&self, applied_index: u64, config_state: &RaftConfigState) -> bool {
        self.core
            .borrow()
            .should_snapshot(applied_index, config_state)
    }
}

impl Storage for MemoryStorage {
    fn initial_state(&self) -> Result<RaftState, RaftError> {
        self.core.borrow().initial_state()
    }

    fn entries(
        &self,
        low_index: u64,
        high_index: u64,
        entries_max_size: impl Into<Option<u64>>,
        context: raft::GetEntriesContext,
    ) -> Result<Vec<RaftEntry>, RaftError> {
        self.core
            .borrow()
            .entries(low_index, high_index, entries_max_size, context)
    }

    fn term(&self, index: u64) -> Result<u64, RaftError> {
        self.core.borrow().entry_term(index)
    }

    fn first_index(&self) -> Result<u64, RaftError> {
        Ok(self.core.borrow().first_entry_index())
    }

    fn last_index(&self) -> Result<u64, RaftError> {
        Ok(self.core.borrow().last_entry_index())
    }

    fn snapshot(&self, request_index: u64, peer_id: u64) -> Result<RaftSnapshot, RaftError> {
        self.core.borrow_mut().snapshot(request_index, peer_id)
    }
}
