use alloc::boxed::Box;
use alloc::vec;
use alloc::vec::Vec;
use raft::{
    eraftpb::ConfChange as RaftConfigChange, eraftpb::ConfState as RaftConfigState,
    eraftpb::Entry as RaftEntry, eraftpb::HardState as RaftHardState,
    eraftpb::Message as RaftMessage, eraftpb::Snapshot as RaftSnapshot, Config as RaftConfig,
    Error as RaftError, LightReady as RaftLightReady, RawNode as RaftNode, RawNode,
    Ready as RaftReady, SoftState as RaftSoftState, StateRole as RaftStateRole,
    Storage as RaftStorage,
};
use slog::Logger;

use crate::util::raft::{
    create_raft_config_state, create_raft_snapshot, create_raft_snapshot_metadata,
};

pub trait Store {
    /// Saves the current Raft hard state.
    fn set_hard_state(&mut self, state: RaftHardState);

    /// Append the new entries to storage.
    ///
    /// # Panics
    ///
    /// Panics if `entries` contains compacted entries, or there's a gap between `entries`
    /// and the last received entry in the storage.
    fn append_entries(&mut self, entries: &[RaftEntry]) -> Result<(), RaftError>;

    /// Overwrites the contents of this Storage object with those of the given snapshot.
    fn apply_snapshot(&mut self, snapshot: RaftSnapshot) -> Result<(), RaftError>;

    /// Checks if snapshotting at given index is desirable.
    fn should_snapshot(&self, applied_index: u64, config_state: &RaftConfigState) -> bool;

    /// CreateSnapshot makes a snapshot which can be retrieved with Snapshot() and
    /// can be used to reconstruct the state at that point.
    ///
    /// If any configuration changes have been made since the last compaction,
    /// the result of the last ApplyConfChange must be passed in.
    fn create_snapshot(
        &mut self,
        applied_index: u64,
        config_state: RaftConfigState,
        snapshot_data: Vec<u8>,
    ) -> Result<(), RaftError>;
}

#[derive(PartialEq, Eq, Clone, Default)]
pub struct RaftState {
    pub leader_node_id: u64,
    pub leader_term: u64,
    pub committed_cluster_config: Vec<u64>,
    pub has_pending_change: bool,
}

impl RaftState {
    pub fn new() -> RaftState {
        RaftState {
            ..Default::default()
        }
    }
}

pub trait Raft {
    type S: Store + RaftStorage;

    fn initialized(&self) -> bool;

    fn leader(&self) -> bool;

    fn state(&self) -> RaftState;

    fn mut_store(&mut self) -> &mut Self::S;

    fn init(
        &mut self,
        node_id: u64,
        leader: bool,
        store: Self::S,
        logger: &Logger,
    ) -> Result<(), RaftError>;

    fn make_step(&mut self, message: RaftMessage) -> Result<(), RaftError>;

    fn make_proposal(&mut self, proposal: Vec<u8>) -> Result<(), RaftError>;

    fn make_config_change_proposal(
        &mut self,
        config_change: RaftConfigChange,
    ) -> Result<(), RaftError>;

    fn make_tick(&mut self);

    fn apply_config_change(
        &mut self,
        config_change: &RaftConfigChange,
    ) -> Result<RaftConfigState, RaftError>;

    fn has_ready(&self) -> bool;

    fn get_ready(&mut self) -> RaftReady;

    fn advance_ready(&mut self, ready: RaftReady) -> RaftLightReady;

    fn advance_apply(&mut self);
}

#[derive(Default)]
pub struct RaftSimple<S: Store + RaftStorage> {
    raft_node: Option<Box<RaftNode<S>>>,
    committed_voters: Vec<u64>,
}

impl<S: Store + RaftStorage> RaftSimple<S> {
    pub fn new() -> RaftSimple<S> {
        RaftSimple {
            raft_node: None,
            committed_voters: Vec::new(),
        }
    }

    fn mut_raft_node(&mut self) -> &mut RaftNode<S> {
        self.raft_node
            .as_mut()
            .expect("Raft node is initialized")
            .as_mut()
    }

    fn raft_node(&self) -> &RaftNode<S> {
        self.raft_node
            .as_ref()
            .expect("Raft node is initialized")
            .as_ref()
    }
}

impl<S: Store + RaftStorage> Raft for RaftSimple<S> {
    type S = S;

    fn initialized(&self) -> bool {
        self.raft_node.is_some()
    }

    fn leader(&self) -> bool {
        self.raft_node.is_some() && self.raft_node().status().ss.raft_state == RaftStateRole::Leader
    }

    fn state(&self) -> RaftState {
        let raft_hard_state: RaftHardState;
        let raft_soft_state: RaftSoftState;
        {
            let raft_status = self.raft_node().status();
            raft_hard_state = raft_status.hs;
            raft_soft_state = raft_status.ss;
        }

        let mut state = RaftState::new();
        if raft_soft_state.raft_state == RaftStateRole::Leader {
            state.leader_node_id = raft_soft_state.leader_id;
            state.leader_term = raft_hard_state.term;
            state.has_pending_change = self.raft_node().raft.has_pending_conf();
            state.committed_cluster_config = self.committed_voters.clone();
        }

        state
    }

    fn mut_store(&mut self) -> &mut S {
        self.mut_raft_node().mut_store()
    }

    fn init(
        &mut self,
        node_id: u64,
        leader: bool,
        mut store: S,
        logger: &Logger,
    ) -> Result<(), RaftError> {
        let config = RaftConfig::new(node_id);

        if leader {
            let snapshot = create_raft_snapshot(
                create_raft_snapshot_metadata(1, 1, create_raft_config_state(vec![node_id])),
                Vec::new(),
            );

            store.apply_snapshot(snapshot)?;
            self.committed_voters = vec![node_id];
        }

        self.raft_node = Some(Box::new(RawNode::new(&config, store, logger)?));

        Ok(())
    }

    fn make_step(&mut self, message: RaftMessage) -> Result<(), RaftError> {
        self.mut_raft_node().step(message)
    }

    fn make_proposal(&mut self, proposal: Vec<u8>) -> Result<(), RaftError> {
        self.mut_raft_node().propose(vec![], proposal)
    }

    fn make_config_change_proposal(
        &mut self,
        config_change: RaftConfigChange,
    ) -> Result<(), RaftError> {
        self.mut_raft_node()
            .propose_conf_change(vec![], config_change)
    }

    fn make_tick(&mut self) {
        self.mut_raft_node().tick();
    }

    fn apply_config_change(
        &mut self,
        config_change: &RaftConfigChange,
    ) -> Result<RaftConfigState, RaftError> {
        let config_state = self.mut_raft_node().apply_conf_change(config_change);
        if let Ok(config_state) = &config_state {
            self.committed_voters = config_state.voters.clone();
        }
        config_state
    }

    fn has_ready(&self) -> bool {
        self.raft_node().has_ready()
    }

    fn get_ready(&mut self) -> RaftReady {
        self.mut_raft_node().ready()
    }

    fn advance_ready(&mut self, ready: RaftReady) -> RaftLightReady {
        self.mut_raft_node().advance(ready)
    }

    fn advance_apply(&mut self) {
        self.mut_raft_node().advance_apply()
    }
}
