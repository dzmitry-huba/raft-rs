use crate::endpoint::*;
use crate::logger::{log::create_remote_logger, DrainOutput};
use crate::model::{Actor, ActorContext};
use crate::util::raft::{
    create_raft_config_change, deserialize_config_change, deserialize_raft_message, get_conf_state,
    serialize_raft_message,
};
use alloc::boxed::Box;
use alloc::rc::Rc;
use alloc::vec;
use alloc::vec::Vec;
use core::cell::RefCell;
use core::cell::RefMut;
use core::cmp;
use core::mem;
use platform::{Application, Host, MessageEnvelope, PalError};
use prost::Message;
use raft::SoftState;
use raft::{
    eraftpb::ConfChangeType as RaftConfigChangeType, eraftpb::ConfState as RaftConfState,
    eraftpb::Entry as RaftEntry, eraftpb::EntryType as RaftEntryType, eraftpb::HardState,
    eraftpb::Message as RaftMessage, eraftpb::Snapshot as RaftSnapshot, storage::MemStorage,
    Config as RaftConfig, Error as RaftError, RawNode, StateRole as RaftStateRole,
};
use slog::{debug, error, info, o, warn, Logger};
type RaftNode = RawNode<MemStorage>;

struct DriverContextCore {
    id: u64,
    instant: u64,
    config: Vec<u8>,
    leader: bool,
    proposals: Vec<Vec<u8>>,
    messages: Vec<envelope_out::Msg>,
}

impl DriverContextCore {
    fn new() -> DriverContextCore {
        DriverContextCore {
            id: 0,
            instant: 0,
            config: Vec::new(),
            leader: false,
            proposals: Vec::new(),
            messages: Vec::new(),
        }
    }

    fn set_state(&mut self, instant: u64, leader: bool) {
        self.instant = instant;
        self.leader = leader;
    }

    fn set_immutable_state(&mut self, id: u64, config: Vec<u8>) {
        self.id = id;
        self.config = config;
    }

    fn id(&self) -> u64 {
        self.id
    }

    fn instant(&self) -> u64 {
        self.instant
    }

    fn leader(&self) -> bool {
        self.leader
    }

    fn config(&self) -> Vec<u8> {
        self.config.clone()
    }

    fn append_proposal(&mut self, proposal: Vec<u8>) {
        self.proposals.push(proposal);
    }

    fn append_message(&mut self, message: envelope_out::Msg) {
        self.messages.push(message);
    }

    fn take_outputs(&mut self) -> (Vec<Vec<u8>>, Vec<envelope_out::Msg>) {
        (
            mem::take(&mut self.proposals),
            mem::take(&mut self.messages),
        )
    }
}

struct DriverContext {
    core: Rc<RefCell<DriverContextCore>>,
    logger: Logger,
}

impl DriverContext {
    fn new(core: Rc<RefCell<DriverContextCore>>, logger: Logger) -> Self {
        DriverContext { core, logger }
    }
}

impl ActorContext for DriverContext {
    fn logger(&self) -> &Logger {
        &self.logger
    }

    fn id(&self) -> u64 {
        self.core.borrow().id()
    }

    fn instant(&self) -> u64 {
        self.core.borrow().instant()
    }

    fn config(&self) -> Vec<u8> {
        self.core.borrow().config()
    }

    fn leader(&self) -> bool {
        self.core.borrow().leader()
    }

    fn propose_event(&mut self, event: Vec<u8>) -> Result<(), crate::model::ActorError> {
        self.core.borrow_mut().append_proposal(event);

        Ok(())
    }

    fn send_message(&mut self, message: Vec<u8>) {
        self.core
            .borrow_mut()
            .append_message(envelope_out::Msg::ExecuteProposal(
                ExecuteProposalResponse {
                    result_contents: message,
                },
            ));
    }
}

#[derive(PartialEq, Eq)]
enum DriverState {
    Created,
    Started,
    Stopped,
}

pub struct DriverConfig {
    pub tick_period: u64,
    pub snapshot_count: u64,
}

#[derive(PartialEq, Eq, Clone)]
struct RaftState {
    leader_node_id: u64,
    leader_term: u64,
    committed_cluster_config: Vec<u64>,
    has_pending_change: bool,
}

impl RaftState {
    fn new() -> RaftState {
        RaftState {
            leader_node_id: 0,
            leader_term: 0,
            committed_cluster_config: Vec::new(),
            has_pending_change: false,
        }
    }
}

pub struct Driver {
    core: Rc<RefCell<DriverContextCore>>,
    actor: Box<dyn Actor>,
    driver_config: DriverConfig,
    driver_state: DriverState,
    messages: Vec<EnvelopeOut>,
    raft_node_id: u64,
    instant: u64,
    tick_instant: u64,
    logger: Logger,
    logger_output: Box<dyn DrainOutput>,
    raft_node: Option<Box<RaftNode>>,
    raft_state: RaftState,
    prev_raft_state: RaftState,
}

impl Driver {
    pub fn new(driver_config: DriverConfig, actor: Box<dyn Actor>) -> Self {
        let (logger, logger_output) = create_remote_logger(0);
        Driver {
            core: Rc::new(RefCell::new(DriverContextCore::new())),
            actor,
            driver_config,
            driver_state: DriverState::Created,
            messages: Vec::new(),
            raft_node_id: 0,
            instant: 0,
            tick_instant: 0,
            logger,
            logger_output,
            raft_node: None,
            raft_state: RaftState::new(),
            prev_raft_state: RaftState::new(),
        }
    }

    fn id(&self) -> u64 {
        self.raft_node_id
    }

    fn mut_core(&mut self) -> RefMut<'_, DriverContextCore> {
        self.core.borrow_mut()
    }

    fn mut_raft_node(&mut self) -> &mut RaftNode {
        self.raft_node
            .as_mut()
            .expect("Raft node is initialized")
            .as_mut()
    }

    fn raft_node(&self) -> &RaftNode {
        self.raft_node
            .as_ref()
            .expect("Raft node is initialized")
            .as_ref()
    }

    fn initilize_raft_node(&mut self, leader: bool) -> Result<(), PalError> {
        let config = RaftConfig::new(self.id());

        let mut snapshot = RaftSnapshot::default();
        snapshot.mut_metadata().index = 1;
        snapshot.mut_metadata().term = 1;
        snapshot.mut_metadata().mut_conf_state().voters = vec![self.id()];

        let storage = MemStorage::new();
        if leader {
            storage.wl().apply_snapshot(snapshot).map_err(|e| {
                error!(
                    self.logger,
                    "Failed to apply Raft snapshot to storage: {}", e
                );

                // Failure to apply Raft snapshot to storage must lead to termination.
                PalError::Storage
            })?;
        }

        self.raft_node = Some(Box::new(
            RawNode::new(&config, storage, &self.logger).map_err(|e| {
                error!(self.logger, "Failed to create Raft node: {}", e);

                // Failure to create Raft node must lead to termination.
                PalError::Raft
            })?,
        ));

        self.tick_instant = self.instant;

        Ok(())
    }

    fn check_raft_leadership(&self) -> bool {
        self.raft_node.is_some() && self.raft_node().status().ss.raft_state == RaftStateRole::Leader
    }

    fn make_raft_step(
        &mut self,
        sender_node_id: u64,
        recipient_node_id: u64,
        message_contents: &Vec<u8>,
    ) -> Result<(), PalError> {
        match deserialize_raft_message(message_contents) {
            Err(e) => {
                warn!(
                    self.logger,
                    "Ignoring failed to deserialize Raft message: {}", e
                );

                Ok(())
            }
            Ok(message) => {
                if self.raft_node_id != recipient_node_id {
                    // Ignore incorrectly routed message
                    warn!(
                        self.logger,
                        "Ignoring incorectly routed Raft message: recipient id {}",
                        recipient_node_id
                    );
                }
                if message.get_from() != sender_node_id {
                    // Ignore malformed message
                    warn!(
                        self.logger,
                        "Ignoring malformed Raft message: sender id {}", sender_node_id
                    );
                }

                // Advance Raft internal state by one step.
                match self.mut_raft_node().step(message) {
                    Err(e) => {
                        error!(self.logger, "Raft experienced unrecoverable error: {}", e);

                        // Unrecoverable Raft errors must lead to termination.
                        Err(PalError::Raft)
                    }
                    Ok(_) => Ok(()),
                }
            }
        }
    }

    fn make_raft_proposal(&mut self, proposal_contents: Vec<u8>) {
        debug!(self.logger, "Making Raft proposal");

        self.mut_raft_node()
            .propose(vec![], proposal_contents)
            .unwrap();
    }

    fn make_raft_config_change_proposal(
        &mut self,
        node_id: u64,
        change_type: RaftConfigChangeType,
    ) -> Result<ChangeClusterStatus, PalError> {
        debug!(self.logger, "Making Raft config change proposal");

        let config_change = create_raft_config_change(node_id, change_type);
        match self
            .mut_raft_node()
            .propose_conf_change(vec![], config_change)
        {
            Ok(_) => Ok(ChangeClusterStatus::ChangeStatusPending),
            Err(RaftError::ProposalDropped) => {
                warn!(self.logger, "Dropping Raft config change proposal");

                Ok(ChangeClusterStatus::ChangeStatusRejected)
            }
            Err(e) => {
                error!(self.logger, "Raft experienced unrecoverable error: {}", e);

                // Unrecoverable Raft errors must lead to termination.
                Err(PalError::Raft)
            }
        }
    }

    fn trigger_raft_tick(&mut self) {
        // Given that Raft is being driven from the outside and arbitrary amount of time can
        // pass between driver invocation we may need to produce multiple ticks.
        while self.instant - self.tick_instant >= self.driver_config.tick_period {
            self.tick_instant += self.driver_config.tick_period;
            // invoke Raft tick to trigger timer based changes.
            self.mut_raft_node().tick();
        }
    }

    fn apply_raft_committed_entries(
        &mut self,
        committed_entries: Vec<RaftEntry>,
    ) -> Result<(), PalError> {
        for committed_entry in committed_entries {
            if committed_entry.data.is_empty() {
                // Empty entry is produced by the newly elected leader to commit entries
                // from the previous terms.
                continue;
            }
            // The entry may either be a config change or a normal proposal.
            if let RaftEntryType::EntryConfChange = committed_entry.get_entry_type() {
                debug!(self.logger, "Applying Raft config change entry");

                // Make committed configuration effective.
                let config_change = match deserialize_config_change(&committed_entry.data) {
                    Ok(config_change) => config_change,
                    Err(e) => {
                        error!(
                            self.logger,
                            "Failed to deserialize Raft config change: {}", e
                        );
                        // Failure to deserialize Raft config change must lead to termination.
                        return Err(PalError::Raft);
                    }
                };

                match self.mut_raft_node().apply_conf_change(&config_change) {
                    Err(e) => {
                        error!(self.logger, "Failed to apply Raft config change: {}", e);
                        // Failure to apply Raft config change must lead to termination.
                        return Err(PalError::Raft);
                    }
                    Ok(config_state) => {
                        self.collect_config_state(&config_state);

                        self.mut_raft_node()
                            .store()
                            .wl()
                            .set_conf_state(config_state);
                    }
                };
            } else {
                debug!(self.logger, "Applying Raft entry");

                // Pass committed entry to the actor to make effective.
                self.actor
                    .on_apply_event(committed_entry.index, committed_entry.get_data())
                    .map_err(|e| {
                        error!(
                            self.logger,
                            "Failed to apply committed event to actor state: {}", e
                        );
                        // Failure to apply committed event to actor state must lead to termination.
                        PalError::Actor
                    })?;
            }
        }

        Ok(())
    }

    fn send_raft_messages(&mut self, raft_messages: Vec<RaftMessage>) -> Result<(), PalError> {
        for raft_message in raft_messages {
            // Buffer message to be sent out.
            self.stash_message(envelope_out::Msg::DeliverMessage(DeliverMessage {
                recipient_node_id: raft_message.to,
                sender_node_id: self.id(),
                message_contents: serialize_raft_message(&raft_message).unwrap(),
            }));
        }

        Ok(())
    }

    fn restore_raft_snapshot(&mut self, raft_snapshot: &RaftSnapshot) -> Result<(), PalError> {
        if raft_snapshot.is_empty() {
            // Nothing to restore if the snapshot is empty.
            return Ok(());
        }

        info!(self.logger, "Restoring Raft snappshot");

        self.collect_config_state(get_conf_state(raft_snapshot));

        // Persist unstable snapshot received from a peer into the stable storage.
        let apply_result = self
            .mut_raft_node()
            .store()
            .wl()
            .apply_snapshot(raft_snapshot.clone());
        if let Err(e) = apply_result {
            error!(
                self.logger,
                "Failed to apply Raft snapshot to storage: {}", e
            );
            // Failure to apply Raft snapshot to storage snapshot must lead to termination.
            return Err(PalError::Raft);
        }

        // Pass snapshot to the actor to restore.
        self.actor
            .on_load_snapshot(raft_snapshot.get_data())
            .map_err(|e| {
                error!(self.logger, "Failed to load actor state snapshot: {}", e);
                // Failure to load actor snapshot must lead to termination.
                PalError::Actor
            })?;

        Ok(())
    }

    fn maybe_create_raft_snapshot(&mut self) -> Result<(), PalError> {
        let _actor_snapshot = self.actor.on_save_snapshot().map_err(|e| {
            error!(self.logger, "Failed to save actor state to snapshot: {}", e);
            // Failure to apply Raft snapshot to storage snapshot must lead to termination.
            PalError::Actor
        })?;

        // todo!()

        Ok(())
    }

    fn advance_raft(&mut self) -> Result<(), PalError> {
        // Given that instant only set once trigger Raft tick once as well.
        self.trigger_raft_tick();

        if !self.raft_node().has_ready() {
            // There is nothing process.
            return Ok(());
        }

        let mut raft_ready = self.mut_raft_node().ready();

        // Send out messages to the peers.
        self.send_raft_messages(raft_ready.take_messages())?;

        if let Some(raft_hard_state) = raft_ready.hs() {
            // Persist changed hard state into the stable storage.
            self.mut_raft_node()
                .store()
                .wl()
                .set_hardstate(raft_hard_state.clone());
        }

        // If not empty persist snapshot to stable storage and apply it to the
        // actor.
        self.restore_raft_snapshot(raft_ready.snapshot())?;

        // Apply committed entries to the actor state machine.
        self.apply_raft_committed_entries(raft_ready.take_committed_entries())?;
        // Send out messages that had to await the persistence of the hard state, entries
        // and snapshot to the stable storage.
        self.send_raft_messages(raft_ready.take_persisted_messages())?;

        if !raft_ready.entries().is_empty() {
            // Persist unstable entries into the stable storage.
            let append_result = self
                .mut_raft_node()
                .store()
                .wl()
                .append(raft_ready.entries());
            if let Err(e) = append_result {
                error!(
                    self.logger,
                    "Failed to append Raft entries to storage: {}", e
                );
                // Failure to append Raft entries to storage snapshot must lead to termination.
                return Err(PalError::Actor);
            }
        }

        // Advance Raft state after fully processing ready.
        let mut light_raft_ready: raft::LightReady = self.mut_raft_node().advance(raft_ready);

        // Send out messages to the peers.
        self.send_raft_messages(light_raft_ready.take_messages())?;
        // Apply all committed entries.
        self.apply_raft_committed_entries(light_raft_ready.take_committed_entries())?;
        // Advance the apply index.
        self.mut_raft_node().advance_apply();

        Ok(())
    }

    fn reset_leader_state(&mut self) {
        self.raft_state = RaftState::new();
    }

    fn collect_config_state(&mut self, config_state: &RaftConfState) {
        self.raft_state.committed_cluster_config = config_state.voters.clone();
    }

    fn stash_leader_state(&mut self) {
        let raft_hard_state: HardState;
        let raft_soft_state: SoftState;
        {
            let raft_status = self.raft_node().status();
            raft_hard_state = raft_status.hs;
            raft_soft_state = raft_status.ss;
        }
        if raft_soft_state.raft_state == RaftStateRole::Leader {
            self.raft_state.leader_node_id = raft_soft_state.leader_id;
            self.raft_state.leader_term = raft_hard_state.term;
            self.raft_state.has_pending_change = self.raft_node().raft.has_pending_conf();
        }

        if self.prev_raft_state == self.raft_state {
            return;
        }

        self.prev_raft_state = self.raft_state.clone();

        self.stash_message(envelope_out::Msg::CheckCluster(CheckClusterResponse {
            leader_node_id: self.raft_state.leader_node_id,
            leader_term: self.raft_state.leader_term,
            cluster_node_ids: self.raft_state.committed_cluster_config.clone(),
            has_pending_changes: self.raft_state.has_pending_change,
        }));
    }

    fn preset_state_machine(&mut self, instant: u64) {
        self.prev_raft_state = self.raft_state.clone();
        self.instant = cmp::max(self.instant, instant);
        let instant = self.instant;
        let leader = self.check_raft_leadership();
        self.mut_core().set_state(instant, leader);
    }

    fn check_driver_state(&self, state: DriverState) -> Result<(), PalError> {
        if self.driver_state != state {
            return Err(PalError::InvalidOperation);
        }

        Ok(())
    }

    fn check_driver_started(&self) -> Result<(), PalError> {
        self.check_driver_state(DriverState::Started)
    }

    fn initialize_driver(
        &mut self,
        app_config: Vec<u8>,
        _app_signing_key: Vec<u8>,
        node_id_hint: u64,
    ) {
        let id = node_id_hint;
        self.mut_core().set_immutable_state(id, app_config);
        self.raft_node_id = id;
        (self.logger, self.logger_output) = create_remote_logger(self.raft_node_id);
    }

    fn process_start_node(
        &mut self,
        start_node_request: &StartNodeRequest,
        app_config: Vec<u8>,
        app_signing_key: Vec<u8>,
    ) -> Result<(), PalError> {
        self.check_driver_state(DriverState::Created)?;

        self.initialize_driver(app_config, app_signing_key, start_node_request.node_id_hint);

        self.initilize_raft_node(start_node_request.is_leader)?;

        let actor_context = Box::new(DriverContext::new(
            Rc::clone(&self.core),
            self.logger.new(o!("type" => "actor")),
        ));

        self.actor.on_init(actor_context).map_err(|e| {
            error!(self.logger, "Failed to initialize actor: {}", e);

            // Failure to initialize actor must lead to termination.
            PalError::Actor
        })?;

        self.driver_state = DriverState::Started;

        self.stash_message(envelope_out::Msg::StartNode(StartNodeResponse {
            node_id: self.id(),
        }));

        Ok(())
    }

    fn process_stop_node(&mut self, _stop_node_request: &StopNodeRequest) -> Result<(), PalError> {
        if let DriverState::Stopped = self.driver_state {
            return Ok(());
        }

        self.actor.on_shutdown();

        self.driver_state = DriverState::Stopped;

        Ok(())
    }

    fn process_change_cluster(
        &mut self,
        change_cluster_request: &ChangeClusterRequest,
    ) -> Result<(), PalError> {
        self.check_driver_started()?;

        let change_status = match ChangeClusterType::from_i32(change_cluster_request.change_type) {
            Some(ChangeClusterType::ChangeTypeAddNode) => self.make_raft_config_change_proposal(
                change_cluster_request.node_id,
                RaftConfigChangeType::AddNode,
            )?,
            Some(ChangeClusterType::ChangeTypeRemoveNode) => self
                .make_raft_config_change_proposal(
                    change_cluster_request.node_id,
                    RaftConfigChangeType::RemoveNode,
                )?,
            _ => {
                warn!(self.logger, "Rejecting cluster change command: unknown");

                ChangeClusterStatus::ChangeStatusRejected
            }
        };

        self.stash_message(envelope_out::Msg::ChangeCluster(ChangeClusterResponse {
            change_id: change_cluster_request.change_id,
            change_status: change_status.into(),
        }));

        Ok(())
    }

    fn process_check_cluster(
        &mut self,
        _check_cluster_request: &CheckClusterRequest,
    ) -> Result<(), PalError> {
        self.check_driver_started()?;

        self.reset_leader_state();

        Ok(())
    }

    fn process_deliver_message(
        &mut self,
        deliver_message: &DeliverMessage,
    ) -> Result<(), PalError> {
        self.check_driver_started()?;

        self.make_raft_step(
            deliver_message.sender_node_id,
            deliver_message.recipient_node_id,
            &deliver_message.message_contents,
        )
    }

    fn process_execute_proposal(
        &mut self,
        execute_proposal_request: &ExecuteProposalRequest,
    ) -> Result<(), PalError> {
        self.check_driver_started()?;

        self.actor
            .on_process_command(execute_proposal_request.proposal_contents.as_ref())
            .map_err(|e| {
                error!(self.logger, "Failed to process actor command: {}", e);

                // Failure to process actor command must lead to termination.
                PalError::Actor
            })?;

        Ok(())
    }

    fn process_actor_output(&mut self) {
        let (proposals, messages) = self.mut_core().take_outputs();

        for proposal in proposals {
            self.make_raft_proposal(proposal);
        }

        for message in messages {
            self.stash_message(message);
        }
    }

    fn process_state_machine(&mut self) -> Result<Vec<EnvelopeOut>, PalError> {
        if self.raft_node.is_some() {
            // Advance Raft internal state.
            self.advance_raft()?;

            // Maybe create a snashot of the actor to reduce the size of the log.
            self.maybe_create_raft_snapshot()?;

            // If the leader state has changed send it out for observability.
            self.stash_leader_state();
        }

        self.stash_log_entries();

        // Take messages to be sent out.
        Ok(mem::take(&mut self.messages))
    }

    fn stash_log_entries(&mut self) {
        for log_message in self.logger_output.take_entries() {
            self.stash_message(envelope_out::Msg::Log(log_message));
        }
    }

    fn stash_message(&mut self, message: envelope_out::Msg) {
        self.messages.push(EnvelopeOut { msg: Some(message) });
    }

    fn send_messages(&mut self, host: &mut impl Host, out_messages: Vec<EnvelopeOut>) {
        let mut serialized_out_messages: Vec<MessageEnvelope> =
            Vec::with_capacity(out_messages.len());
        for out_message in &out_messages {
            serialized_out_messages.push(out_message.encode_to_vec());
        }

        host.send_messages(&serialized_out_messages);
    }
}

impl Application for Driver {
    /// Handles messages received from the trusted host.
    fn receive_message(
        &mut self,
        host: &mut impl Host,
        instant: u64,
        opt_message: Option<MessageEnvelope>,
    ) -> Result<(), PalError> {
        // Update state of the context that will remain unchanged while messages are
        // dispatched for processing.
        self.preset_state_machine(instant);

        // Dispatch incoming message for processing.
        if let Some(serialized_message) = opt_message {
            match EnvelopeIn::decode(serialized_message.as_ref()) {
                Ok(deserialized_message) => match deserialized_message.msg {
                    None => {
                        warn!(self.logger, "Rejecting message: unknown");
                        // Ignore unknown message.
                        return Ok(());
                    }
                    Some(message) => {
                        match message {
                            envelope_in::Msg::StartNode(ref start_node_request) => self
                                .process_start_node(
                                    start_node_request,
                                    host.get_self_config(),
                                    host.get_self_attestation().public_signing_key(),
                                ),
                            envelope_in::Msg::StopNode(ref stop_node_request) => {
                                self.process_stop_node(stop_node_request)
                            }
                            envelope_in::Msg::ChangeCluster(ref change_cluster_request) => {
                                self.process_change_cluster(change_cluster_request)
                            }
                            envelope_in::Msg::CheckCluster(ref check_cluster_request) => {
                                self.process_check_cluster(check_cluster_request)
                            }
                            envelope_in::Msg::DeliverMessage(ref deliver_message) => {
                                self.process_deliver_message(deliver_message)
                            }
                            envelope_in::Msg::ExecuteProposal(ref execute_proposal_request) => {
                                self.process_execute_proposal(execute_proposal_request)
                            }
                        }?;
                    }
                },
                Err(e) => {
                    warn!(self.logger, "Rejecting message: {}", e);
                    // Ignore message that cannot be decoded.
                    return Ok(());
                }
            };
        }

        // Collect outpus like messages, log entries and proposals from the actor.
        self.process_actor_output();

        // Advance the Raft and collect results messages.
        let out_messages = self.process_state_machine()?;

        // Send messages to Raft peers and consumers through the trusted host.
        self.send_messages(host, out_messages);

        Ok(())
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn test() {}
}
