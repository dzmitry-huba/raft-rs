#![cfg(all(test, feature = "std"))]
extern crate mockall;

use self::mockall::mock;
use model::{Actor, ActorContext, ActorError};
use platform::{Attestation, Host, MessageEnvelope, PalError};

mock! {
    pub Actor {
    }

    impl Actor for Actor {
        fn on_init(&mut self, context: Box<dyn ActorContext>) -> Result<(), ActorError>;

        fn on_shutdown(&mut self);

        fn on_save_snapshot(&mut self) -> Result<Vec<u8>, ActorError>;

        fn on_load_snapshot(&mut self, snapshot: &[u8]) -> Result<(), ActorError>;

        fn on_process_command(&mut self, command: &[u8]) -> Result<(), ActorError>;

        fn on_apply_event(&mut self, index: u64, event: &[u8]) -> Result<(), ActorError>;
    }
}

mock! {
    pub Host {
    }

    impl Host for Host {
        fn get_self_attestation(&self) -> Box<dyn Attestation>;

        fn get_self_config(&self) -> Vec<u8>;

        fn send_messages(&mut self, messages: &[MessageEnvelope]);

        fn verify_peer_attestation(
            &self,
            peer_attestation: &[u8],
        ) -> Result<Box<dyn Attestation>, PalError>;
    }
}

mock! {
    pub Attestation {
    }

    impl Attestation for Attestation {
        fn serialize(&self) -> Result<Vec<u8>, PalError>;

        fn sign(&self, data: &[u8]) -> Result<Vec<u8>, PalError>;

        fn verify(&self, data: &[u8], signature: &[u8]) -> Result<(), PalError>;

        fn public_signing_key(&self) -> Vec<u8>;
    }
}
