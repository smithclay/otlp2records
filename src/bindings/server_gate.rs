//! The host frees `user_data` as soon as `otlp_grpc_server_stop` returns, so no
//! batch or auth callback may be running, or start, after that. Tokio cannot
//! enforce this: a callback is synchronous host code that a runtime shutdown or
//! a drain deadline cannot interrupt.

use std::cell::Cell;
use std::sync::{Condvar, Mutex, PoisonError};

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct GateState {
    closed: bool,
    in_flight: usize,
}

impl GateState {
    pub(crate) fn enter(&mut self) -> bool {
        if self.closed {
            return false;
        }
        self.in_flight += 1;
        true
    }

    pub(crate) fn exit(&mut self) {
        self.in_flight -= 1;
    }

    pub(crate) fn close(&mut self) {
        self.closed = true;
    }

    pub(crate) fn drained(&self) -> bool {
        self.closed && self.in_flight == 0
    }
}

thread_local! {
    static IN_HOST_CALL: Cell<bool> = const { Cell::new(false) };
}

pub(crate) fn in_host_call() -> bool {
    IN_HOST_CALL.with(Cell::get)
}

#[derive(Default)]
pub(crate) struct CallbackGate {
    state: Mutex<GateState>,
    drained: Condvar,
}

struct Admitted<'a>(&'a CallbackGate);

impl Drop for Admitted<'_> {
    fn drop(&mut self) {
        IN_HOST_CALL.with(|flag| flag.set(false));
        let mut state = self.0.lock();
        state.exit();
        if state.drained() {
            self.0.drained.notify_all();
        }
    }
}

impl CallbackGate {
    fn lock(&self) -> std::sync::MutexGuard<'_, GateState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn call<R>(&self, host_call: impl FnOnce() -> R) -> Option<R> {
        if !self.lock().enter() {
            return None;
        }
        let _admitted = Admitted(self);
        IN_HOST_CALL.with(|flag| flag.set(true));
        Some(host_call())
    }

    pub(crate) fn close_and_wait(&self) {
        let mut state = self.lock();
        state.close();
        while !state.drained() {
            state = self
                .drained
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }
}
