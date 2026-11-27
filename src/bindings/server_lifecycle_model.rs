use std::collections::{HashMap, VecDeque};

use crate::server_gate::GateState;

const WORKERS: usize = 2;
const MESSAGES: u8 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Protocol {
    Detached,
    TrackedUntilDeadline,
    Gated,
    GatedWithoutReentrancyCheck,
}

impl Protocol {
    fn gated(self) -> bool {
        matches!(
            self,
            Protocol::Gated | Protocol::GatedWithoutReentrancyCheck
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Stop {
    NotCalled,
    Draining,
    Closing,
    Returned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    OtapStream,
    Unary,
}

const KINDS: [Kind; WORKERS] = [Kind::OtapStream, Kind::Unary];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Stopper {
    Host,
    Callback(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Worker {
    /// At an await point: a runtime shutdown drops it here.
    Waiting,
    /// Holding a decoded message inside one poll, about to call the host. A
    /// runtime shutdown does not interrupt a poll in progress.
    Ready,
    InCallback,
    InCallbackStopping,
    Finished,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct State {
    stop: Stop,
    stopper: Stopper,
    gate: GateState,
    workers: [Worker; WORKERS],
    remaining: [u8; WORKERS],
}

impl State {
    fn initial() -> Self {
        State {
            stop: Stop::NotCalled,
            stopper: Stopper::Host,
            gate: GateState::default(),
            workers: [Worker::Waiting; WORKERS],
            remaining: [MESSAGES; WORKERS],
        }
    }

    fn runtime_alive(&self) -> bool {
        self.stop != Stop::Returned
    }

    fn all_finished(&self) -> bool {
        self.workers.iter().all(|w| *w == Worker::Finished)
    }

    fn terminal(&self) -> bool {
        self.stop == Stop::Returned
            && self
                .workers
                .iter()
                .all(|w| matches!(w, Worker::Finished | Worker::Waiting))
    }

    fn callback_running(&self) -> bool {
        self.workers
            .iter()
            .any(|w| matches!(w, Worker::InCallback | Worker::InCallbackStopping))
    }
}

fn successors(protocol: Protocol, callback_calls_stop: bool, s: &State) -> Vec<(String, State)> {
    let mut next = Vec::new();
    let mut push = |label: String, state: State| next.push((label, state));
    let cancelled = s.stop != Stop::NotCalled;

    if s.stop == Stop::NotCalled {
        let mut t = s.clone();
        t.stop = Stop::Draining;
        push("host: stop() signals shutdown".into(), t);
    }

    let stopper_free = match s.stopper {
        Stopper::Host => true,
        Stopper::Callback(w) => s.workers[w] == Worker::InCallbackStopping,
    };
    if stopper_free {
        match s.stop {
            Stop::NotCalled | Stop::Returned => {}
            Stop::Draining => {
                let drained = match protocol {
                    Protocol::Detached => true,
                    _ => s.all_finished(),
                };
                let after = if protocol.gated() {
                    Stop::Closing
                } else {
                    Stop::Returned
                };
                let mut t = s.clone();
                t.stop = after;
                if after == Stop::Closing {
                    t.gate.close();
                }
                let label = if drained {
                    "stop: drain complete"
                } else {
                    "stop: drain deadline expires"
                };
                push(label.into(), t);
            }
            Stop::Closing => {
                if s.gate.drained() {
                    let mut t = s.clone();
                    t.stop = Stop::Returned;
                    push("stop: gate drained, returns".into(), t);
                }
            }
        }
    }

    for (i, kind) in KINDS.into_iter().enumerate() {
        match s.workers[i] {
            Worker::Waiting if !s.runtime_alive() => {}
            Worker::Waiting if s.remaining[i] == 0 => {
                let mut t = s.clone();
                t.workers[i] = Worker::Finished;
                push(format!("worker {i}: stream ends"), t);
            }
            Worker::Waiting => {
                let mut t = s.clone();
                t.workers[i] = Worker::Ready;
                t.remaining[i] -= 1;
                push(format!("worker {i}: receives a message"), t);
                if kind == Kind::OtapStream && cancelled && protocol != Protocol::Detached {
                    let mut t = s.clone();
                    t.workers[i] = Worker::Finished;
                    push(format!("worker {i}: observes cancellation"), t);
                }
            }
            Worker::Ready => {
                let mut t = s.clone();
                if !protocol.gated() || t.gate.enter() {
                    t.workers[i] = Worker::InCallback;
                    push(format!("worker {i}: enters host callback"), t);
                } else {
                    t.workers[i] = Worker::Finished;
                    push(format!("worker {i}: refused by closed gate"), t);
                }
            }
            Worker::InCallback => {
                let mut t = s.clone();
                if protocol.gated() {
                    t.gate.exit();
                }
                t.workers[i] = Worker::Waiting;
                push(format!("worker {i}: callback returns"), t);

                if callback_calls_stop
                    && i == 0
                    && s.stop == Stop::NotCalled
                    && protocol == Protocol::GatedWithoutReentrancyCheck
                {
                    let mut t = s.clone();
                    t.workers[i] = Worker::InCallbackStopping;
                    t.stopper = Stopper::Callback(i);
                    t.stop = Stop::Draining;
                    push(format!("worker {i}: callback calls stop()"), t);
                }
            }
            Worker::InCallbackStopping => {
                if s.stop == Stop::Returned {
                    let mut t = s.clone();
                    t.workers[i] = Worker::InCallback;
                    push(format!("worker {i}: its stop() returns"), t);
                }
            }
            Worker::Finished => {}
        }
    }
    next
}

#[derive(Debug)]
enum Violation {
    CallbackAfterStop(Vec<String>),
    Stuck(Vec<String>),
}

fn check(protocol: Protocol, callback_calls_stop: bool) -> Result<usize, Violation> {
    let initial = State::initial();
    let mut parent: HashMap<State, Option<(State, String)>> = HashMap::new();
    parent.insert(initial.clone(), None);
    let mut queue = VecDeque::from([initial]);
    let trace = |parent: &HashMap<State, Option<(State, String)>>, mut s: State| {
        let mut steps = Vec::new();
        while let Some(Some((prev, label))) = parent.get(&s) {
            steps.push(label.clone());
            s = prev.clone();
        }
        steps.reverse();
        steps
    };
    while let Some(s) = queue.pop_front() {
        if s.stop == Stop::Returned && s.stopper == Stopper::Host && s.callback_running() {
            return Err(Violation::CallbackAfterStop(trace(&parent, s)));
        }
        let next = successors(protocol, callback_calls_stop, &s);
        if next.is_empty() && !s.terminal() {
            return Err(Violation::Stuck(trace(&parent, s)));
        }
        for (label, t) in next {
            if !parent.contains_key(&t) {
                parent.insert(t.clone(), Some((s.clone(), label)));
                queue.push_back(t);
            }
        }
    }
    Ok(parent.len())
}

#[test]
fn gated_stop_never_returns_while_a_host_callback_runs() {
    let states = check(Protocol::Gated, false).unwrap();
    assert!(states > 50, "explored only {states} states");
}

#[test]
fn gated_stop_called_from_a_callback_cannot_deadlock() {
    check(Protocol::Gated, true).unwrap();
}

#[test]
fn model_replays_the_detached_stream_task_use_after_free() {
    let Err(Violation::CallbackAfterStop(trace)) = check(Protocol::Detached, false) else {
        panic!("the model no longer finds the #20 detached-task bug");
    };
    assert!(
        trace.contains(&"stop: drain complete".to_string()),
        "{trace:?}"
    );
}

#[test]
fn model_replays_the_drain_deadline_use_after_free() {
    let Err(Violation::CallbackAfterStop(trace)) = check(Protocol::TrackedUntilDeadline, false)
    else {
        panic!("the model no longer finds the drain-deadline bug");
    };
    assert!(
        trace.contains(&"stop: drain deadline expires".to_string()),
        "{trace:?}"
    );
}

#[test]
fn model_replays_the_stop_from_callback_deadlock() {
    let Err(Violation::Stuck(trace)) = check(Protocol::GatedWithoutReentrancyCheck, true) else {
        panic!("the model no longer finds the stop-from-callback deadlock");
    };
    assert!(
        trace.contains(&"worker 0: callback calls stop()".to_string()),
        "{trace:?}"
    );
}
