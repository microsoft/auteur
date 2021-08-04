//! Control the state of nodes

use crate::utils::ErrorMessage;
use actix::prelude::*;
use anyhow::{anyhow, Error};
use chrono::{DateTime, Utc};
use tracing::{instrument, trace};

use auteur_controlling::controller::State;

use crate::node::{NodeManager, NodeStatusMessage};
use crate::utils::get_now;

/// State machine governing the progression of a [`node`](crate::node) state
/// along a timeline
#[derive(Debug)]
pub struct StateMachine {
    /// The current state of the node
    pub state: State,
    /// When the node was scheduled to start
    pub cue_time: Option<DateTime<Utc>>,
    /// When the node was scheduled to end
    pub end_time: Option<DateTime<Utc>>,
    /// Scheduling timer
    handle: Option<SpawnHandle>,
}

impl Default for StateMachine {
    fn default() -> Self {
        Self {
            state: State::Initial,
            cue_time: None,
            end_time: None,
            handle: None,
        }
    }
}

/// Controls how to schedule future state changes after
/// a successful transtion
pub enum StateChangeResult {
    /// Wait for next time, if any, before progressing further
    Success,
    /// Proceed to the next state
    Skip,
}

/// (`Nodes`)[crate::node] should implement this trait in order to drive their
/// state progression.
///
/// IMPORTANT: this trait is only concerned about progressing the state along
/// a timeline, nodes are expected to deal with non-scheduled stops themselves,
/// by calling stop_scheduling and managing their state machine as they wish
/// on teardown, see Destination for an advanced case.
pub trait Schedulable<A>
where
    A: Actor<Context = Context<A>> + Schedulable<A> + Handler<ErrorMessage>,
{
    /// Reference to the node state machine
    fn state_machine(&self) -> &StateMachine;
    /// Mutable reference to the node state machine
    fn state_machine_mut(&mut self) -> &mut StateMachine;
    /// Progress to the target state
    ///
    /// State progresses linearly, but it is authorized to return
    /// to the Initial state from Starting and nodes should
    /// be prepared for this possibility, for example a prerolling source
    /// should tear down its media elements before progressing through
    /// the normal order again
    fn transition(
        &mut self,
        ctx: &mut Context<A>,
        target: State,
    ) -> Result<StateChangeResult, Error>;
    /// For tracing
    fn node_id(&self) -> &str;

    #[instrument(level = "trace", skip(self), fields(node_id = %self.node_id()))]
    fn next_time(&self) -> Option<DateTime<Utc>> {
        let machine = self.state_machine();

        match machine.state {
            State::Initial => machine.cue_time,
            State::Starting => machine.cue_time,
            State::Started => machine.end_time,
            State::Stopping => None,
            State::Stopped => None,
        }
    }

    /// Start scheduling state changes, valid only in Initial state
    #[instrument(level = "debug", skip(self, ctx), fields(node_id = %self.node_id()))]
    fn start_schedule(
        &mut self,
        ctx: &mut Context<A>,
        cue_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        let machine = self.state_machine_mut();
        let cue_time = cue_time.unwrap_or_else(get_now);

        if let Some(end_time) = end_time {
            if cue_time >= end_time {
                return Err(anyhow!("cue time >= end time"));
            }
        }

        match machine.state {
            State::Initial => {
                machine.cue_time = Some(cue_time);
                machine.end_time = end_time;

                self.do_schedule(ctx);

                Ok(())
            }
            _ => Err(anyhow!("can't start node with state {:?}", machine.state)),
        }
    }

    /// Reschedule state changes, fully valid in Initial and Starting states,
    /// only end_time may change in Started state, it is not permitted to
    /// reschedule in any other state
    #[instrument(level = "debug", skip(self, ctx), fields(node_id = %self.node_id()))]
    fn reschedule(
        &mut self,
        ctx: &mut Context<A>,
        cue_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        {
            match self.state_machine().state {
                State::Initial => Ok(()),
                State::Starting => Ok(()),
                State::Started => {
                    if cue_time.is_some() {
                        Err(anyhow!("can't change cue time when playing"))
                    } else {
                        Ok(())
                    }
                }
                State::Stopping => Err(anyhow!("can't reschedule when stopping")),
                State::Stopped => Err(anyhow!("can't reschedule when stopped")),
            }?;

            self.update_times(&cue_time, &end_time)?;
        }

        self.transition(ctx, State::Initial)?;

        self.update_state(State::Initial);

        Ok(())
    }

    /// Stop the scheduling loop
    #[instrument(level = "debug", skip(self, ctx), fields(node_id = %self.node_id()))]
    fn stop_schedule(&mut self, ctx: &mut Context<A>) {
        if let Some(handle) = self.state_machine_mut().handle.take() {
            trace!("cancelling current state scheduling");
            ctx.cancel_future(handle);
        }
    }

    fn update_state(&mut self, state: State) {
        self.state_machine_mut().state = state;
        let _ = NodeManager::from_registry().do_send(NodeStatusMessage::State {
            id: self.node_id().to_string(),
            state,
        });
    }

    /// Internal scheduling loop
    #[instrument(level = "trace", skip(self, ctx), fields(node_id = %self.node_id()))]
    fn do_schedule(&mut self, ctx: &mut Context<A>) {
        let next_time = self.next_time();

        let machine = self.state_machine_mut();

        if let Some(handle) = machine.handle.take() {
            trace!("cancelling current state scheduling");
            ctx.cancel_future(handle);
        }

        if let Some(next_time) = next_time {
            let now = get_now();

            let timeout = next_time - now;

            if timeout > chrono::Duration::zero() {
                trace!("not ready to progress to next state");

                machine.handle = Some(ctx.run_later(timeout.to_std().unwrap(), |s, ctx| {
                    s.do_schedule(ctx);
                }));
            } else {
                loop {
                    let machine = self.state_machine();

                    let target = match machine.state {
                        State::Initial => State::Starting,
                        State::Starting => State::Started,
                        State::Started => State::Stopping,
                        State::Stopping => State::Stopped,
                        State::Stopped => unreachable!(),
                    };

                    trace!("progressing to next state {:?}", target);

                    match self.transition(ctx, target) {
                        Ok(StateChangeResult::Skip) => {
                            trace!("Skipping state {:?}", target);
                            self.update_state(target);
                            continue;
                        }
                        Ok(StateChangeResult::Success) => {
                            trace!("Progressed to next state {:?}", target);
                            self.update_state(target);
                            self.do_schedule(ctx);
                            break;
                        }
                        Err(err) => {
                            ctx.notify(ErrorMessage(format!(
                                "Failed to change node state to {:?}: {:?}",
                                target, err
                            )));
                            break;
                        }
                    }
                }
            }
        } else {
            trace!("going back to sleep");
        }
    }

    /// Utility function to perform sanity checks on reschedule commands.
    ///
    /// Transactional, either updates all times or none.
    fn update_times(
        &mut self,
        new_cue_time: &Option<DateTime<Utc>>,
        new_end_time: &Option<DateTime<Utc>>,
    ) -> Result<(), Error> {
        let machine = self.state_machine_mut();

        if let Some(cue_time) = new_cue_time {
            if let Some(end_time) = new_end_time.as_ref().or_else(|| machine.end_time.as_ref()) {
                if end_time <= cue_time {
                    return Err(anyhow!("end_time {} <= cue_time {}", end_time, cue_time));
                }
            }
        }

        if let Some(end_time) = new_end_time {
            if let Some(cue_time) = new_cue_time.as_ref().or_else(|| machine.cue_time.as_ref()) {
                if end_time <= cue_time {
                    return Err(anyhow!("end_time {} <= cue_time {}", end_time, cue_time));
                }
            }
        }

        if let Some(cue_time) = new_cue_time {
            machine.cue_time = Some(*cue_time);
        }

        if let Some(end_time) = new_end_time {
            machine.end_time = Some(*end_time);
        }

        Ok(())
    }
}
