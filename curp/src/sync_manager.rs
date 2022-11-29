use std::collections::{HashMap, VecDeque};
use std::iter;
use std::ops::Range;
#[cfg(test)]
use std::sync::atomic::AtomicBool;
use std::{sync::Arc, time::Duration};

use clippy_utilities::NumericCast;
use event_listener::Event;
use futures::stream::FuturesUnordered;
use futures::StreamExt;

use madsim::rand::{thread_rng, Rng};
use parking_lot::lock_api::{Mutex, RwLockUpgradableReadGuard};
use parking_lot::{RawMutex, RwLock};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::time::Instant;
use tracing::{debug, error, info, warn};

use crate::cmd::{CommandExecutor, ProposeId};
use crate::rpc::{AppendEntriesRequest, VoteRequest};
use crate::server::{CmdBoardState, CmdBoardValue, Protocol, ServerRole, State};
use crate::util::{MutexMap, RwLockMap};
use crate::ServerId;
use crate::{
    channel::{key_mpsc::MpscKeyBasedReceiver, key_spmc::SpmcKeyBasedSender, RecvError},
    cmd::Command,
    log::LogEntry,
    message::TermNum,
    rpc::{self, Connect},
    LogIndex,
};

/// "sync task complete" message
pub(crate) struct SyncCompleteMessage<C>
where
    C: Command,
{
    /// The log index
    log_index: LogIndex,
    /// Command
    cmd: Arc<C>,
}

impl<C> SyncCompleteMessage<C>
where
    C: Command,
{
    /// Create a new `SyncCompleteMessage`
    fn new(log_index: LogIndex, cmd: Arc<C>) -> Self {
        Self { log_index, cmd }
    }

    /// Get Log Index
    pub(crate) fn log_index(&self) -> u64 {
        self.log_index
    }

    /// Get commands
    pub(crate) fn cmd(&self) -> Arc<C> {
        Arc::clone(&self.cmd)
    }
}

/// The message sent to the `SyncManager`
pub(crate) struct SyncMessage<C>
where
    C: Command,
{
    /// Term number
    term: TermNum,
    /// Command
    cmd: Arc<C>,
}

impl<C> SyncMessage<C>
where
    C: Command,
{
    /// Create a new `SyncMessage`
    pub(crate) fn new(term: TermNum, cmd: Arc<C>) -> Self {
        Self { term, cmd }
    }

    /// Get all values from the message
    fn inner(&mut self) -> (TermNum, Arc<C>) {
        (self.term, Arc::clone(&self.cmd))
    }
}

/// The manager to sync commands to other follower servers
pub(crate) struct SyncManager<C: Command + 'static, CE: 'static + CommandExecutor<C>> {
    /// Current state
    state: Arc<RwLock<State<C, CE>>>,
    /// Last time a rpc is received
    last_rpc_time: Arc<RwLock<Instant>>,
    /// Other addrs
    connects: Vec<Arc<Connect>>,
    /// Get cmd sync request from speculative command
    sync_chan: MpscKeyBasedReceiver<C::K, SyncMessage<C>>,
}

impl<C: Command + 'static, CE: 'static + CommandExecutor<C>> SyncManager<C, CE> {
    /// Create a `SyncedManager`
    pub(crate) async fn new(
        sync_chan: MpscKeyBasedReceiver<C::K, SyncMessage<C>>,
        others: HashMap<ServerId, String>,
        state: Arc<RwLock<State<C, CE>>>,
        last_rpc_time: Arc<RwLock<Instant>>,
        #[cfg(test)] reachable: Arc<AtomicBool>,
    ) -> Self {
        Self {
            sync_chan,
            state,
            connects: rpc::try_connect(
                others,
                #[cfg(test)]
                reachable,
            )
            .await,
            last_rpc_time,
        }
    }

    /// Get a clone of the connections
    fn connects(&self) -> Vec<Arc<Connect>> {
        self.connects.clone()
    }

    /// Try to receive all the messages in `sync_chan`, preparing for the batch sync.
    /// The term number comes from the command received.
    // TODO: set a maximum value
    async fn fetch_sync_msgs(&mut self) -> Result<(Vec<Arc<C>>, u64), ()> {
        let mut term = 0;
        let mut met_msg = false;
        let mut cmds = vec![];
        loop {
            let sync_msg = match self.sync_chan.try_recv() {
                Ok(sync_msg) => sync_msg,
                Err(RecvError::ChannelStop) => return Err(()),
                Err(RecvError::NoAvailable) => {
                    if met_msg {
                        break;
                    }

                    match self.sync_chan.async_recv().await {
                        Ok(msg) => msg,
                        Err(_) => return Err(()),
                    }
                }
                Err(RecvError::Timeout) => unreachable!("try_recv won't return timeout error"),
            };
            met_msg = true;
            let (t, cmd) = sync_msg.map_msg(SyncMessage::inner);
            term = t;
            cmds.push(cmd);
        }
        Ok((cmds, term))
    }

    /// Run the `SyncManager`
    pub(crate) async fn run(
        &mut self,
        comp_chan: SpmcKeyBasedSender<C::K, SyncCompleteMessage<C>>,
        cmd_executor: Arc<CE>,
        commit_trigger: UnboundedSender<()>,
        commit_trigger_rx: UnboundedReceiver<()>,
        spec: Arc<Mutex<RawMutex, VecDeque<C>>>,
    ) {
        // notify when a broadcast of append_entries is needed immediately(a new log is received or committed)
        let (ae_trigger, ae_trigger_rx) = tokio::sync::mpsc::unbounded_channel::<usize>();
        // notify when a calibration is needed
        let calibrate_trigger = Arc::new(Event::new());
        // TODO: gracefully stop the background tasks
        let _background_ae_handle = tokio::spawn(Self::bg_append_entries(
            self.connects(),
            Arc::clone(&self.state),
            commit_trigger,
            ae_trigger_rx,
        ));
        let _background_election_handle = tokio::spawn(Self::bg_election(
            self.connects(),
            Arc::clone(&self.state),
            Arc::clone(&self.last_rpc_time),
            Arc::clone(&calibrate_trigger),
            Arc::clone(&spec),
            ae_trigger.clone(),
        ));
        let _background_apply_handle = tokio::spawn(Self::bg_apply(
            Arc::clone(&self.state),
            cmd_executor,
            comp_chan,
            commit_trigger_rx,
            spec,
        ));
        let _background_heartbeat_handle =
            tokio::spawn(Self::bg_heartbeat(self.connects(), Arc::clone(&self.state)));
        let _calibrate_handle = tokio::spawn(Self::leader_calibrates_followers(
            self.connects(),
            Arc::clone(&self.state),
            calibrate_trigger,
        ));

        loop {
            let (cmds, term) = match self.fetch_sync_msgs().await {
                Ok((cmds, term)) => (cmds, term),
                Err(_) => return,
            };

            self.state.map_write(|mut state| {
                state.log.push(LogEntry::new(term, &cmds));
                if let Err(e) = ae_trigger.send(state.last_log_index()) {
                    error!("ae_trigger failed: {}", e);
                }

                debug!(
                    "received new log, index {:?}, contains {} cmds",
                    state.log.len().checked_sub(1),
                    cmds.len()
                );
            });
        }
    }

    /// Interval between sending heartbeats
    const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(150);
    /// Rpc request timeout
    const RPC_TIMEOUT: Duration = Duration::from_millis(50);

    /// Background `append_entries`, only works for the leader
    async fn bg_append_entries(
        connects: Vec<Arc<Connect>>,
        state: Arc<RwLock<State<C, CE>>>,
        commit_trigger: UnboundedSender<()>,
        mut ae_trigger_rx: UnboundedReceiver<usize>,
    ) {
        while let Some(i) = ae_trigger_rx.recv().await {
            if !state.read().is_leader() {
                continue;
            }

            // send append_entries to each server in parallel
            for connect in &connects {
                let _handle = tokio::spawn(Self::send_log_until_succeed(
                    i,
                    Arc::clone(connect),
                    Arc::clone(&state),
                    commit_trigger.clone(),
                ));
            }
        }
    }

    /// Send `append_entries` containing a single log to a server
    #[allow(clippy::integer_arithmetic, clippy::indexing_slicing)] // log.len() >= 1 because we have a fake log[0], indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
    async fn send_log_until_succeed(
        i: usize,
        connect: Arc<Connect>,
        state: Arc<RwLock<State<C, CE>>>,
        commit_trigger: UnboundedSender<()>,
    ) {
        // prepare append_entries request args
        #[allow(clippy::shadow_unrelated)] // clippy false positive
        let (term, id, prev_log_index, prev_log_term, entries, leader_commit) =
            state.map_read(|state| {
                (
                    state.term,
                    state.id(),
                    i - 1,
                    state.log[i - 1].term(),
                    vec![state.log[i].clone()],
                    state.commit_index,
                )
            });
        let req = match AppendEntriesRequest::new(
            term,
            id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        ) {
            Err(e) => {
                error!("unable to serialize append entries request: {}", e);
                return;
            }
            Ok(req) => req,
        };

        // send log[i] until succeed
        loop {
            debug!("append_entries sent to {}", connect.id);
            let resp = connect.append_entries(req.clone(), Self::RPC_TIMEOUT).await;

            #[allow(clippy::unwrap_used)]
            // indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
            match resp {
                Err(e) => {
                    warn!("append_entries error: {}", e);
                    // network error, wait some time until next retry
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                Ok(resp) => {
                    let resp = resp.into_inner();
                    let state = state.upgradable_read();

                    // calibrate term
                    if resp.term > state.term {
                        let mut state = RwLockUpgradableReadGuard::upgrade(state);
                        state.update_to_term(resp.term);
                        return;
                    }

                    if resp.success {
                        let mut state = RwLockUpgradableReadGuard::upgrade(state);
                        // update match_index and next_index
                        let match_index = state.match_index.get_mut(&connect.id).unwrap();
                        if *match_index < i {
                            *match_index = i;
                        }
                        *state.next_index.get_mut(&connect.id).unwrap() = *match_index + 1;

                        let min_replicated = (state.others.len() + 1) / 2;
                        // If the majority of servers has replicated the log, commit
                        if state.commit_index < i
                            && state.log[i].term() == state.term
                            && state
                                .others
                                .keys()
                                .filter(|addr| state.match_index[*addr] >= i)
                                .count()
                                >= min_replicated
                        {
                            state.commit_index = i;
                            debug!("commit_index updated to {i}");
                            if let Err(e) = commit_trigger.send(()) {
                                error!("commit_trigger failed: {}", e);
                            }
                        }
                        break;
                    }
                }
            }
        }
    }

    /// Background `append_entries`, only works for the leader
    async fn bg_heartbeat(connects: Vec<Arc<Connect>>, state: Arc<RwLock<State<C, CE>>>) {
        let role_trigger = state.read().role_trigger();
        #[allow(clippy::integer_arithmetic)] // tokio internal triggered
        loop {
            // only leader should run this task
            while !state.read().is_leader() {
                role_trigger.listen().await;
            }

            tokio::time::sleep(Self::HEARTBEAT_INTERVAL).await;

            // send heartbeat to each server in parallel
            for connect in &connects {
                let _handle = tokio::spawn(Self::send_heartbeat(
                    Arc::clone(connect),
                    Arc::clone(&state),
                ));
            }
        }
    }

    /// Send `append_entries` to a server
    #[allow(clippy::integer_arithmetic, clippy::indexing_slicing)] // log.len() >= 1 because we have a fake log[0], indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
    async fn send_heartbeat(connect: Arc<Connect>, state: Arc<RwLock<State<C, CE>>>) {
        // prepare append_entries request args
        #[allow(clippy::shadow_unrelated)] // clippy false positive
        let (term, id, prev_log_index, prev_log_term, leader_commit) = state.map_read(|state| {
            let next_index = state.next_index[&connect.id];
            (
                state.term,
                state.id(),
                next_index - 1,
                state.log[next_index - 1].term(),
                state.commit_index,
            )
        });
        let req = AppendEntriesRequest::new_heartbeat(
            term,
            id,
            prev_log_index,
            prev_log_term,
            leader_commit,
        );

        // send append_entries request and receive response
        // debug!("heartbeat sent to {}", connect.id);
        let resp = connect.append_entries(req, Self::RPC_TIMEOUT).await;

        #[allow(clippy::unwrap_used)]
        // indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
        match resp {
            Err(_e) => {
                //  warn!("append_entries error: {}", e)
            }
            Ok(resp) => {
                let resp = resp.into_inner();
                // calibrate term
                let state = state.upgradable_read();
                if resp.term > state.term {
                    let mut state = RwLockUpgradableReadGuard::upgrade(state);
                    state.update_to_term(resp.term);
                    return;
                }
                if !resp.success {
                    let mut state = RwLockUpgradableReadGuard::upgrade(state);
                    *state.next_index.get_mut(&connect.id).unwrap() -= 1;
                }
            }
        };
    }

    /// Background apply
    async fn bg_apply(
        state: Arc<RwLock<State<C, CE>>>,
        cmd_executor: Arc<CE>,
        comp_chan: SpmcKeyBasedSender<C::K, SyncCompleteMessage<C>>,
        mut commit_notify: UnboundedReceiver<()>,
        spec: Arc<Mutex<RawMutex, VecDeque<C>>>,
    ) {
        while commit_notify.recv().await.is_some() {
            let state = state.upgradable_read();
            if !state.need_commit() {
                continue;
            }

            let mut state = RwLockUpgradableReadGuard::upgrade(state);
            #[allow(clippy::integer_arithmetic, clippy::indexing_slicing)]
            // TODO: overflow of log index should be prevented
            for i in (state.last_applied + 1)..=state.commit_index {
                for cmd in state.log[i].cmds().iter() {
                    // TODO: execution of leaders and followers should be merged
                    // leader commits a log by sending it through compl_chan
                    if state.is_leader() {
                        if comp_chan
                            .send(
                                cmd.keys(),
                                SyncCompleteMessage::new(i.numeric_cast(), Arc::clone(cmd)),
                            )
                            .is_err()
                        {
                            error!("The comp_chan is closed on the remote side");
                            break;
                        }
                    } else {
                        // followers execute commands directly
                        let cmd_executor = Arc::clone(&cmd_executor);
                        let cmd = Arc::clone(cmd);
                        let spec = Arc::clone(&spec);
                        let _handle = tokio::spawn(async move {
                            // TODO: execute command in parallel
                            // TODO: handle execution error
                            let _execute_result = cmd.execute(cmd_executor.as_ref()).await;
                            let _after_sync_result = cmd
                                .after_sync(cmd_executor.as_ref(), i.numeric_cast())
                                .await;

                            // TODO: fixed in new commit
                            let _ignore = Protocol::<C, CE>::spec_remove_cmd(&spec, cmd.id());
                        });
                    }
                }
                state.last_applied = i;
                debug!("log[{i}] committed, last_applied updated to {}", i);
            }
        }
        info!("background apply stopped");
    }

    /// How long a candidate should wait before it starts another round of election
    const CANDIDATE_TIMEOUT: Duration = Duration::from_secs(1);
    /// How long a follower should wait before it starts a round of election (in millis)
    const FOLLOWER_TIMEOUT: Range<u64> = 300..500;

    /// Background election
    async fn bg_election(
        connects: Vec<Arc<Connect>>,
        state: Arc<RwLock<State<C, CE>>>,
        last_rpc_time: Arc<RwLock<Instant>>,
        calibrate_trigger: Arc<Event>,
        spec: Arc<Mutex<RawMutex, VecDeque<C>>>,
        ae_trigger: UnboundedSender<usize>,
    ) {
        let role_trigger = state.read().role_trigger();
        loop {
            // only follower or candidate should run this task
            while state.read().is_leader() {
                role_trigger.listen().await;
            }

            let current_role = state.read().role();
            let start_vote = match current_role {
                ServerRole::Follower => {
                    let timeout =
                        Duration::from_millis(thread_rng().gen_range(Self::FOLLOWER_TIMEOUT));
                    // wait until it needs to vote
                    loop {
                        let next_check = last_rpc_time.read().to_owned() + timeout;
                        tokio::time::sleep_until(next_check).await;
                        if Instant::now() - *last_rpc_time.read() > timeout {
                            break;
                        }
                    }
                    true
                }
                ServerRole::Candidate => loop {
                    let next_check = last_rpc_time.read().to_owned() + Self::CANDIDATE_TIMEOUT;
                    tokio::time::sleep_until(next_check).await;
                    // check election status
                    match state.read().role() {
                        // election failed, becomes a follower || election succeeded, becomes a leader
                        ServerRole::Follower | ServerRole::Leader => {
                            break false;
                        }
                        ServerRole::Candidate => {}
                    }
                    if Instant::now() - *last_rpc_time.read() > Self::CANDIDATE_TIMEOUT {
                        break true;
                    }
                },
                ServerRole::Leader => false, // leader should not vote
            };
            if !start_vote {
                continue;
            }

            *last_rpc_time.write() = Instant::now();
            debug!("server {} starts election", state.read().id());

            Self::candidate_sends_vote(
                connects.clone(),
                Arc::clone(&state),
                Arc::clone(&calibrate_trigger),
                Arc::clone(&spec),
                ae_trigger.clone(),
            )
            .await; // the await here won't block because we have rpc timeout
        }
    }

    /// send vote request
    #[allow(clippy::too_many_lines, clippy::shadow_unrelated)] // TODO: split it
    async fn candidate_sends_vote(
        connects: Vec<Arc<Connect>>,
        state: Arc<RwLock<State<C, CE>>>,
        calibrate_trigger: Arc<Event>,
        spec: Arc<Mutex<RawMutex, VecDeque<C>>>,
        ae_trigger: UnboundedSender<usize>,
    ) {
        // start election
        #[allow(clippy::integer_arithmetic)] // TODO: handle possible overflow
        let req = {
            let mut state = state.write();
            let new_term = state.term + 1;
            state.update_to_term(new_term);
            state.set_role(ServerRole::Candidate);
            state.recovery = true;
            state.voted_for = Some(state.id());
            state.votes_received = 1;
            VoteRequest::new(
                state.term,
                state.id(),
                state.last_log_index(),
                state.last_log_term(),
            )
        };

        let mut rpcs: FuturesUnordered<_> = connects
            .into_iter()
            .zip(iter::repeat(req))
            .map(|(connect, request)| async move {
                (connect.id(), connect.vote(request, Self::RPC_TIMEOUT).await)
            })
            .collect();

        let mut spec_pools = vec![spec.lock().drain(..).collect()];
        while let Some((id, resp)) = rpcs.next().await {
            match resp {
                Err(e) => error!("vote failed, {}", e),
                Ok(resp) => {
                    let resp = resp.into_inner();

                    // calibrate term
                    let state_lock = Arc::clone(&state);
                    let state = state.upgradable_read();
                    if resp.term > state.term {
                        let mut state = RwLockUpgradableReadGuard::upgrade(state);
                        state.update_to_term(resp.term);
                        return;
                    }

                    // is still a candidate
                    if !matches!(state.role(), ServerRole::Candidate) {
                        return;
                    }

                    #[allow(clippy::integer_arithmetic)]
                    if resp.vote_granted {
                        debug!("vote is granted by server {}", id);
                        let mut state = RwLockUpgradableReadGuard::upgrade(state);
                        state.votes_received += 1;

                        let follower_spec_pool = match resp.spec_pool() {
                            Err(e) => {
                                error!("get vote response spec pool failed, {e}");
                                return;
                            }
                            Ok(spec_pool) => spec_pool,
                        };
                        spec_pools.push(follower_spec_pool);

                        // the majority has granted the vote
                        let min_granted = (state.others.len() + 1) / 2 + 1;
                        if state.votes_received >= min_granted {
                            state.set_role(ServerRole::Leader);
                            state.recovery = false;
                            state.leader_id = Some(state.id());
                            debug!("server {} becomes leader", state.id());

                            // init next_index
                            let last_log_index = state.last_log_index();
                            for index in state.next_index.values_mut() {
                                *index = last_log_index + 1; // iter from the end to front is more likely to match the follower
                            }

                            // recover possibly speculatively executed cmds
                            let recovered_cmds =
                                Self::recover_possibly_executed_cmds(spec_pools, min_granted);
                            if !recovered_cmds.is_empty() {
                                // recovered command will be treated as speculatively executed commands
                                // put them in spec pool and cmd_board
                                state.cmd_board.map_lock(|mut cmd_board| {
                                    for cmd in &recovered_cmds {
                                        let old_value = cmd_board.insert(
                                            cmd.id().clone(),
                                            CmdBoardValue::new_wait_sync(
                                                Event::new(),
                                                CmdBoardState::NeedExecute,
                                            ),
                                        );

                                        if let Some(CmdBoardValue::EarlyArrive(event)) = old_value {
                                            // FIXME: Maybe more than one waiting?
                                            event.notify(1);
                                        }
                                    }
                                });
                                spec.map_lock(|mut spec| {
                                    spec.extend(recovered_cmds.iter().cloned());
                                });

                                // execute them and sync
                                let ce = Arc::clone(&state.cmd_executor);
                                let ae_trigger = ae_trigger.clone();
                                let _exe = tokio::spawn(async move {
                                    // TODO: execute in parallel, handle error(though currently, the client has no way to get the execute results of recovered commands)
                                    for cmd in &recovered_cmds {
                                        let _er = cmd.execute(ce.as_ref()).await;
                                    }
                                    debug!(
                                        "recovered commands have finished execution, start sync"
                                    );
                                    let wrapped: Vec<_> = recovered_cmds
                                        .into_iter()
                                        .map(|cmd| Arc::new(cmd))
                                        .collect();
                                    let mut state = state_lock.write();
                                    let term = state.term;
                                    state.log.push(LogEntry::new(term, &wrapped));
                                    if let Err(e) = ae_trigger.send(state.last_log_index()) {
                                        warn!("can't trigger ae, {e}");
                                    }
                                });
                            }

                            // trigger calibrate
                            calibrate_trigger.notify(1);

                            return; // other servers' response no longer matters
                        }
                    }
                }
            }
        }
    }

    /// Recover possibly speculatively executed cmds from spec pools
    #[allow(clippy::integer_arithmetic)] // won't overflow here
    fn recover_possibly_executed_cmds(spec_pools: Vec<Vec<C>>, majority_cnt: usize) -> Vec<C> {
        let mut cmd_cnt: HashMap<ProposeId, (C, usize)> = HashMap::new();
        for cmd in spec_pools.into_iter().flatten() {
            let entry = cmd_cnt.entry(cmd.id().clone()).or_insert((cmd, 0));
            entry.1 += 1;
        }

        // get all possibly executed(fast round) commands: the majority of the majority
        let min_appearances = majority_cnt / 2 + 1;
        let mut log = vec![];
        for cmd in cmd_cnt
            .into_values()
            .filter_map(|(cmd, cnt)| (cnt >= min_appearances).then(|| cmd))
        {
            debug!("new leader recovers command {:?}", cmd.id());
            log.push(cmd);
        }

        log
    }

    /// Leader should first enforce followers to be consistent with it when it comes to power
    #[allow(clippy::integer_arithmetic, clippy::indexing_slicing)] // log.len() >= 1 because we have a fake log[0], indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
    async fn leader_calibrates_followers(
        connects: Vec<Arc<Connect>>,
        state: Arc<RwLock<State<C, CE>>>,
        calibrate_trigger: Arc<Event>,
    ) {
        loop {
            calibrate_trigger.listen().await;

            for connect in connects.iter().cloned() {
                let state = Arc::clone(&state);
                let _handle = tokio::spawn(async move {
                    loop {
                        // send append entry
                        #[allow(clippy::shadow_unrelated)] // clippy false positive
                        let (
                            term,
                            id,
                            prev_log_index,
                            prev_log_term,
                            entries,
                            leader_commit,
                            last_sent_index,
                        ) = state.map_read(|state| {
                            let next_index = state.next_index[&connect.id];
                            (
                                state.term,
                                state.id(),
                                next_index - 1,
                                state.log[next_index - 1].term(),
                                state.log[next_index..].to_vec(),
                                state.commit_index,
                                state.log.len() - 1,
                            )
                        });
                        let req = match AppendEntriesRequest::new(
                            term,
                            id,
                            prev_log_index,
                            prev_log_term,
                            entries,
                            leader_commit,
                        ) {
                            Err(e) => {
                                error!("unable to serialize append entries request: {}", e);
                                return;
                            }
                            Ok(req) => req,
                        };

                        let resp = connect.append_entries(req, Self::RPC_TIMEOUT).await;

                        #[allow(clippy::unwrap_used)]
                        // indexing of `next_index` or `match_index` won't panic because we created an entry when initializing the server state
                        match resp {
                            Err(e) => {
                                warn!("append_entries error: {}", e);
                                // network error, wait some time until next retry
                                tokio::time::sleep(Duration::from_secs(1)).await;
                            }
                            Ok(resp) => {
                                let resp = resp.into_inner();
                                // calibrate term
                                let state = state.upgradable_read();
                                if resp.term > state.term {
                                    let mut state = RwLockUpgradableReadGuard::upgrade(state);
                                    state.update_to_term(resp.term);
                                    return;
                                }

                                let mut state = RwLockUpgradableReadGuard::upgrade(state);

                                // successfully calibrate
                                if resp.success {
                                    *state.next_index.get_mut(&connect.id).unwrap() =
                                        last_sent_index + 1;
                                    *state.match_index.get_mut(&connect.id).unwrap() =
                                        last_sent_index;
                                    debug!("new leader successfully calibrates {:?}", connect.id());
                                    break;
                                }

                                *state.next_index.get_mut(&connect.id).unwrap() -= 1;
                            }
                        };
                    }
                });
            }
        }
    }

    // /// Leader collects spec pool of followers
    // #[allow(clippy::integer_arithmetic, clippy::indexing_slicing)]
    // async fn leader_collects_spec_pool(
    //     connects: Vec<Arc<Connect>>,
    //     state: Arc<RwLock<State<C>>>,
    //     spec: Arc<Mutex<RawMutex, VecDeque<C>>>,
    // ) {
    //     let majority_cnt = (connects.len() + 1) / 2 + 1;

    //     // speculative pools of the majority of followers
    //     let mut spec_pools: Vec<Vec<C>> = connects
    //         .iter()
    //         .cloned()
    //         .zip(std::iter::repeat_with(|| Arc::clone(&state)))
    //         .map(|(connect, state)| async move {
    //             let (id, term) = state.map_read(|state| (state.id(), state.term));
    //             connect
    //                 .collect_spec_pool(
    //                     CollectSpecPoolRequest::new(term, id),
    //                     Duration::from_secs(1),
    //                 )
    //                 .await
    //         })
    //         .collect::<FuturesUnordered<_>>() // Send concurrently
    //         .filter_map(|resp| {
    //             future::ready(match resp {
    //                 Ok(resp) => {
    //                     let resp = resp.into_inner();
    //                     if resp.succeed {
    //                         resp.cmds().map_or_else(
    //                             |e| {
    //                                 warn!("cmds deserialize failed, {e}");
    //                                 None
    //                             },
    //                             |cmds| Some(cmds),
    //                         )
    //                     } else {
    //                         None
    //                     }
    //                 }
    //                 Err(e) => {
    //                     warn!("collect spec pool failed, {e}");
    //                     None
    //                 }
    //             })
    //         }) // filter out failed requests
    //         .take((connects.len() + 1) / 2) // majority
    //         .collect()
    //         .await;
    //     spec_pools.push(spec.map_lock(|mut spec| spec.drain(..).collect_vec())); // add self spec pool
    //     if spec_pools.len() < majority_cnt {
    //         return;
    //     }

    //     // count the number of each command's appearances in spec pools
    //     let mut cmd_cnt: HashMap<ProposeId, (C, usize)> = HashMap::new();
    //     for cmd in spec_pools.into_iter().flatten() {
    //         let entry = cmd_cnt.entry(cmd.id().to_owned()).or_insert((cmd, 0));
    //         entry.1 += 1;
    //     }

    //     // get all possibly executed(fast round) commands: the majority of the majority
    //     let min_appearances = majority_cnt / 2 + 1;
    //     let mut log = vec![];
    //     for cmd in cmd_cnt
    //         .into_iter()
    //         .filter_map(|(id, (cmd, cnt))| (cnt >= min_appearances).then(|| cmd))
    //     {
    //         log.push(Arc::new(cmd));
    //     }

    //     let mut state = state.write();
    //     let term = state.term;
    //     state.log.push(LogEntry::new(term, &log));
    // }
}
