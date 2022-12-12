use std::{collections::HashMap, ops::Range, time::Duration};

use crate::{
    cmd::{Command, CommandExecutor},
    message::ServerId,
    server::Rpc,
};

/// Server Config
#[derive(Debug)]
pub struct Config {
    /// Server id
    id: ServerId,
    /// Others
    others: HashMap<ServerId, String>,
    /// How long we should wait before we retry if current rpc fails
    retry_timeout: Duration,
    /// Interval between sending heartbeats
    heartbeat_interval: Duration,
    /// Rpc request timeout
    rpc_timeout: Duration,
    /// How long a candidate should wait before it starts another round of election
    candidate_timeout: Duration,
    /// How long a follower should wait before it starts a round of election if it fails to receive heartbeat from the leader (in millis)
    follower_timeout_range: Range<u64>,
}

impl Config {
    /// Create a default config
    /// The default configuration works for network conditions with around 150ms latency
    #[inline]
    #[must_use]
    pub fn new(id: ServerId, others: HashMap<ServerId, String>) -> Self {
        Self {
            id,
            others,
            retry_timeout: Duration::from_millis(500),
            heartbeat_interval: Duration::from_millis(800),
            rpc_timeout: Duration::from_millis(500),
            candidate_timeout: Duration::from_secs(3),
            follower_timeout_range: 1000..2000,
        }
    }

    /// Set retry timeout: How long we should wait before we retry if current rpc fails
    #[inline]
    #[must_use]
    pub fn set_retry_timeout(mut self, retry_timeout: Duration) -> Self {
        self.retry_timeout = retry_timeout;
        self
    }

    /// Set heartbeat interval: Interval between sending heartbeats
    #[inline]
    #[must_use]
    pub fn set_heartbeat_interval(mut self, heartbeat_interval: Duration) -> Self {
        self.heartbeat_interval = heartbeat_interval;
        self
    }

    /// Set rpc timeout: Rpc request timeout
    #[inline]
    #[must_use]
    pub fn set_rpc_timeout(mut self, rpc_timeout: Duration) -> Self {
        self.rpc_timeout = rpc_timeout;
        self
    }

    /// Set candidate timeout: How long a candidate should wait before it starts another round of election
    #[inline]
    #[must_use]
    pub fn set_candidate_timeout(mut self, candidate_timeout: Duration) -> Self {
        self.candidate_timeout = candidate_timeout;
        self
    }

    /// Set follower timeout range: How long a follower should wait before it starts a round of election if it fails to receive heartbeat from the leader (in millis)
    #[inline]
    #[must_use]
    pub fn set_follower_timeout_range(mut self, follower_timeout_range: Range<u64>) -> Self {
        self.follower_timeout_range = follower_timeout_range;
        self
    }

    /// Build a server, will run bg tasks
    #[inline]
    #[must_use]
    pub fn build<C: Command + 'static, CE: CommandExecutor<C> + 'static>(
        self,
        is_leader: bool,
        cmd_executor: CE,
    ) -> Rpc<C> {
        Rpc::new(self, is_leader, cmd_executor)
    }

    /// Get the server id
    #[inline]
    #[must_use]
    pub fn id(&self) -> &ServerId {
        &self.id
    }

    /// Get the retry timeout
    #[inline]
    #[must_use]
    pub fn retry_timeout(&self) -> Duration {
        self.retry_timeout
    }

    /// Get the heartbeat interval
    #[inline]
    #[must_use]
    pub fn heartbeat_interval(&self) -> Duration {
        self.heartbeat_interval
    }

    /// Get the rpc timeout
    #[inline]
    #[must_use]
    pub fn rpc_timeout(&self) -> Duration {
        self.rpc_timeout
    }

    /// Get the candidate timeout
    #[inline]
    #[must_use]
    pub fn candidate_timeout(&self) -> Duration {
        self.candidate_timeout
    }

    /// Get follower timeout range
    #[inline]
    #[must_use]
    pub fn follower_timeout_range(&self) -> Range<u64> {
        self.follower_timeout_range.clone()
    }

    /// Get others
    #[inline]
    #[must_use]
    pub fn others(&self) -> &HashMap<ServerId, String> {
        &self.others
    }
}
