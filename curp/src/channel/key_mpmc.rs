//! This is a single-producer and multi-consumer channel.
//!
//! The channel has the following features:
//! 1. The message coupled with keys, any two message with conflicting keys are conflicted.
//! 2. Any message send to the channel is control by a done token returned by the receiver API.
//! 3. Undone message blocks all the following conflict messages

#![allow(dead_code)]

use std::{
    cmp::Eq,
    collections::{HashMap, HashSet, VecDeque},
    hash::Hash,
    sync::Arc,
    time::Duration,
};

use clippy_utilities::{NumericCast, OverflowArithmetic};
use event_listener::Event;
use parking_lot::Mutex;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

use super::{
    hash_eq, KeyBasedChannel, KeyBasedReceiverInner, KeyBasedSenderInner, KeysMessageInner,
    RecvError, SendError,
};
use crate::cmd::ConflictCheck;

/// Keys and Messages combined structure
pub(crate) struct MpmcKeyBasedMessage<K, M> {
    /// Inner
    inner: Arc<KeysMessageInner<K, M>>,
}

hash_eq!(MpmcKeyBasedMessage);

impl<K, M> Clone for MpmcKeyBasedMessage<K, M> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

#[allow(unsafe_code)]
unsafe impl<K: Send, M: Send> Send for MpmcKeyBasedMessage<K, M> {}

impl<K, M> MpmcKeyBasedMessage<K, M> {
    /// Create a new `KeysMessage`
    fn new(keys: Vec<K>, message: M) -> Self {
        Self {
            inner: Arc::new(KeysMessageInner::new(keys, message)),
        }
    }

    /// modify data in the message if necessary
    pub(crate) fn keys(&self) -> &[K] {
        self.inner.keys()
    }

    /// map the inner message to a closure
    pub(crate) fn map_msg<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut M) -> R,
    {
        self.inner.map_msg(f)
    }
}

impl<K: Eq + Hash + Clone + ConflictCheck, M> KeyBasedChannel<MpmcKeyBasedMessage<K, M>> {
    /// Insert successors and predecessors info and return if it conflicts with any previous message
    fn insert_graph(&mut self, new_km: MpmcKeyBasedMessage<K, M>) -> bool {
        let predecessor_cnt: u64 = self
            .successor
            .iter_mut()
            .filter_map(|(k, v)| {
                super::keys_conflict(k.keys(), new_km.keys()).then(|| {
                    let _ignore = v.insert(new_km.clone());
                })
            })
            .count()
            .numeric_cast();
        // the message can only be inserted once, so we ignore the return value
        let _ignore = self.successor.insert(new_km.clone(), HashSet::new());

        if predecessor_cnt == 0 {
            false
        } else {
            // the message can only be inserted once, so we ignore the return value
            let _ignore2 = self.predecessor.insert(new_km, predecessor_cnt);
            true
        }
    }

    /// Append a key and message to this `KeyBasedChannel`
    fn append(&mut self, keys: &[K], msg: M) {
        let km = MpmcKeyBasedMessage::new(keys.to_vec(), msg);
        if !self.insert_graph(km.clone()) {
            self.inner.push_back(km);
            self.new_msg_event.notify(1);
        }
    }

    /// Move a message for the `key` from pending to inner
    fn mark_done(&mut self, km: &MpmcKeyBasedMessage<K, M>) {
        let ready_cnt = if let Some(successor) = self.successor.remove(km) {
            successor
                .into_iter()
                .map(|s| {
                    let (no_predecessor, has_pre) = if let Some(s_p) = self.predecessor.get_mut(&s)
                    {
                        *s_p = s_p.overflow_sub(1);
                        (*s_p == 0, true)
                    } else {
                        (true, false)
                    };

                    if no_predecessor && has_pre {
                        let _ignore_removed = self.predecessor.remove(&s);
                    }

                    if no_predecessor {
                        self.inner.push_back(s);
                        1
                    } else {
                        0
                    }
                })
                .sum()
        } else {
            0
        };

        if ready_cnt > 0 {
            self.new_msg_event.notify(ready_cnt);
        }
    }
}

/// The sender inner type
type MpmcKeyBasedSenderInner<K, M> = KeyBasedSenderInner<MpmcKeyBasedMessage<K, M>>;

/// The Sender for the `KeyBasedChannel`
pub(crate) struct MpmcKeyBasedSender<K, M> {
    /// inner sender
    inner: Arc<MpmcKeyBasedSenderInner<K, M>>,
}

impl<K, M> Clone for MpmcKeyBasedSender<K, M> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<K: Eq + Hash + Clone + ConflictCheck + Send + 'static, M: Send + 'static>
    MpmcKeyBasedSender<K, M>
{
    /// Send a key and message to the channel
    pub(crate) fn send(&self, keys: &[K], msg: M) -> Result<(), SendError> {
        let mut channel = self.inner.channel.lock();

        if !channel.is_working {
            return Err(SendError::ChannelStop);
        }

        channel.append(keys, msg);
        Ok(())
    }
}

/// The Receiver for the `KeyBasedChannel`
pub(crate) struct MpmcKeyBasedReceiver<K, M> {
    /// Inner receiver
    inner: Arc<tokio::sync::Mutex<KeyBasedReceiverInner<MpmcKeyBasedMessage<K, M>>>>,
    /// Message done notifier
    done_tx: UnboundedSender<MpmcKeyBasedMessage<K, M>>,
}

impl<K, M> Clone for MpmcKeyBasedReceiver<K, M> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            done_tx: self.done_tx.clone(),
        }
    }
}

impl<K: Eq + Hash + Clone + ConflictCheck, M> MpmcKeyBasedReceiver<K, M> {
    /// Receive a message
    /// Return (message, `msg_complete_sender`)
    pub(crate) async fn recv(
        &self,
    ) -> Result<
        (
            MpmcKeyBasedMessage<K, M>,
            UnboundedSender<MpmcKeyBasedMessage<K, M>>,
        ),
        RecvError,
    > {
        self.inner
            .lock()
            .await
            .async_recv()
            .await
            .map(|msg| (msg, self.done_tx.clone()))
    }

    /// Receive a message with a `timeout`.
    /// Return None if `timeout` hits,
    /// otherwise return Some((message, `msg_complete_sender`))
    #[allow(dead_code, clippy::expect_used, clippy::unwrap_in_result)]
    pub(crate) async fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> Result<
        (
            MpmcKeyBasedMessage<K, M>,
            UnboundedSender<MpmcKeyBasedMessage<K, M>>,
        ),
        RecvError,
    > {
        self.inner
            .lock()
            .await
            .recv_timeout(timeout)
            .map(|msg| (msg, self.done_tx.clone()))
    }
}

/// Create a `KeyBasedQueue`
/// Return (sender, receiver)
pub(crate) fn channel<
    K: Clone + Eq + Hash + Send + Sync + ConflictCheck + 'static,
    M: Send + 'static,
>() -> (MpmcKeyBasedSender<K, M>, MpmcKeyBasedReceiver<K, M>) {
    let inner_channel = Arc::new(Mutex::new(KeyBasedChannel {
        inner: VecDeque::new(),
        new_msg_event: Event::new(),
        predecessor: HashMap::new(),
        successor: HashMap::new(),
        is_working: true,
    }));

    let (done_tx, mut done_rx) = unbounded_channel();

    let channel4complete = Arc::clone(&inner_channel);

    let _ignore_handler = tokio::spawn(async move {
        while let Some(msg) = done_rx.recv().await {
            channel4complete.lock().mark_done(&msg);
        }
    });

    (
        MpmcKeyBasedSender {
            inner: Arc::new(KeyBasedSenderInner {
                channel: Arc::clone(&inner_channel),
            }),
        },
        MpmcKeyBasedReceiver {
            inner: Arc::new(tokio::sync::Mutex::new(KeyBasedReceiverInner {
                channel: inner_channel,
                buf: VecDeque::new(),
            })),
            done_tx,
        },
    )
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use super::channel;
    use crate::channel::RecvError;

    #[tokio::test]
    #[allow(unused_results)]
    async fn test_multi() {
        let (tx, rx) = channel::<String, String>();
        let tx1 = tx.clone();
        let tx2 = tx.clone();
        let rx1 = rx.clone();
        let rx2 = rx.clone();
        drop(rx);

        tokio::spawn(async move {
            tx1.send(&["1".to_owned()], "A".to_owned()).unwrap();
        });
        tokio::spawn(async move {
            tx2.send(&["1".to_owned()], "B".to_owned()).unwrap();
        });

        tokio::spawn(async move {
            let (msg, done) = rx1.recv().await.unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
            if done.send(msg).is_err() {
                panic!("send err");
            }
        });
        tokio::spawn(async move {
            if rx2.recv_timeout(Duration::from_millis(500)).await.is_ok() {
                panic!("should timeout")
            }
            let (msg, done) = rx2.recv().await.expect("fail to mark done");
            if done.send(msg).is_err() {
                panic!("send err");
            }
        })
        .await
        .unwrap();
    }

    #[allow(clippy::expect_used, unused_must_use)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_channel_in_order() {
        let (tx, rx) = channel::<String, String>();

        tx.send(&["1".to_owned()], "A".to_owned());
        tx.send(&["1".to_owned()], "B".to_owned());
        tx.send(&["2".to_owned()], "C".to_owned());
        let (first, f_done) = rx.recv().await.expect("first message should recv success");
        first.map_msg(|msg| {
            assert_eq!(*msg, "A");
        });

        let (third, _) = rx.recv().await.expect("third message should recv success");
        third.map_msg(|msg| {
            assert_eq!(*msg, "C");
        });

        assert!(matches!(
            rx.recv_timeout(Duration::from_secs(1)).await,
            Err(RecvError::Timeout)
        ));
        f_done.send(first);
        let (second, _) = rx.recv().await.expect("second message should recv success");
        second.map_msg(|msg| {
            assert_eq!(*msg, "B");
        });
    }

    // Test the receiver should not block the thread. A bug was found that the receiver might block the whole tokio worker thread when recv().await is called. This test verifies that it was fixed.
    // The test will be blocked should the async_recv() in SpmcKeyBasedReceiver::recv() is changed to recv().
    // The lesson here is that l.wait() should only be called in non-async code. If it is called in async code, it will not hand the control flow back to tokio runtime like l.await and, therefore, block the tokio worker thread.
    #[tokio::test]
    async fn recv_no_blocking() {
        let (tx, rx) = channel::<String, String>();
        let _ignored_handle = tokio::spawn(async move {
            let _ignore = rx.recv().await; // should not block
        });
        tokio::time::sleep(Duration::from_millis(500)).await; // make sure the rx.recv is called
        let _ignore = tx.send(&["hello".to_owned()], "world".to_owned());
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}
