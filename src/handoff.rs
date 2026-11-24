use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

/// Passes a value from `authenticate()` to `provide_user_detail()`, keyed by username.
///
/// libunftp's `Principal` carries only a username, so anything the authenticator
/// establishes (here, the Kafka login) must be deposited here and claimed by the
/// provider. Items are queued per username; any item will do, but each is claimed once.
///
/// An item that is not claimed within the handoff's time limit is dropped, which
/// releases whatever it holds (here, an open Kafka connection). This covers a login
/// abandoned between the two calls, for example by a client that disconnects mid-PASS.
/// Expiry is lazy: expired items are dropped at the start of the next `deposit` or
/// `claim`, so on an idle server an abandoned item lives until the next login.
/// Queues are bounded by age and never by length, because a length cap would drop a
/// legitimate concurrent login by the same user.
///
/// Cloning yields another handle on the same queues.
pub struct Handoff<T> {
    queues: Arc<Mutex<Queues<T>>>,
    time_limit: Duration,
}

type Queues<T> = HashMap<String, Vec<Waiting<T>>>;

struct Waiting<T> {
    deposited_at: Instant,
    item: T,
}

impl<T> Clone for Handoff<T> {
    fn clone(&self) -> Self {
        Handoff {
            queues: Arc::clone(&self.queues),
            time_limit: self.time_limit,
        }
    }
}

impl<T> Handoff<T> {
    /// Creates a handoff with nothing waiting, in which an item left unclaimed for
    /// `time_limit` is dropped.
    pub fn new(time_limit: Duration) -> Self {
        Handoff {
            queues: Arc::new(Mutex::new(HashMap::new())),
            time_limit,
        }
    }

    /// Queues `item` for `username` until a later `claim` takes it or the time limit
    /// passes, and drops any items that have already outlived the limit.
    pub fn deposit(&self, username: &str, item: T) {
        self.deposit_at(Instant::now(), username, item);
    }

    /// Takes the most recently deposited item for `username` that is still within the
    /// time limit, or `None` if nothing is waiting for it. Items that have outlived the
    /// limit are dropped, never returned.
    ///
    /// The username's queue is removed with its last item, so the handoff holds only
    /// items still awaiting a claim.
    pub fn claim(&self, username: &str) -> Option<T> {
        self.claim_at(Instant::now(), username)
    }

    fn deposit_at(&self, now: Instant, username: &str, item: T) {
        let expired = {
            let mut queues = self.lock();
            let expired = self.remove_expired(&mut queues, now);
            queues
                .entry(username.to_string())
                .or_default()
                .push(Waiting {
                    deposited_at: now,
                    item,
                });
            expired
        };
        // Dropping a Kafka producer can block while it shuts down, so expired items
        // must not be dropped while the lock is held.
        drop(expired);
    }

    fn claim_at(&self, now: Instant, username: &str) -> Option<T> {
        let (result, expired) = {
            let mut queues = self.lock();
            let expired = self.remove_expired(&mut queues, now);
            (Self::pop(&mut queues, username), expired)
        };
        drop(expired);
        result
    }

    fn pop(queues: &mut Queues<T>, username: &str) -> Option<T> {
        let queue = queues.get_mut(username)?;
        let result = queue.pop().map(|waiting| waiting.item);
        if queue.is_empty() {
            queues.remove(username);
        }
        result
    }

    fn remove_expired(&self, queues: &mut Queues<T>, now: Instant) -> Vec<Waiting<T>> {
        let mut result = Vec::new();
        queues.retain(|_, queue| {
            let (live, expired): (Vec<_>, Vec<_>) =
                std::mem::take(queue).into_iter().partition(|waiting| {
                    now.saturating_duration_since(waiting.deposited_at) < self.time_limit
                });
            *queue = live;
            result.extend(expired);
            !queue.is_empty()
        });
        result
    }

    /// A panic elsewhere while the lock was held cannot leave the map
    /// structurally invalid, so a poisoned lock is safe to keep using.
    fn lock(&self) -> MutexGuard<'_, Queues<T>> {
        self.queues
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[cfg(test)]
    fn usernames_waiting(&self) -> usize {
        self.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    const TIME_LIMIT: Duration = Duration::from_secs(5);

    fn handoff<T>() -> Handoff<T> {
        Handoff::new(TIME_LIMIT)
    }

    struct SetsFlagWhenDropped(Arc<AtomicBool>);

    impl Drop for SetsFlagWhenDropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    fn unclaimed_item_deposited_at(
        handoff: &Handoff<SetsFlagWhenDropped>,
        at: Instant,
    ) -> Arc<AtomicBool> {
        let dropped = Arc::new(AtomicBool::new(false));
        handoff.deposit_at(at, "alice", SetsFlagWhenDropped(Arc::clone(&dropped)));
        dropped
    }

    #[test]
    fn an_item_unclaimed_past_the_time_limit_is_dropped_by_the_next_deposit() {
        let handoff = handoff();
        let start = Instant::now();
        let dropped = unclaimed_item_deposited_at(&handoff, start);

        handoff.deposit_at(
            start + TIME_LIMIT * 2,
            "bob",
            SetsFlagWhenDropped(Arc::default()),
        );

        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(handoff.usernames_waiting(), 1);
    }

    #[test]
    fn a_claim_past_the_time_limit_drops_the_item_instead_of_returning_it() {
        let handoff = handoff();
        let start = Instant::now();
        let dropped = unclaimed_item_deposited_at(&handoff, start);

        let claimed = handoff.claim_at(start + TIME_LIMIT * 2, "alice");

        assert!(claimed.is_none());
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(handoff.usernames_waiting(), 0);
    }

    #[test]
    fn an_item_within_the_time_limit_is_claimable_and_survives_other_deposits() {
        let handoff = handoff();
        let start = Instant::now();
        let dropped = unclaimed_item_deposited_at(&handoff, start);
        let within_limit = start + TIME_LIMIT / 2;

        handoff.deposit_at(within_limit, "bob", SetsFlagWhenDropped(Arc::default()));

        assert!(!dropped.load(Ordering::SeqCst));
        assert!(handoff.claim_at(within_limit, "alice").is_some());
    }

    #[test]
    fn a_deposited_item_is_claimed_for_its_username_only() {
        let handoff = handoff();
        handoff.deposit("alice", 1);

        assert_eq!(handoff.claim("bob"), None);
        assert_eq!(handoff.claim("alice"), Some(1));
        assert_eq!(handoff.claim("alice"), None);
    }

    #[test]
    fn items_deposited_for_one_username_are_claimed_newest_first() {
        let handoff = handoff();
        handoff.deposit("alice", 1);
        handoff.deposit("alice", 2);

        assert_eq!(handoff.claim("alice"), Some(2));
        assert_eq!(handoff.claim("alice"), Some(1));
    }

    #[test]
    fn claiming_the_last_item_for_a_username_forgets_the_username() {
        let handoff = handoff();
        handoff.deposit("alice", 1);
        handoff.deposit("alice", 2);
        handoff.deposit("bob", 3);

        handoff.claim("alice");
        assert_eq!(handoff.usernames_waiting(), 2);
        handoff.claim("alice");
        handoff.claim("bob");

        assert_eq!(handoff.usernames_waiting(), 0);
    }

    #[test]
    fn a_panic_while_the_lock_is_held_does_not_stop_later_deposits_and_claims() {
        let handoff = handoff();
        handoff.deposit("alice", 1);
        let poisoner = handoff.clone();
        let _ = std::thread::spawn(move || {
            let _held = poisoner.queues.lock().unwrap();
            panic!("poison the lock");
        })
        .join();

        handoff.deposit("alice", 2);

        assert_eq!(handoff.claim("alice"), Some(2));
        assert_eq!(handoff.claim("alice"), Some(1));
    }

    #[test]
    fn claiming_for_an_unknown_username_gives_nothing() {
        let handoff: Handoff<&str> = handoff();

        assert_eq!(handoff.claim("alice"), None);
    }
}
