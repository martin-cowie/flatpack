use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

/// Passes a value from `authenticate()` to `provide_user_detail()`, keyed by username.
///
/// libunftp provides no means of passing data out from the authenticator, only a `Principal`
///
/// libunftp's `Principal` carries only a username, so anything the authenticator
/// establishes (here, the Kafka login) must be deposited here and claimed by the
/// provider. Items are queued per username; any item will do, but each is claimed once.
///
/// Unclaimed items are never expired (issue #29). Cloning yields another handle
/// on the same queues.
pub struct Handoff<T> {
    queues: Arc<Mutex<HashMap<String, Vec<T>>>>,
}

impl<T> Clone for Handoff<T> {
    fn clone(&self) -> Self {
        Handoff {
            queues: Arc::clone(&self.queues),
        }
    }
}

impl<T> Default for Handoff<T> {
    fn default() -> Self {
        Handoff::new()
    }
}

impl<T> Handoff<T> {
    /// Creates a handoff with nothing waiting.
    pub fn new() -> Self {
        Handoff {
            queues: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Queues `item` for `username` until a later `claim` takes it.
    pub fn deposit(&self, username: &str, item: T) {
        self.lock()
            .entry(username.to_string())
            .or_default()
            .push(item);
    }

    /// Takes the most recently deposited item for `username`, or `None` if
    /// nothing is waiting for it.
    ///
    /// The username's queue is removed with its last item, so the handoff
    /// holds only items still awaiting a claim.
    pub fn claim(&self, username: &str) -> Option<T> {
        let mut queues = self.lock();
        let queue = queues.get_mut(username)?;
        let result = queue.pop();
        if queue.is_empty() {
            queues.remove(username);
        }
        result
    }

    /// A panic elsewhere while the lock was held cannot leave the map
    /// structurally invalid, so a poisoned lock is safe to keep using.
    fn lock(&self) -> MutexGuard<'_, HashMap<String, Vec<T>>> {
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

    #[test]
    fn a_deposited_item_is_claimed_for_its_username_only() {
        let handoff = Handoff::new();
        handoff.deposit("alice", 1);

        assert_eq!(handoff.claim("bob"), None);
        assert_eq!(handoff.claim("alice"), Some(1));
        assert_eq!(handoff.claim("alice"), None);
    }

    #[test]
    fn items_deposited_for_one_username_are_claimed_newest_first() {
        let handoff = Handoff::new();
        handoff.deposit("alice", 1);
        handoff.deposit("alice", 2);

        assert_eq!(handoff.claim("alice"), Some(2));
        assert_eq!(handoff.claim("alice"), Some(1));
    }

    #[test]
    fn claiming_the_last_item_for_a_username_forgets_the_username() {
        let handoff = Handoff::new();
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
        let handoff = Handoff::new();
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
        let handoff: Handoff<&str> = Handoff::new();

        assert_eq!(handoff.claim("alice"), None);
    }
}
