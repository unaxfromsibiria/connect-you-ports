use bytes::Bytes;
use chrono::{DateTime, Utc};
use log::{debug, info, warn};
use tokio::time::sleep;
use std::time::Duration;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use uuid::Uuid;

struct Router {
    client: HashMap<Uuid, mpsc::Sender<(Uuid, Bytes)>>,
    when: HashMap<Uuid, DateTime<Utc>>,
    default_size: usize,
}

impl Router {
    fn new(default_size: usize) -> Self {
        Self {
            default_size: default_size,
            client: HashMap::new(),
            when: HashMap::new(),
        }
    }

    fn update_timestamp(&mut self, transfer: &Uuid) {
        let now = Utc::now();
        debug!("Updated route timestamp for {}", transfer);
        self.when.insert(*transfer, now);
    }
}

static SYS_ROUTER: Lazy<RwLock<Router>> = Lazy::new(|| {
    RwLock::new(Router::new(100))
});


// Set default channel size for new routes
pub async fn set_channel_size(value: usize) {
    let mut rt = SYS_ROUTER.write().await;
    rt.default_size = value;
}

// Add a new route for transfer and return its receiver
pub async fn add_route(transfer: &Uuid) -> mpsc::Receiver<(Uuid, Bytes)> {
    let mut rt = SYS_ROUTER.write().await;
    if rt.client.contains_key(transfer) {
        warn!("Route already exists: {}", transfer);
    }
    let (tx, rx) = mpsc::channel(rt.default_size);
    rt.client.insert(*transfer, tx);
    rt.update_timestamp(transfer);
    info!("Added route: {}", transfer);
    rx
}

pub async fn exists(transfer: &Uuid) -> bool {
    let rt = SYS_ROUTER.read().await;
    rt.client.contains_key(transfer)
}

pub async fn remove_route(transfer: &Uuid) {
    let mut rt = SYS_ROUTER.write().await;
    if rt.client.remove(transfer).is_some() {
        rt.when.remove(transfer);
        info!("Removed route: {}", transfer);
    } else {
        // Normal now: the target task and the idle close-out may both remove the same route.
        debug!("Route not found for removal: {}", transfer);
    }
}

// Remove routes whose last data send is older than max_age.
// Termination markers are sent after the lock is released: a full channel of a stuck receiver
// must not block every route operation in the process while the write lock is held.
pub async fn clear_old(max_age: Duration) {
    let mut rt = SYS_ROUTER.write().await;
    let cutoff = Utc::now() - max_age;
    let stale: Vec<(Uuid, mpsc::Sender<(Uuid, Bytes)>)> = rt.client.iter().filter(
        |(t, _)| rt.when.get(*t).map_or(false, |last| *last < cutoff)
    ).map(
        |(t, tx)| (*t, tx.clone())
    ).collect();
    for (t, _) in &stale {
        rt.client.remove(t);
        rt.when.remove(t);
    }
    drop(rt);
    let cleared = stale.len();
    for (t, tx) in stale {
        if tx.send((t, Bytes::new())).await.is_err() {
            debug!("Failed to send termination marker for route: {}", t);
        }
    }
    if cleared > 0 {
        warn!("Cleared {} old route(s)", cleared);
    }
}

// Periodic cleanup of stale routes; runs every `interval`.
pub async fn run_cleanup(interval: Duration) {
    loop {
        sleep(interval).await;
        clear_old(interval).await;
    }
}

// Send data to transfer; false if the route is missing or the channel is full/closed
pub async fn send_data(transfer: &Uuid, data: &Bytes) -> bool {
    let tx = {
        let mut rt = SYS_ROUTER.write().await;
        if !rt.client.contains_key(transfer) {
            warn!("Route not found for sending data: {}", transfer);
            return false;
        }
        rt.update_timestamp(transfer);
        match rt.client.get(transfer) {
            Some(tx) => tx.clone(),
            None => return false,
        }
    };
    let result = tx.send((*transfer, data.clone())).await;
    if result.is_err() {
        warn!("Failed to send data for route: {} (data size: {})", transfer, data.len());
        false
    } else {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tokio::sync::Mutex;
    use std::time::Duration;
    use tokio::time::{sleep, timeout};

    static TEST_MUTEX: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

    async fn reset_router() {
        let mut rt = SYS_ROUTER.write().await;
        rt.client.clear();
        rt.when.clear();
        rt.default_size = 100;
    }

    async fn cleanup_route(transfer: &Uuid) {
        let mut rt = SYS_ROUTER.write().await;
        rt.client.remove(transfer);
        rt.when.remove(transfer);
    }

    async fn test_guard() -> tokio::sync::MutexGuard<'static, ()> {
        TEST_MUTEX.lock().await
    }

    #[tokio::test]
    async fn test_add_route_and_exists() {
        let _g = test_guard().await;
        reset_router().await;
        let transfer = Uuid::new_v4();
        assert!(!exists(&transfer).await);
        let _rx = add_route(&transfer).await;
        assert!(exists(&transfer).await);
        cleanup_route(&transfer).await;
    }

    #[tokio::test]
    async fn test_add_duplicate_route() {
        let _g = test_guard().await;
        reset_router().await;
        let transfer = Uuid::new_v4();
        let _rx1 = add_route(&transfer).await;
        let _rx2 = add_route(&transfer).await;
        assert!(exists(&transfer).await);
        cleanup_route(&transfer).await;
    }

    #[tokio::test]
    async fn test_send_data_success() {
        let _g = test_guard().await;
        reset_router().await;
        let transfer = Uuid::new_v4();
        let mut rx = add_route(&transfer).await;
        let data = Bytes::from("hello world");
        assert!(send_data(&transfer, &data).await);

        let (received_transfer, received_data) = timeout(
            Duration::from_secs(1),
            rx.recv()
        ).await.expect("Receive timed out").expect("Channel closed");
        assert_eq!(received_transfer, transfer);
        assert_eq!(received_data, data);
        cleanup_route(&transfer).await;
    }

    #[tokio::test]
    async fn test_send_data_to_nonexistent_route() {
        let _g = test_guard().await;
        reset_router().await;
        let transfer = Uuid::new_v4();
        let data = Bytes::from("hello world");
        assert!(!send_data(&transfer, &data).await);
    }

    #[tokio::test]
    async fn test_send_data_to_closed_channel() {
        let _g = test_guard().await;
        reset_router().await;
        let transfer = Uuid::new_v4();
        let rx = add_route(&transfer).await;
        drop(rx);
        sleep(Duration::from_millis(10)).await;
        assert!(!send_data(&transfer, &Bytes::from("hello world")).await);
        cleanup_route(&transfer).await;
    }

    #[tokio::test]
    async fn test_set_channel_size() {
        let _g = test_guard().await;
        reset_router().await;
        let transfer = Uuid::new_v4();
        set_channel_size(256).await;
        let _rx = add_route(&transfer).await;
        assert!(exists(&transfer).await);
        cleanup_route(&transfer).await;
        reset_router().await;
    }

    #[tokio::test]
    async fn test_multiple_sends_and_receives() {
        let _g = test_guard().await;
        reset_router().await;
        let transfer = Uuid::new_v4();
        let mut rx = add_route(&transfer).await;
        for i in 0..5 {
            let data = Bytes::from(format!("message {}", i));
            assert!(send_data(&transfer, &data).await);
        }
        for i in 0..5 {
            let (received_transfer, received_data) = timeout(
                Duration::from_secs(1),
                rx.recv()
            ).await.expect("Receive timed out").expect("Channel closed");
            assert_eq!(received_transfer, transfer);
            assert_eq!(received_data, Bytes::from(format!("message {}", i)));
        }
        cleanup_route(&transfer).await;
    }

    #[tokio::test]
    async fn test_channel_capacity_limit() {
        let _g = test_guard().await;
        reset_router().await;
        let transfer = Uuid::new_v4();
        set_channel_size(2).await;
        let mut rx = add_route(&transfer).await;

        assert!(send_data(&transfer, &Bytes::from("msg1")).await);
        assert!(send_data(&transfer, &Bytes::from("msg2")).await);

        let result = timeout(
            Duration::from_millis(100),
            send_data(&transfer, &Bytes::from("msg3"))
        ).await;
        assert!(result.is_err(), "Send should have timed out due to full channel");

        let _ = rx.recv().await;
        let _ = rx.recv().await;

        cleanup_route(&transfer).await;
        reset_router().await;
    }

    #[tokio::test]
    async fn test_clear_old_does_not_block_router_on_full_channel() {
        let _g = test_guard().await;
        reset_router().await;
        set_channel_size(1).await;
        let transfer = Uuid::new_v4();
        // A live receiver that never consumes: simulates a stuck target connection.
        let rx = add_route(&transfer).await;
        assert!(send_data(&transfer, &Bytes::from("fill")).await);

        let clear_task = tokio::spawn(async move {
            clear_old(Duration::from_secs(1)).await;
        });
        // Give the cleanup task time to reach the blocking marker send.
        sleep(Duration::from_millis(100)).await;
        // The router must remain usable while a marker send is pending on a full channel.
        let router_usable = timeout(
            Duration::from_millis(500),
            async {
                let other = Uuid::new_v4();
                assert!(!exists(&other).await);
                let _rx2 = add_route(&other).await;
                assert!(exists(&other).await);
                cleanup_route(&other).await;
            }
        ).await;
        assert!(router_usable.is_ok(), "router was blocked while clear_old sent a marker on a full channel");
        drop(rx); // unblock the pending send so the task can finish
        let joined = timeout(
            Duration::from_secs(1),
            clear_task
        ).await.expect("clear_old did not finish");
        joined.unwrap();
    }
}
