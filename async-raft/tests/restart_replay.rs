mod fixtures;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_raft::raft::{Entry, EntryNormal, EntryPayload};
use async_raft::storage::{HardState, RaftStorage};
use async_raft::{Config, Raft, State};
use memstore::{ClientRequest, MemStore};
use tokio::time::timeout;

use fixtures::RaftRouter;

/// A node may restart after persisting a log entry but before updating the state machine.
///
/// The log entry is committed by the new leader's initial entry. Recovery must replay the
/// persisted entry before reporting the new applied index.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replays_persisted_log_after_restart() -> Result<()> {
    let config = Arc::new(
        Config::build("test".into())
            .validate()
            .expect("failed to build Raft config"),
    );
    let router = Arc::new(RaftRouter::new(config.clone()));
    let store = Arc::new(MemStore::new(0));
    let request = ClientRequest {
        client: "client-0".into(),
        serial: 1,
        status: "committed-before-crash".into(),
    };

    store
        .save_hard_state(&HardState { current_term: 1, voted_for: Some(0) })
        .await?;
    store.get_log().await.insert(
        1,
        Entry {
            term: 1,
            index: 1,
            payload: EntryPayload::Normal(EntryNormal { data: request.clone() }),
        },
    );

    let raft = Raft::new(0, config, router, store.clone());
    let mut metrics = raft.metrics();
    timeout(Duration::from_secs(2), async {
        while metrics.borrow().last_applied < 2 {
            if metrics.changed().await.is_err() {
                break;
            }
        }
    })
    .await
    .expect("node did not finish recovering persisted log entries");

    let metrics = metrics.borrow().clone();
    assert_eq!(metrics.state, State::Leader);
    assert_eq!(metrics.last_log_index, 2);
    assert_eq!(metrics.last_applied, 2);

    let sm = store.get_state_machine().await;
    assert_eq!(sm.last_applied_log, 1);
    assert_eq!(sm.client_status.get(&request.client), Some(&request.status));

    raft.shutdown().await?;
    Ok(())
}
