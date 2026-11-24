mod fixtures;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use async_raft::{Config, NodeId, State};
use fixtures::RaftRouter;
use tokio::time::{sleep, Instant};

/// Return the IDs of every node currently reporting `State::Leader`.
async fn leader_ids(router: &RaftRouter) -> Vec<NodeId> {
    router
        .latest_metrics()
        .await
        .into_iter()
        .filter(|metrics| metrics.state == State::Leader)
        .map(|metrics| metrics.id)
        .collect()
}

/// Poll until exactly one node reports leader state, failing if two leaders are ever observed.
async fn wait_for_unique_leader(router: &RaftRouter, timeout: Duration) -> Result<NodeId> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        let leaders = leader_ids(router).await;
        if leaders.len() > 1 {
            return Err(anyhow!("split brain: multiple leaders observed: {:?}", leaders));
        }
        if leaders.len() == 1 {
            return Ok(leaders[0]);
        }
        sleep(Duration::from_millis(50)).await;
    }
    Err(anyhow!("timed out waiting for a unique leader"))
}

/// Sample the cluster for `window`, asserting that there is never more than one leader and that
/// any leader present is the deterministic priority member (node `0`).
async fn assert_no_second_leader(router: &RaftRouter, window: Duration) {
    let start = Instant::now();
    while start.elapsed() < window {
        let leaders = leader_ids(router).await;
        assert!(leaders.len() <= 1, "split brain: two leaders observed: {:?}", leaders);
        if let Some(id) = leaders.first() {
            assert_eq!(*id, 0, "only the lowest configured node may win the half-vote tie-break");
        }
        sleep(Duration::from_millis(50)).await;
    }
}

/// Even-sized clusters split cleanly in half used to campaign forever: each half holds exactly
/// half the votes, one below majority, and tightly randomized timeouts kept both sides in lock
/// step. The deterministic priority tie-break must end the deadlock with exactly one leader.
///
/// RUST_LOG=async_raft,memstore,election_convergence=trace cargo test -p async-raft --test election_convergence
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn half_split_converges_to_unique_leader() -> Result<()> {
    fixtures::init_tracing();

    // Fast timeouts with a deliberately small interval: the tie-break, not random luck, is what
    // must guarantee convergence. Two observed deadlock rounds are required before it fires.
    let base_config = || -> Arc<Config> {
        Arc::new(
            Config::build("election-convergence".into())
                .election_timeout_min(100)
                .election_timeout_max(200)
                .election_tie_break_rounds(2)
                .validate()
                .expect("failed to build raft config"),
        )
    };

    // Phase 1: the smallest even cluster (2 nodes), split 1/1 before init. Each candidate only
    // holds its own vote, so this specifically exercises the timeout-driven tie-break path.
    tracing::info!("--- phase 1: 2-node cluster split 1/1 before init");
    {
        let router = Arc::new(RaftRouter::new(base_config()));
        router.new_raft_node(0).await;
        router.new_raft_node(1).await;
        router.split_network(&[0], &[1]).await;

        let started = Instant::now();
        router.initialize_from_single_node(0).await?;
        let leader = wait_for_unique_leader(&router, Duration::from_secs(20)).await?;
        assert_eq!(leader, 0, "lowest-id member must be the tie-break winner");
        tracing::info!(elapsed_ms = started.elapsed().as_millis() as u64, "2-node split elected a unique leader");
        assert_no_second_leader(&router, Duration::from_secs(2)).await;
        // Heal before dropping the router so the leader's replication stream settles instead of
        // retrying a blocked link for the rest of the test process.
        router.heal_network().await;
    }

    // Phase 2: a 4-node cluster split 2/2 before the first election. Node 0 can reach node 1
    // only, giving it exactly half the votes; baseline Raft can never reach the 3-vote majority.
    tracing::info!("--- phase 2: 4-node cluster split 2/2 before init");
    let router = Arc::new(RaftRouter::new(base_config()));
    for id in 0..4 {
        router.new_raft_node(id).await;
    }
    router.split_network(&[0, 1], &[2, 3]).await;

    let started = Instant::now();
    router.initialize_from_single_node(0).await?;
    let leader = wait_for_unique_leader(&router, Duration::from_secs(20)).await?;
    assert_eq!(leader, 0, "lowest-id member in the reachable half must break the tie");
    tracing::info!(elapsed_ms = started.elapsed().as_millis() as u64, "4-node half split elected a unique leader");

    // The partition persists: no second leader may appear on the other half.
    assert_no_second_leader(&router, Duration::from_secs(3)).await;

    // Heal the split. The tie-break leader then reaches a real majority, replicates the missed
    // log and the whole cluster converges; client writes commit again.
    tracing::info!("--- phase 2: heal partition and assert writes resume");
    router.heal_network().await;
    sleep(Duration::from_secs(6)).await;
    let leader = wait_for_unique_leader(&router, Duration::from_secs(15)).await?;
    router.client_request_many(leader, "convergence", 5).await;
    sleep(Duration::from_secs(3)).await;
    router.assert_stable_cluster(None, None).await;

    // Phase 3: the production incident shape - a healthy 4-node cluster first, then a clean 2/2
    // split. The established leader (node 0, also the priority member) stays the single leader;
    // the isolated other half campaigns but can neither reach majority nor invoke the tie-break.
    tracing::info!("--- phase 3: healthy cluster then 2/2 split");
    let router = Arc::new(RaftRouter::new(base_config()));
    for id in 0..4 {
        router.new_raft_node(id).await;
    }
    sleep(Duration::from_secs(1)).await;
    router.initialize_from_single_node(0).await?;
    let leader = wait_for_unique_leader(&router, Duration::from_secs(15)).await?;
    assert_eq!(leader, 0, "node 0 is the expected initial leader");

    router.split_network(&[0, 1], &[2, 3]).await;
    assert_no_second_leader(&router, Duration::from_secs(5)).await;

    tracing::info!("--- phase 3: heal partition and assert convergence");
    router.heal_network().await;
    sleep(Duration::from_secs(6)).await;
    let leader = wait_for_unique_leader(&router, Duration::from_secs(15)).await?;
    router.client_request_many(leader, "after-split", 5).await;
    sleep(Duration::from_secs(3)).await;
    router.assert_stable_cluster(None, None).await;

    Ok(())
}
