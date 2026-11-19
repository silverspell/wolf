use std::sync::{Arc, Barrier};
use std::time::Duration;

use bytes::Bytes;
use wolf_broker::exchange::{ExchangeConfig, ExchangeKind};
use wolf_broker::queue::{ConnectionId, QueueConfig};
use wolf_broker::{
    DomainError, Message, PublishOutcome, PublishRequest, Vhost, VhostId, VhostName,
};

const ITERATIONS: usize = 300;
const PUBLISHERS: usize = 4;

fn message(body: String) -> Message {
    Message::new(Bytes::from(body))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn publish_races_topology_changes_without_deadlock() {
    tokio::time::timeout(Duration::from_secs(30), async {
        let vhost = Arc::new(Vhost::new(
            VhostId::new(1),
            VhostName::new("/").expect("valid vhost name"),
        ));
        let connection = ConnectionId::new(1);

        vhost
            .declare_exchange("events", ExchangeConfig::new(ExchangeKind::Direct))
            .await
            .expect("declare stable exchange");
        vhost
            .declare_queue("stable", QueueConfig::default(), connection)
            .await
            .expect("declare stable queue");

        let barrier = Arc::new(Barrier::new(PUBLISHERS));
        let mut publishers = Vec::with_capacity(PUBLISHERS);

        for publisher in 0..PUBLISHERS {
            let vhost = Arc::clone(&vhost);
            let barrier = Arc::clone(&barrier);
            publishers.push(tokio::spawn(async move {
                barrier.wait();
                for iteration in 0..ITERATIONS {
                    let outcome = vhost
                        .publish(
                            connection,
                            PublishRequest {
                                exchange: "",
                                routing_key: "stable",
                                mandatory: false,
                                message: message(format!("{publisher}-{iteration}")),
                                properties: (),
                            },
                        )
                        .await
                        .expect("publish to the stable queue");
                    assert_eq!(outcome, PublishOutcome::Routed { queues: 1 });
                }
            }));
        }

        let topology = {
            let vhost = Arc::clone(&vhost);
            tokio::spawn(async move {
                for _ in 0..ITERATIONS {
                    vhost
                        .declare_queue("churn", QueueConfig::default(), connection)
                        .await
                        .expect("declare churn queue");
                    match vhost
                        .bind_queue("events", "churn", "churn", connection)
                        .await
                    {
                        Ok(()) | Err(DomainError::QueueNotFound { .. }) => {}
                        Err(error) => panic!("unexpected bind error: {error}"),
                    }
                    match vhost
                        .unbind_queue("events", "churn", "churn", connection)
                        .await
                    {
                        Ok(_) | Err(DomainError::QueueNotFound { .. }) => {}
                        Err(error) => panic!("unexpected unbind error: {error}"),
                    }
                    vhost
                        .delete_queue("churn", connection, false, false)
                        .await
                        .expect("delete churn queue");
                }
            })
        };

        for publisher in publishers {
            publisher.await.expect("publisher task does not panic");
        }
        topology.await.expect("topology task does not panic");

        vhost
            .declare_queue("fresh", QueueConfig::default(), connection)
            .await
            .expect("declare fresh queue");
        let outcome = vhost
            .publish(
                connection,
                PublishRequest {
                    exchange: "",
                    routing_key: "fresh",
                    mandatory: false,
                    message: message("after".to_string()),
                    properties: (),
                },
            )
            .await
            .expect("publish after churn");
        assert_eq!(outcome, PublishOutcome::Routed { queues: 1 });
        assert_eq!(vhost.queue_message_count("fresh"), 1);
    })
    .await
    .expect("concurrency scenario finishes before the timeout");
}
