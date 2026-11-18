use std::future::Future;

use bytes::Bytes;
use wolf_broker::{
    CancelReason, ChannelId, ConsumerCancel, ConsumerId, CreditSignal, DeliverOutcome, Delivery,
    DeliverySink, DeliveryTag, Message,
};

#[derive(Debug, Default)]
struct RecordingSink {
    deliveries: Vec<Delivery>,
    cancels: Vec<ConsumerCancel>,
}

impl DeliverySink for RecordingSink {
    fn deliver(&mut self, delivery: Delivery) -> impl Future<Output = DeliverOutcome> + Send {
        self.deliveries.push(delivery);
        std::future::ready(DeliverOutcome::Delivered)
    }

    fn cancel(&mut self, cancel: ConsumerCancel) -> impl Future<Output = ()> + Send {
        self.cancels.push(cancel);
        std::future::ready(())
    }
}

#[tokio::test]
async fn delivery_contract_forwards_message_metadata() {
    let body = Bytes::from_static(b"payload");
    let ptr = body.as_ptr();

    let delivery = Delivery {
        consumer: ConsumerId::new(7),
        delivery_tag: DeliveryTag::new(42),
        message: Message::new(body),
        redelivered: true,
    };

    let mut sink = RecordingSink::default();
    sink.deliver(delivery.clone()).await;

    assert_eq!(sink.deliveries.len(), 1);
    let recorded = &sink.deliveries[0];
    assert_eq!(recorded.consumer, ConsumerId::new(7));
    assert_eq!(recorded.delivery_tag, DeliveryTag::new(42));
    assert!(recorded.redelivered);
    assert_eq!(recorded.message.body().as_ptr(), ptr);
    assert_eq!(recorded.message.body().as_ref(), b"payload");
}

#[test]
fn credit_contract_covers_consumer_and_channel_scopes() {
    let consumer_credit = CreditSignal::Consumer {
        consumer: ConsumerId::new(1),
        credit: 10,
    };
    let channel_credit = CreditSignal::Channel {
        channel: ChannelId::new(2),
        credit: 20,
    };

    match consumer_credit {
        CreditSignal::Consumer { consumer, credit } => {
            assert_eq!(consumer, ConsumerId::new(1));
            assert_eq!(credit, 10);
        }
        CreditSignal::Channel { .. } => panic!("expected consumer scope"),
    }

    match channel_credit {
        CreditSignal::Channel { channel, credit } => {
            assert_eq!(channel, ChannelId::new(2));
            assert_eq!(credit, 20);
        }
        CreditSignal::Consumer { .. } => panic!("expected channel scope"),
    }
}

#[tokio::test]
async fn cancel_contract_is_protocol_neutral() {
    let cancel = ConsumerCancel {
        consumer: ConsumerId::new(9),
        reason: CancelReason::QueueDeleted,
    };

    let mut sink = RecordingSink::default();
    sink.cancel(cancel.clone()).await;

    assert_eq!(sink.cancels, vec![cancel]);
    assert_eq!(sink.cancels[0].consumer, ConsumerId::new(9));
    assert_eq!(sink.cancels[0].reason, CancelReason::QueueDeleted);
}

#[test]
fn credit_signals_can_flow_over_tokio_channel() {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<CreditSignal>(4);
    tx.try_send(CreditSignal::Consumer {
        consumer: ConsumerId::new(1),
        credit: 1,
    })
    .expect("send credit");

    assert_eq!(
        rx.try_recv().expect("recv credit"),
        CreditSignal::Consumer {
            consumer: ConsumerId::new(1),
            credit: 1,
        }
    );
}
