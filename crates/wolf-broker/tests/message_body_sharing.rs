use bytes::Bytes;
use wolf_broker::Message;

#[test]
fn cloning_message_shares_body_allocation() {
    let body = Bytes::from(vec![0xAB; 4096]);
    let ptr = body.as_ptr();

    let message = Message::new(body);
    let cloned = message.clone();

    assert_eq!(message.body().as_ptr(), ptr);
    assert_eq!(cloned.body().as_ptr(), ptr);
    assert_eq!(message.body().len(), 4096);
    assert_eq!(cloned.body().len(), 4096);

    let independent = Bytes::from(vec![0xAB; 4096]);
    assert_eq!(&independent, message.body());
    assert_ne!(independent.as_ptr(), ptr);
}

#[test]
fn into_body_preserves_allocation() {
    let body = Bytes::from(vec![0xCD; 1024]);
    let ptr = body.as_ptr();

    let message = Message::new(body);
    let recovered = message.into_body();

    assert_eq!(recovered.as_ptr(), ptr);
    assert_eq!(recovered.len(), 1024);
}
