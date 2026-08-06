use proptest::prelude::*;
use rhiza_kv::{KvCommandV1, Error};

proptest! {
    #[test]
    fn kv_command_encode_decode_roundtrip(
        request_id in "[a-zA-Z0-9_-]{1,64}",
        key in prop::collection::vec(any::<u8>(), 1..1024),
        value in prop::collection::vec(any::<u8>(), 0..4096),
    ) {
        let command = KvCommandV1::put(request_id, key, value)?;
        let encoded = command.encode()?;
        let decoded = KvCommandV1::decode(&encoded)?;
        prop_assert_eq!(command, decoded);
    }

    #[test]
    fn kv_command_delete_encode_decode_roundtrip(
        request_id in "[a-zA-Z0-9_-]{1,64}",
        key in prop::collection::vec(any::<u8>(), 1..1024),
    ) {
        let command = KvCommandV1::delete(request_id, key)?;
        let encoded = command.encode()?;
        let decoded = KvCommandV1::decode(&encoded)?;
        prop_assert_eq!(command, decoded);
    }

    #[test]
    fn kv_command_encode_is_canonical(
        request_id in "[a-zA-Z0-9_-]{1,64}",
        key in prop::collection::vec(any::<u8>(), 1..1024),
        value in prop::collection::vec(any::<u8>(), 0..4096),
    ) {
        let command = KvCommandV1::put(request_id, key, value)?;
        let encoded = command.encode()?;
        let decoded = KvCommandV1::decode(&encoded)?;
        let re_encoded = decoded.encode()?;
        prop_assert_eq!(encoded, re_encoded);
    }

    #[test]
    fn kv_command_decode_rejects_trailing_bytes(
        request_id in "[a-zA-Z0-9_-]{1,64}",
        key in prop::collection::vec(any::<u8>(), 1..1024),
        trailing in prop::collection::vec(any::<u8>(), 1..64),
    ) {
        let command = KvCommandV1::put(request_id, key, vec![1, 2, 3])?;
        let mut encoded = command.encode()?;
        encoded.extend_from_slice(&trailing);
        let result = KvCommandV1::decode(&encoded);
        prop_assert!(result.is_err());
    }
}

#[test]
fn kv_command_rejects_empty_request_id() {
    let result = KvCommandV1::put("", vec![1], vec![2]);
    assert!(matches!(result, Err(Error::InvalidCommand(_))));
}

#[test]
fn kv_command_rejects_empty_key() {
    let result = KvCommandV1::put("req-1", vec![], vec![2]);
    assert!(matches!(result, Err(Error::InvalidCommand(_))));
}

#[test]
fn kv_command_rejects_oversized_request_id() {
    let result = KvCommandV1::put("x".repeat(300), vec![1], vec![2]);
    assert!(matches!(result, Err(Error::InvalidCommand(_))));
}

#[test]
fn kv_command_rejects_oversized_key() {
    let result = KvCommandV1::put("req-1", vec![0u8; 10000], vec![2]);
    assert!(matches!(result, Err(Error::InvalidCommand(_))));
}

#[test]
fn kv_command_rejects_oversized_value() {
    let result = KvCommandV1::put("req-1", vec![1], vec![0u8; 300_000]);
    assert!(matches!(result, Err(Error::InvalidCommand(_))));
}
