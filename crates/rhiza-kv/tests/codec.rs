use rhiza_core::{ExecutionProfile, ReplicatedCommandEnvelope};
use rhiza_kv::{
    encode_replicated_kv_command, Error, KvCommandV1, MAX_KV_KEY_BYTES, MAX_KV_VALUE_BYTES,
};

#[test]
fn put_and_delete_commands_have_one_canonical_encoding() {
    let commands = [
        KvCommandV1::put("put-1", b"alpha".to_vec(), vec![0, 1, 2, 255]).unwrap(),
        KvCommandV1::delete("delete-1", b"alpha".to_vec()).unwrap(),
    ];

    for command in commands {
        let encoded = command.encode().unwrap();
        assert!(encoded.starts_with(b"RHKV\0\x01"));
        assert_eq!(KvCommandV1::decode(&encoded).unwrap(), command);
        assert_eq!(command.encode().unwrap(), encoded);

        let replicated = encode_replicated_kv_command(&command).unwrap();
        let envelope = ReplicatedCommandEnvelope::decode(&replicated).unwrap();
        assert_eq!(envelope.profile(), ExecutionProfile::Kv);
        assert_eq!(envelope.command_version(), 1);
        assert_eq!(envelope.request_id(), command.request_id());
        assert_eq!(envelope.body(), encoded);
    }
}

#[test]
fn command_codec_rejects_empty_oversized_and_noncanonical_values() {
    assert!(KvCommandV1::put("", b"key".to_vec(), b"value".to_vec()).is_err());
    assert!(KvCommandV1::put("request", Vec::new(), b"value".to_vec()).is_err());
    assert!(
        KvCommandV1::put("request", vec![0; MAX_KV_KEY_BYTES + 1], b"value".to_vec(),).is_err()
    );
    assert!(
        KvCommandV1::put("request", b"key".to_vec(), vec![0; MAX_KV_VALUE_BYTES + 1],).is_err()
    );

    let command = KvCommandV1::delete("request", b"key".to_vec()).unwrap();
    let mut trailing = command.encode().unwrap();
    trailing.push(0);
    assert!(matches!(
        KvCommandV1::decode(&trailing),
        Err(Error::Codec(_))
    ));
}
