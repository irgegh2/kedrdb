// SPDX-License-Identifier: Apache-2.0
use kedr_core::{
    page::MAX_RECORD_SIZE,
    record::{Value, decode_row, encode_row, primary_key, secondary_key},
};
#[test]
fn row_roundtrip_boundaries_null_and_unicode() {
    let values = vec![
        Value::Null,
        Value::Int(i32::MIN),
        Value::Int(i32::MAX),
        Value::BigInt(i64::MIN),
        Value::BigInt(i64::MAX),
        Value::Bool(false),
        Value::Bool(true),
        Value::Text(String::new()),
        Value::Text("Кедр\0🌲".into()),
    ];
    let encoded = encode_row(&values).unwrap();
    assert_eq!(decode_row(&encoded).unwrap(), values);
    assert_eq!(
        &encode_row(&[Value::Int(0x01020304)]).unwrap(),
        &[1, 1, 0, 1, 4, 3, 2, 1]
    );
    let largest = vec![Value::Text("x".repeat(MAX_RECORD_SIZE - 8))];
    assert_eq!(encode_row(&largest).unwrap().len(), MAX_RECORD_SIZE);
    assert_eq!(decode_row(&encode_row(&largest).unwrap()).unwrap(), largest);
    assert!(encode_row(&[Value::Text("x".repeat(MAX_RECORD_SIZE - 7))]).is_err());
    assert!(encode_row(&vec![Value::Null; 257]).is_err());
}
#[test]
fn malformed_rows_are_rejected_without_panics() {
    for bytes in [
        vec![],
        vec![2, 0, 0],
        vec![1, 1, 0, 3, 2],
        vec![1, 1, 0, 4, 255, 255, 255, 255],
        vec![1, 1, 0, 4, 1, 0, 0, 0, 0xff],
        vec![1, 0, 0, 1],
        vec![1, 1, 0, 99],
        vec![1, 1, 1],
    ] {
        assert!(decode_row(&bytes).is_err(), "{bytes:?}");
    }
    let encoded = encode_row(&[Value::Text("hello".into()), Value::BigInt(7)]).unwrap();
    for length in 0..encoded.len() {
        assert!(decode_row(&encoded[..length]).is_err());
    }
    for seed in 0u64..1000 {
        let mut state = seed + 1;
        let data: Vec<u8> = (0..seed % 64)
            .map(|_| {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                (state >> 32) as u8
            })
            .collect();
        let _ = decode_row(&data);
    }
}
#[test]
fn keys_follow_value_order_and_preserve_duplicate_values() {
    let values = [i64::MIN, -100, -1, 0, 1, 100, i64::MAX];
    for pair in values.windows(2) {
        assert!(primary_key(pair[0]) < primary_key(pair[1]));
    }
    let texts = ["", "\0", "\0a", "a", "a\0", "aa", "b", "я"];
    for pair in texts.windows(2) {
        assert!(
            secondary_key(&Value::Text(pair[0].into()), 0).unwrap()
                < secondary_key(&Value::Text(pair[1].into()), 0).unwrap()
        );
    }
    assert!(secondary_key(&Value::Null, 1).unwrap() < secondary_key(&Value::Null, 2).unwrap());
    assert!(
        secondary_key(&Value::Null, i64::MAX).unwrap()
            < secondary_key(&Value::Text(String::new()), i64::MIN).unwrap()
    );
    assert_ne!(
        secondary_key(&Value::Text("same".into()), 1).unwrap(),
        secondary_key(&Value::Text("same".into()), 2).unwrap()
    );
    assert!(secondary_key(&Value::Text("\0".repeat(251)), 1).is_err());
}
