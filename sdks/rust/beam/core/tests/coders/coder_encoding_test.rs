/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! Hand-written golden and edge-case tests for coders, beyond `standard_coders.yaml`.
//!
//! Covers URNs, primitive framing, unit, bool, strings, bytes, varints and floats.

use beam::coders::{
    BoolCoder, BytesCoder, Coder, Context, DefaultCoder, DoubleCoder, FloatCoder, StringUtf8Coder,
    UnitCoder, VarIntCoder,
};

/// URNs are spelled out, not taken from SDK constants, so a typo in a constant is caught.
#[test]
fn every_coder_reports_its_standard_urn() {
    let cases: [(&str, &str); 8] = [
        (
            Coder::<String>::urn(&StringUtf8Coder),
            "beam:coder:string_utf8:v1",
        ),
        (Coder::<Vec<u8>>::urn(&BytesCoder), "beam:coder:bytes:v1"),
        (Coder::<bool>::urn(&BoolCoder), "beam:coder:bool:v1"),
        (Coder::<f64>::urn(&DoubleCoder), "beam:coder:double:v1"),
        (Coder::<f32>::urn(&FloatCoder), "beam:coder:double:v1"),
        (Coder::<i64>::urn(&VarIntCoder), "beam:coder:varint:v1"),
        (Coder::<i32>::urn(&VarIntCoder), "beam:coder:varint:v1"),
        // `()` has no standard coder of its own; it is written as empty bytes.
        (Coder::<()>::urn(&UnitCoder), "beam:coder:bytes:v1"),
    ];
    for (actual, expected) in cases {
        assert_eq!(actual, expected);
    }
}

#[test]
fn a_bool_element_is_one_byte() {
    for (value, byte) in [(false, 0x00), (true, 0x01)] {
        assert_eq!(DefaultCoder::encode(&value).unwrap(), [byte]);
        assert_eq!(<bool as DefaultCoder>::decode(&[byte]).unwrap(), value);
    }
    assert!(<bool as DefaultCoder>::decode(&[0x02]).is_err());
}

/// An `f32` has the bytes of the same value as a double, so other SDKs read a double.
#[test]
fn an_f32_element_encodes_as_a_double() {
    for value in [1.5f32, -0.0, f32::MAX, f32::INFINITY] {
        let bytes = DefaultCoder::encode(&value).unwrap();
        assert_eq!(bytes, DefaultCoder::encode(&f64::from(value)).unwrap());
        assert_eq!(<f32 as DefaultCoder>::decode(&bytes).unwrap(), value);
    }
    // A double outside the `f32` range decodes as an infinity.
    let large = DefaultCoder::encode(&f64::MAX).unwrap();
    assert_eq!(
        <f32 as DefaultCoder>::decode(&large).unwrap(),
        f32::INFINITY
    );
}

#[test]
fn primitive_elements_round_trip_and_respect_context() {
    // Nested strings and bytes carry a VarInt length prefix; whole-stream ones do not.
    let text = "apache beam".to_string();
    let mut nested = Vec::new();
    StringUtf8Coder
        .encode(&text, &mut nested, Context::Nested)
        .unwrap();
    assert_eq!(nested[0], 11);
    assert_eq!(&nested[1..], b"apache beam");
    assert_eq!(text.encode().unwrap(), nested);
    assert_eq!(String::decode(&nested).unwrap(), text);

    let mut whole = Vec::new();
    StringUtf8Coder
        .encode(&text, &mut whole, Context::WholeStream)
        .unwrap();
    assert_eq!(whole, b"apache beam");
    assert_eq!(
        StringUtf8Coder
            .decode(&mut &whole[..], Context::WholeStream)
            .unwrap(),
        text
    );

    let payload = vec![0xDE, 0xAD, 0xBE, 0xEF];
    assert_eq!(
        Vec::<u8>::decode(&payload.encode().unwrap()).unwrap(),
        payload
    );

    for n in [0i64, 1, -1, 300, i64::MIN, i64::MAX] {
        assert_eq!(i64::decode(&n.encode().unwrap()).unwrap(), n);
    }
    for n in [0i32, -42, i32::MIN, i32::MAX] {
        assert_eq!(i32::decode(&n.encode().unwrap()).unwrap(), n);
    }
    // An i64 outside the i32 range fails to decode as i32.
    assert!(i32::decode(&(i64::from(i32::MAX) + 1).encode().unwrap()).is_err());

    // A nested `()` writes a zero length byte.
    assert_eq!(().encode().unwrap(), [0x00]);
    <()>::decode(&[0x00]).unwrap();
}
