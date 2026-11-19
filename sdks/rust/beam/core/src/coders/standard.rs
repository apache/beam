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

//! Standard primitive Beam coders and the wire helpers that the other coders share.

use std::io::Write;

use super::traits::{Coder, CoderError, CoderRegistry, Context, DefaultCoder};
use super::{URN_BOOL, URN_BYTES, URN_DOUBLE, URN_STRING_UTF8, URN_VARINT};

fn unexpected_eof(wanted: usize, left: usize) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::UnexpectedEof,
        format!("expected {wanted} bytes, {left} left"),
    )
}

/// Splits the next `len` bytes off `reader` and returns them, borrowed.
pub fn take_bytes<'a>(reader: &mut &'a [u8], len: usize) -> Result<&'a [u8], std::io::Error> {
    if reader.len() < len {
        return Err(unexpected_eof(len, reader.len()));
    }
    let (head, rest) = reader.split_at(len);
    *reader = rest;
    Ok(head)
}

pub(super) fn read_array<const N: usize>(reader: &mut &[u8]) -> Result<[u8; N], std::io::Error> {
    let bytes = take_bytes(reader, N)?;
    let mut buf = [0u8; N];
    buf.copy_from_slice(bytes);
    Ok(buf)
}

/// Reads `len` bytes into a new buffer. The length is checked against the input first, so a
/// corrupt length cannot allocate more than the input holds.
pub(super) fn read_exact_vec(reader: &mut &[u8], len: usize) -> Result<Vec<u8>, std::io::Error> {
    take_bytes(reader, len).map(<[u8]>::to_vec)
}

/// Reads a varint length prefix. A negative length is an error, not a huge `usize`.
pub(super) fn read_varint_len(reader: &mut &[u8]) -> Result<usize, CoderError> {
    let len = VarIntCoder::decode_varint(reader)?;
    usize::try_from(len).map_err(|_| CoderError::Format(format!("Negative length: {len}")))
}

/// Writes a byte payload, with a varint length prefix in `Nested` context.
fn write_bytes(value: &[u8], writer: &mut Vec<u8>, context: Context) -> Result<(), CoderError> {
    if context == Context::Nested {
        VarIntCoder::encode_varint(value.len() as i64, writer)?;
    }
    writer.write_all(value)?;
    Ok(())
}

/// Reads a byte payload: varint length-prefixed in `Nested` context, else the rest of the stream.
fn read_bytes(reader: &mut &[u8], context: Context) -> Result<Vec<u8>, CoderError> {
    match context {
        Context::Nested => {
            let len = read_varint_len(reader)?;
            Ok(read_exact_vec(reader, len)?)
        }
        Context::WholeStream => Ok(std::mem::take(reader).to_vec()),
    }
}

/// Coder for UTF-8 strings.
#[derive(Clone, Debug, Default)]
pub struct StringUtf8Coder;

impl Coder<String> for StringUtf8Coder {
    fn urn(&self) -> &'static str {
        URN_STRING_UTF8
    }

    fn encode(
        &self,
        value: &String,
        writer: &mut Vec<u8>,
        context: Context,
    ) -> Result<(), CoderError> {
        write_bytes(value.as_bytes(), writer, context)
    }

    fn decode(&self, reader: &mut &[u8], context: Context) -> Result<String, CoderError> {
        let bytes = read_bytes(reader, context)?;
        Ok(String::from_utf8(bytes)?)
    }
}

impl DefaultCoder for String {
    type Coder = StringUtf8Coder;
    fn coder() -> Self::Coder {
        StringUtf8Coder
    }

    fn encode_element(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        StringUtf8Coder.encode(self, writer, Context::Nested)
    }

    fn decode_element(reader: &mut &[u8]) -> Result<Self, CoderError> {
        StringUtf8Coder.decode(reader, Context::Nested)
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        registry.register_coder(URN_STRING_UTF8, Vec::new())
    }
}

/// Coder for raw byte vectors.
#[derive(Clone, Debug, Default)]
pub struct BytesCoder;

impl Coder<Vec<u8>> for BytesCoder {
    fn urn(&self) -> &'static str {
        URN_BYTES
    }

    fn encode(
        &self,
        value: &Vec<u8>,
        writer: &mut Vec<u8>,
        context: Context,
    ) -> Result<(), CoderError> {
        write_bytes(value, writer, context)
    }

    fn decode(&self, reader: &mut &[u8], context: Context) -> Result<Vec<u8>, CoderError> {
        read_bytes(reader, context)
    }
}

impl DefaultCoder for Vec<u8> {
    type Coder = BytesCoder;
    fn coder() -> Self::Coder {
        BytesCoder
    }

    fn encode_element(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        BytesCoder.encode(self, writer, Context::Nested)
    }

    fn decode_element(reader: &mut &[u8]) -> Result<Self, CoderError> {
        BytesCoder.decode(reader, Context::Nested)
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        registry.register_coder(URN_BYTES, Vec::new())
    }
}

/// Coder for booleans, as one byte: 0 is false and 1 is true. Other values are an error.
#[derive(Clone, Debug, Default)]
pub struct BoolCoder;

impl Coder<bool> for BoolCoder {
    fn urn(&self) -> &'static str {
        URN_BOOL
    }

    fn encode(
        &self,
        value: &bool,
        writer: &mut Vec<u8>,
        _context: Context,
    ) -> Result<(), CoderError> {
        writer.write_all(&[u8::from(*value)])?;
        Ok(())
    }

    fn decode(&self, reader: &mut &[u8], _context: Context) -> Result<bool, CoderError> {
        let [byte] = read_array(reader)?;
        match byte {
            0 => Ok(false),
            1 => Ok(true),
            other => Err(CoderError::Format(format!("Invalid boolean byte: {other}"))),
        }
    }
}

impl DefaultCoder for bool {
    type Coder = BoolCoder;
    fn coder() -> Self::Coder {
        BoolCoder
    }

    fn encode_element(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        BoolCoder.encode(self, writer, Context::WholeStream)
    }

    fn decode_element(reader: &mut &[u8]) -> Result<Self, CoderError> {
        BoolCoder.decode(reader, Context::WholeStream)
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        registry.register_coder(URN_BOOL, Vec::new())
    }
}

/// Coder for 64-bit IEEE 754 floating-point values, big-endian.
#[derive(Clone, Debug, Default)]
pub struct DoubleCoder;

impl Coder<f64> for DoubleCoder {
    fn urn(&self) -> &'static str {
        URN_DOUBLE
    }

    fn encode(
        &self,
        value: &f64,
        writer: &mut Vec<u8>,
        _context: Context,
    ) -> Result<(), CoderError> {
        writer.write_all(&value.to_be_bytes())?;
        Ok(())
    }

    fn decode(&self, reader: &mut &[u8], _context: Context) -> Result<f64, CoderError> {
        Ok(f64::from_be_bytes(read_array(reader)?))
    }
}

impl DefaultCoder for f64 {
    type Coder = DoubleCoder;
    fn coder() -> Self::Coder {
        DoubleCoder
    }

    fn encode_element(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        DoubleCoder.encode(self, writer, Context::WholeStream)
    }

    fn decode_element(reader: &mut &[u8]) -> Result<Self, CoderError> {
        DoubleCoder.decode(reader, Context::WholeStream)
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        registry.register_coder(URN_DOUBLE, Vec::new())
    }
}

/// Coder for 32-bit floats, in the format of `beam:coder:double:v1`, so other SDKs read a
/// double. A double outside the `f32` range decodes as an infinity.
#[derive(Clone, Debug, Default)]
pub struct FloatCoder;

impl Coder<f32> for FloatCoder {
    fn urn(&self) -> &'static str {
        URN_DOUBLE
    }

    fn encode(
        &self,
        value: &f32,
        writer: &mut Vec<u8>,
        context: Context,
    ) -> Result<(), CoderError> {
        DoubleCoder.encode(&f64::from(*value), writer, context)
    }

    fn decode(&self, reader: &mut &[u8], context: Context) -> Result<f32, CoderError> {
        // The narrowing is the documented behavior: an `f32` written here comes back exactly.
        DoubleCoder
            .decode(reader, context)
            .map(|value| value as f32)
    }
}

impl DefaultCoder for f32 {
    type Coder = FloatCoder;
    fn coder() -> Self::Coder {
        FloatCoder
    }

    fn encode_element(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        FloatCoder.encode(self, writer, Context::WholeStream)
    }

    fn decode_element(reader: &mut &[u8]) -> Result<Self, CoderError> {
        FloatCoder.decode(reader, Context::WholeStream)
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        registry.register_coder(URN_DOUBLE, Vec::new())
    }
}

/// Coder for 64-bit varints. A negative value encodes as its two's-complement `u64` (10 bytes).
#[derive(Clone, Debug, Default)]
pub struct VarIntCoder;

impl VarIntCoder {
    pub(crate) fn encode_varint_bytes(value: i64, buf: &mut [u8; 10]) -> usize {
        let mut v = value as u64;
        let mut len = 0;
        loop {
            if (v & !0x7F) == 0 {
                buf[len] = v as u8;
                return len + 1;
            }
            buf[len] = ((v & 0x7F) as u8) | 0x80;
            len += 1;
            v >>= 7;
        }
    }

    pub fn encode_varint(value: i64, writer: &mut impl Write) -> Result<(), std::io::Error> {
        let mut buf = [0u8; 10];
        let len = Self::encode_varint_bytes(value, &mut buf);
        writer.write_all(&buf[..len])
    }

    /// Appends the 64-bit varint encoding of `value` to `out`.
    pub fn encode_varint_to_vec(value: i64, out: &mut Vec<u8>) {
        let mut buf = [0u8; 10];
        let len = Self::encode_varint_bytes(value, &mut buf);
        out.extend_from_slice(&buf[..len]);
    }

    /// Returns an error if the varint is longer than 64 bits.
    pub fn decode_varint(reader: &mut &[u8]) -> Result<i64, std::io::Error> {
        let mut result: u64 = 0;
        let mut shift = 0;
        loop {
            let (&byte, rest) = reader.split_first().ok_or_else(|| unexpected_eof(1, 0))?;
            *reader = rest;
            let bits = (byte & 0x7F) as u64;
            if shift >= 64 || (shift == 63 && bits > 1) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "varint too long or out of range",
                ));
            }
            result |= bits << shift;
            shift += 7;
            if (byte & 0x80) == 0 {
                break;
            }
        }
        Ok(result as i64)
    }
}

impl Coder<i64> for VarIntCoder {
    fn urn(&self) -> &'static str {
        URN_VARINT
    }

    fn encode(
        &self,
        value: &i64,
        writer: &mut Vec<u8>,
        _context: Context,
    ) -> Result<(), CoderError> {
        Self::encode_varint(*value, writer)?;
        Ok(())
    }

    fn decode(&self, reader: &mut &[u8], _context: Context) -> Result<i64, CoderError> {
        let v = Self::decode_varint(reader)?;
        Ok(v)
    }
}

impl DefaultCoder for i64 {
    type Coder = VarIntCoder;
    fn coder() -> Self::Coder {
        VarIntCoder
    }

    fn encode_element(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        Coder::<i64>::encode(&VarIntCoder, self, writer, Context::WholeStream)
    }

    fn decode_element(reader: &mut &[u8]) -> Result<Self, CoderError> {
        Coder::<i64>::decode(&VarIntCoder, reader, Context::WholeStream)
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        registry.register_coder(URN_VARINT, Vec::new())
    }
}

impl Coder<i32> for VarIntCoder {
    fn urn(&self) -> &'static str {
        URN_VARINT
    }

    fn encode(
        &self,
        value: &i32,
        writer: &mut Vec<u8>,
        _context: Context,
    ) -> Result<(), CoderError> {
        Self::encode_varint(*value as i64, writer)?;
        Ok(())
    }

    fn decode(&self, reader: &mut &[u8], _context: Context) -> Result<i32, CoderError> {
        let v = Self::decode_varint(reader)?;
        i32::try_from(v)
            .map_err(|_| CoderError::Format(format!("varint {v} is outside the i32 range")))
    }
}

impl DefaultCoder for i32 {
    type Coder = VarIntCoder;
    fn coder() -> Self::Coder {
        VarIntCoder
    }

    fn encode_element(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        Coder::<i32>::encode(&VarIntCoder, self, writer, Context::WholeStream)
    }

    fn decode_element(reader: &mut &[u8]) -> Result<Self, CoderError> {
        Coder::<i32>::decode(&VarIntCoder, reader, Context::WholeStream)
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        registry.register_coder(URN_VARINT, Vec::new())
    }
}

/// Coder for the unit type `()`. It uses the bytes coder URN and encodes an empty payload.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct UnitCoder;

impl Coder<()> for UnitCoder {
    fn urn(&self) -> &'static str {
        URN_BYTES
    }

    fn encode(
        &self,
        _value: &(),
        writer: &mut Vec<u8>,
        context: Context,
    ) -> Result<(), CoderError> {
        write_bytes(&[], writer, context)
    }

    fn decode(&self, reader: &mut &[u8], context: Context) -> Result<(), CoderError> {
        let _ = read_bytes(reader, context)?;
        Ok(())
    }
}

impl DefaultCoder for () {
    type Coder = UnitCoder;

    fn coder() -> Self::Coder {
        UnitCoder
    }

    fn encode_element(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        UnitCoder.encode(self, writer, Context::Nested)
    }

    fn decode_element(reader: &mut &[u8]) -> Result<Self, CoderError> {
        UnitCoder.decode(reader, Context::Nested)
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        registry.register_coder(URN_BYTES, Vec::new())
    }
}
