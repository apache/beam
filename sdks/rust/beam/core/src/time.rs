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

//! Event time.

use std::fmt;
use std::ops::{Add, AddAssign, Sub, SubAssign};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::coders::{Coder, CoderError, CoderRegistry, Context, DefaultCoder, VarIntCoder};

/// A point in event or processing time, with millisecond precision.
///
/// Arithmetic with [`Duration`] saturates at [`Instant::MIN`] and [`Instant::MAX`]. It never
/// moves a value in the wrong direction. Serde writes the milliseconds.
///
/// ```
/// use std::time::Duration;
/// use beam::Instant;
///
/// let t = Instant::from_millis(1_000);
/// assert_eq!(t + Duration::from_secs(1), Instant::from_millis(2_000));
/// assert_eq!(t.to_string(), "1970-01-01T00:00:01.000Z");
/// ```
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Instant(i64);

impl Instant {
    /// The smallest Beam timestamp, `MIN_TIMESTAMP_MILLIS` of the Runner API. The runner
    /// gives it to the output of `Impulse`.
    pub const MIN: Self = Self(i64::MIN / 1000);
    /// The largest Beam timestamp, `MAX_TIMESTAMP_MILLIS` of the Runner API. A watermark
    /// at this value closes all windows.
    pub const MAX: Self = Self(i64::MAX / 1000);
    /// The last timestamp of the global window, `GLOBAL_WINDOW_MAX_TIMESTAMP_MILLIS` of the
    /// Runner API. It is one day before [`Instant::MAX`].
    pub const END_OF_GLOBAL_WINDOW: Self = Self(Self::MAX.0 - 86_400_000);
    /// 1970-01-01T00:00:00Z.
    pub const EPOCH: Self = Self(0);

    /// The instant `millis` milliseconds after the epoch.
    pub const fn from_millis(millis: i64) -> Self {
        Self(millis)
    }

    /// Milliseconds since the epoch.
    pub const fn as_millis(self) -> i64 {
        self.0
    }

    /// The current system time.
    pub fn now() -> Self {
        SystemTime::now().into()
    }

    /// The time from `earlier` to `self`, or `None` if `earlier` is later.
    pub fn duration_since(self, earlier: Self) -> Option<Duration> {
        u64::try_from(self.0.checked_sub(earlier.0)?)
            .ok()
            .map(Duration::from_millis)
    }

    fn millis(d: Duration) -> i64 {
        i64::try_from(d.as_millis()).unwrap_or(i64::MAX)
    }
}

impl From<SystemTime> for Instant {
    fn from(t: SystemTime) -> Self {
        match t.duration_since(UNIX_EPOCH) {
            Ok(d) => Self::EPOCH + d,
            Err(e) => Self::EPOCH - e.duration(),
        }
    }
}

impl Add<Duration> for Instant {
    type Output = Self;
    fn add(self, d: Duration) -> Self {
        // `max(self)` keeps a value above `MAX` where it is.
        Self(
            self.0
                .saturating_add(Self::millis(d))
                .min(Self::MAX.0)
                .max(self.0),
        )
    }
}

impl Sub<Duration> for Instant {
    type Output = Self;
    fn sub(self, d: Duration) -> Self {
        // `min(self)` keeps a value below `MIN` where it is.
        Self(
            self.0
                .saturating_sub(Self::millis(d))
                .max(Self::MIN.0)
                .min(self.0),
        )
    }
}

impl AddAssign<Duration> for Instant {
    fn add_assign(&mut self, d: Duration) {
        *self = *self + d;
    }
}

impl SubAssign<Duration> for Instant {
    fn sub_assign(&mut self, d: Duration) {
        *self = *self - d;
    }
}

/// Formats as RFC 3339 in UTC with milliseconds, for example `2024-05-01T12:00:00.000Z`.
/// A year that chrono cannot show, as in [`Instant::MAX`], formats as `<millis>ms`.
impl fmt::Display for Instant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match chrono::DateTime::from_timestamp_millis(self.0) {
            Some(t) => write!(f, "{}", t.format("%Y-%m-%dT%H:%M:%S%.3fZ")),
            None => write!(f, "{}ms", self.0),
        }
    }
}

impl fmt::Debug for Instant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

/// Encodes the milliseconds as a varint.
impl Coder<Instant> for VarIntCoder {
    fn urn(&self) -> &'static str {
        Coder::<i64>::urn(self)
    }

    fn encode(&self, value: &Instant, writer: &mut Vec<u8>, cx: Context) -> Result<(), CoderError> {
        Coder::<i64>::encode(self, &value.0, writer, cx)
    }

    fn decode(&self, reader: &mut &[u8], cx: Context) -> Result<Instant, CoderError> {
        Coder::<i64>::decode(self, reader, cx).map(Instant)
    }
}

impl DefaultCoder for Instant {
    type Coder = VarIntCoder;
    fn coder() -> Self::Coder {
        VarIntCoder
    }

    fn encode_element(&self, writer: &mut Vec<u8>) -> Result<(), CoderError> {
        self.0.encode_element(writer)
    }

    fn decode_element(reader: &mut &[u8]) -> Result<Self, CoderError> {
        i64::decode_element(reader).map(Self)
    }

    fn register_coder<R: CoderRegistry + ?Sized>(registry: &R) -> String {
        i64::register_coder(registry)
    }
}
