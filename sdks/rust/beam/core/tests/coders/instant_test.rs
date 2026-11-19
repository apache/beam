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

//! Tests for [`beam::Instant`].

use std::time::Duration;

use beam::Instant;
use beam::coders::DefaultCoder;

const RUNNER_API_PROTO: &str = include_str!(
    "../../../../../../model/pipeline/src/main/proto/org/apache/beam/model/pipeline/v1/beam_runner_api.proto"
);

/// The `beam_constant` value of `name` in the `BeamConstants` enum of the Runner API.
fn beam_constant(name: &str) -> i64 {
    let line = RUNNER_API_PROTO
        .lines()
        .find(|line| line.trim_start().starts_with(&format!("{name} =")))
        .unwrap_or_else(|| panic!("{name} is not in beam_runner_api.proto"));
    let value = line.split('"').nth(1).expect("the constant is quoted");
    value.parse().expect("the constant is an integer")
}

#[test]
fn bounds_are_the_runner_api_constants() {
    assert_eq!(Instant::MIN.as_millis(), -9_223_372_036_854_775);
    assert_eq!(Instant::MAX.as_millis(), 9_223_372_036_854_775);
    assert_eq!(
        Instant::END_OF_GLOBAL_WINDOW.as_millis(),
        9_223_371_950_454_775
    );
    assert_eq!(
        Instant::MIN.as_millis(),
        beam_constant("MIN_TIMESTAMP_MILLIS")
    );
    assert_eq!(
        Instant::MAX.as_millis(),
        beam_constant("MAX_TIMESTAMP_MILLIS")
    );
    assert_eq!(
        Instant::END_OF_GLOBAL_WINDOW.as_millis(),
        beam_constant("GLOBAL_WINDOW_MAX_TIMESTAMP_MILLIS")
    );
}

#[test]
fn arithmetic_saturates_at_the_beam_range() {
    let day = Duration::from_secs(86_400);
    assert_eq!(Instant::MAX + Duration::from_secs(1), Instant::MAX);
    assert_eq!(Instant::MIN - Duration::from_secs(1), Instant::MIN);
    assert_eq!(Instant::END_OF_GLOBAL_WINDOW + day, Instant::MAX);
    assert_eq!(Instant::MAX - day, Instant::END_OF_GLOBAL_WINDOW);
    assert_eq!(Instant::EPOCH + Duration::MAX, Instant::MAX);
    assert_eq!(Instant::EPOCH - Duration::MAX, Instant::MIN);
}

#[test]
fn arithmetic_never_moves_backwards() {
    let instants = [
        Instant::from_millis(i64::MIN),
        Instant::from_millis(Instant::MIN.as_millis() - 1),
        Instant::MIN,
        Instant::EPOCH,
        Instant::END_OF_GLOBAL_WINDOW,
        Instant::MAX,
        Instant::from_millis(Instant::MAX.as_millis() + 1),
        Instant::from_millis(i64::MAX),
    ];
    let durations = [
        Duration::ZERO,
        Duration::from_millis(1),
        Duration::from_secs(86_400),
        Duration::MAX,
    ];
    for t in instants {
        for d in durations {
            assert!(t + d >= t, "{t:?} + {d:?} is before {t:?}");
            assert!(t - d <= t, "{t:?} - {d:?} is after {t:?}");
        }
    }
}

#[test]
fn duration_since_is_none_for_a_later_instant() {
    let t = Instant::from_millis(5_000);
    assert_eq!(
        t.duration_since(Instant::EPOCH),
        Some(Duration::from_secs(5))
    );
    assert_eq!(Instant::EPOCH.duration_since(t), None);
}

#[test]
fn displays_as_rfc3339() {
    assert_eq!(
        Instant::from_millis(253_402_300_799_999).to_string(),
        "9999-12-31T23:59:59.999Z"
    );
    assert_eq!(
        Instant::from_millis(-1).to_string(),
        "1969-12-31T23:59:59.999Z"
    );
}

#[test]
fn displays_the_bounds_as_milliseconds() {
    assert_eq!(Instant::MIN.to_string(), "-9223372036854775ms");
    assert_eq!(Instant::MAX.to_string(), "9223372036854775ms");
    assert_eq!(
        Instant::END_OF_GLOBAL_WINDOW.to_string(),
        "9223371950454775ms"
    );
}

#[test]
fn round_trips_through_its_coder() {
    let t = Instant::from_millis(-42);
    assert_eq!(Instant::decode(&t.encode().unwrap()).unwrap(), t);
}
