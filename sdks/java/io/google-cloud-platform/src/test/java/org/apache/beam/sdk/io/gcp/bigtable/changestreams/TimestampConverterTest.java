/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.gcp.bigtable.changestreams;

import static org.junit.Assert.assertEquals;

import org.joda.time.Instant;
import org.junit.Test;

public class TimestampConverterTest {

  @Test
  public void testCloudTimestampTotoInstant() {
    int nanos = 123000000; // 123 ms
    com.google.cloud.Timestamp timestamp =
        com.google.cloud.Timestamp.ofTimeSecondsAndNanos(1000, nanos);
    Instant instant = TimestampConverter.toInstant(timestamp);
    assertEquals(1000123, instant.getMillis());
  }

  @Test
  public void testProtoTimestampTotoInstant() {
    int nanos = 123000000; // 123 ms
    com.google.protobuf.Timestamp timestamp =
        com.google.protobuf.Timestamp.newBuilder().setSeconds(1000).setNanos(nanos).build();
    Instant instant = TimestampConverter.toInstant(timestamp);
    assertEquals(1000123, instant.getMillis());
  }
}
