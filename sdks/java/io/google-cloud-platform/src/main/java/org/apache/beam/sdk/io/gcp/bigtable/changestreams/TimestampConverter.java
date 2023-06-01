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

import org.apache.beam.sdk.annotations.Internal;

/** Convert between different Timestamp and Instant classes. */
@Internal
public class TimestampConverter {

  public static org.threeten.bp.Instant toThreetenInstant(org.joda.time.Instant jodaInstant) {
    return org.threeten.bp.Instant.ofEpochMilli(jodaInstant.getMillis());
  }

  public static org.joda.time.Instant toJodaTime(org.threeten.bp.Instant threetenInstant) {
    return org.joda.time.Instant.ofEpochMilli(threetenInstant.toEpochMilli());
  }

  public static long toSeconds(org.joda.time.Instant jodaInstant) {
    return jodaInstant.getMillis() / 1000;
  }

  public static org.joda.time.Instant microsecondToInstant(long microsecond) {
    return org.joda.time.Instant.ofEpochMilli(microsecond / 1_000L);
  }
}
