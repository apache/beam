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
package org.apache.beam.runners.samza.runtime;

import java.io.ByteArrayOutputStream;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;

/** Tests for {@link KeyedTimerData}. */
public class KeyedTimerDataTest {
  private static final Coder<String> STRING_CODER = StringUtf8Coder.of();
  private static final Instant TIMESTAMP =
      new DateTime(2020, 8, 11, 13, 42, 9, DateTimeZone.UTC).toInstant();
  private static final Instant OUTPUT_TIMESTAMP = TIMESTAMP.plus(Duration.standardSeconds(30));

  @Test
  public void testCoder() throws Exception {
    final TimerInternals.TimerData td =
        TimerInternals.TimerData.of(
            "timer", StateNamespaces.global(), TIMESTAMP, OUTPUT_TIMESTAMP, TimeDomain.EVENT_TIME);

    final String key = "timer-key";
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    STRING_CODER.encode(key, baos);
    final byte[] keyBytes = baos.toByteArray();
    final KeyedTimerData<String> ktd = new KeyedTimerData<>(keyBytes, key, td);

    final KeyedTimerData.KeyedTimerDataCoder<String> ktdCoder =
        new KeyedTimerData.KeyedTimerDataCoder<>(STRING_CODER, GlobalWindow.Coder.INSTANCE);

    CoderProperties.coderDecodeEncodeEqual(ktdCoder, ktd);
  }
}
