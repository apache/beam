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
package org.apache.beam.runners.fnexecution.control;

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.values.KV;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TimerReceiverFactoryTest {
  @Rule public transient Timeout globalTimeout = Timeout.seconds(600);

  @Test
  public void testEncodeAndDecode() {
    KV<String, String> expected = KV.of("123:\"ab\n'c:", "456:sdf:d");
    assertEquals(
        expected,
        TimerReceiverFactory.decodeTimerDataTimerId(
            TimerReceiverFactory.encodeToTimerDataTimerId(expected.getKey(), expected.getValue())));
  }
}
