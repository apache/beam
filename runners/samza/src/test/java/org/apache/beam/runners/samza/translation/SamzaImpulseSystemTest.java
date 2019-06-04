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
package org.apache.beam.runners.samza.translation;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.samza.runtime.OpMessage;
import org.apache.samza.Partition;
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.WatermarkMessage;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link
 * org.apache.beam.runners.samza.translation.SamzaImpulseSystemFactory.SamzaImpulseSystemConsumer}.
 */
public class SamzaImpulseSystemTest {
  @Test
  public void testSamzaImpulseSystemConsumer() throws Exception {
    SystemConsumer consumer =
        new SamzaImpulseSystemFactory().getConsumer("default-system", new MapConfig(), null);
    Map<SystemStreamPartition, List<IncomingMessageEnvelope>> result =
        consumer.poll(Collections.singleton(sspForPartition(0)), 100);
    Assert.assertEquals(1, result.size());
    Assert.assertTrue(result.containsKey(sspForPartition(0)));

    List<IncomingMessageEnvelope> messageEnvelopes = result.get(sspForPartition(0));
    Assert.assertEquals(3, messageEnvelopes.size());

    Assert.assertTrue(messageEnvelopes.get(0).getMessage() instanceof OpMessage);
    OpMessage impulseEvent = (OpMessage) messageEnvelopes.get(0).getMessage();
    Assert.assertEquals(OpMessage.Type.ELEMENT, impulseEvent.getType());

    Assert.assertTrue(messageEnvelopes.get(1).getMessage() instanceof WatermarkMessage);

    Assert.assertTrue(messageEnvelopes.get(2).isEndOfStream());
  }

  private SystemStreamPartition sspForPartition(int i) {
    return new SystemStreamPartition("default-system", "default-stream", new Partition(i));
  }
}
