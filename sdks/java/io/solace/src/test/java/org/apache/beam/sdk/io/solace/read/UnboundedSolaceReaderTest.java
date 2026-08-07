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
package org.apache.beam.sdk.io.solace.read;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.XMLMessage.Outcome;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.beam.sdk.io.solace.MockSempClientFactory;
import org.apache.beam.sdk.io.solace.MockSessionServiceFactory;
import org.apache.beam.sdk.io.solace.data.Solace;
import org.apache.beam.sdk.io.solace.data.SolaceDataUtils;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class UnboundedSolaceReaderTest {

  @Test
  public void testCheckpointTimeoutAndNack() throws Exception {
    AtomicReference<Outcome> settledOutcome = new AtomicReference<>();

    // 1. Create a mock message that captures the settle outcome
    SerializableFunction<Integer, BytesXMLMessage> recordFn =
        index -> {
          if (index == 0) {
            return SolaceDataUtils.getBytesXmlMessageWithSettle(
                "payload_test0",
                "450",
                null, // ackCallback
                settledOutcome::set // settleCallback
                );
          }
          return null;
        };

    MockSessionServiceFactory fakeSessionServiceFactory =
        MockSessionServiceFactory.builder().recordFn(recordFn).minMessagesReceived(1).build();

    // 2. Create the source directly
    UnboundedSolaceSource<Solace.Record> source =
        new UnboundedSolaceSource<>(
            com.solacesystems.jcsmp.JCSMPFactory.onlyInstance().createQueue("queue"),
            MockSempClientFactory.getDefaultMock(),
            fakeSessionServiceFactory,
            1, // maxNumConnections
            false, // enableDeduplication
            null, // coder (not needed for direct reader test if we don't serialize)
            input -> org.joda.time.Instant.ofEpochMilli(1000L), // timestampFn
            org.joda.time.Duration.standardSeconds(1), // watermarkIdleDurationThreshold
            input -> SolaceDataUtils.getSolaceRecord("payload_test0", "450"), // parseFn
            org.joda.time.Duration.standardSeconds(30) // ackDeadline
            );

    UnboundedSolaceReader<Solace.Record> reader =
        (UnboundedSolaceReader<Solace.Record>)
            source.createReader(PipelineOptionsFactory.create(), null);

    // 3. Inject a controllable clock
    AtomicReference<Long> currentTime = new AtomicReference<>(1000L);
    reader.clock = currentTime::get;

    // 4. Start the reader (ingests message 0)
    assertTrue(reader.start());

    // 5. Create a checkpoint (T1)
    reader.getCheckpointMark();

    // Verify no nack yet
    assertEquals(null, settledOutcome.get());

    // 6. Advance time by 10 seconds (less than 30s timeout) and advance reader
    currentTime.set(11000L);
    reader.advance(); // This calls checkTimeouts()
    assertEquals(null, settledOutcome.get());

    // 7. Advance time by 21 more seconds (total 31 seconds, > 30s timeout) and advance reader
    currentTime.set(32000L);
    reader.advance(); // This calls checkTimeouts() which should trigger Nack

    // 8. Wait for async nack to complete (since it runs in ackExecutor)
    // We shut down the reader which awaits termination of the executor
    reader.close();

    // 9. Verify that the message was Nacked with FAILED outcome
    assertEquals(Outcome.FAILED, settledOutcome.get());
  }
}
