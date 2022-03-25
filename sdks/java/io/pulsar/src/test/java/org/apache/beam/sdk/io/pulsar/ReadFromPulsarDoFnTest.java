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
package org.apache.beam.sdk.io.pulsar;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.internal.DefaultImplementation;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ReadFromPulsarDoFnTest {

  public static final String SERVICE_URL = "pulsar://localhost:6650";
  public static final String ADMIN_URL = "http://localhost:8080";
  public static final String TOPIC = "PULSARIO_READFROMPULSAR_TEST";
  public static final int NUMBEROFMESSAGES = 100;

  private final ReadFromPulsarDoFn dofnInstance = new ReadFromPulsarDoFn(readSourceDescriptor());
  public FakePulsarReader fakePulsarReader = new FakePulsarReader(TOPIC, NUMBEROFMESSAGES);
  private FakePulsarClient fakePulsarClient = new FakePulsarClient(fakePulsarReader);

  private PulsarIO.Read readSourceDescriptor() {
    return PulsarIO.read()
        .withClientUrl(SERVICE_URL)
        .withTopic(TOPIC)
        .withAdminUrl(ADMIN_URL)
        .withPublishTime()
        .withPulsarClient(
            new SerializableFunction<String, PulsarClient>() {
              @Override
              public PulsarClient apply(String input) {
                return fakePulsarClient;
              }
            });
  }

  @Before
  public void setup() throws Exception {
    dofnInstance.initPulsarClients();
    fakePulsarReader.reset();
  }

  @Test
  public void testInitialRestrictionWhenHasStartOffset() throws Exception {
    long expectedStartOffset = 0;
    OffsetRange result =
        dofnInstance.getInitialRestriction(
            PulsarSourceDescriptor.of(
                TOPIC, expectedStartOffset, null, null, SERVICE_URL, ADMIN_URL));
    assertEquals(new OffsetRange(expectedStartOffset, Long.MAX_VALUE), result);
  }

  @Test
  public void testInitialRestrictionWithConsumerPosition() throws Exception {
    long expectedStartOffset = Instant.now().getMillis();
    OffsetRange result =
        dofnInstance.getInitialRestriction(
            PulsarSourceDescriptor.of(
                TOPIC, expectedStartOffset, null, null, SERVICE_URL, ADMIN_URL));
    assertEquals(new OffsetRange(expectedStartOffset, Long.MAX_VALUE), result);
  }

  @Test
  public void testInitialRestrictionWithConsumerEndPosition() throws Exception {
    long startOffset = fakePulsarReader.getStartTimestamp();
    long endOffset = fakePulsarReader.getEndTimestamp();
    OffsetRange result =
        dofnInstance.getInitialRestriction(
            PulsarSourceDescriptor.of(TOPIC, startOffset, endOffset, null, SERVICE_URL, ADMIN_URL));
    assertEquals(new OffsetRange(startOffset, endOffset), result);
  }

  @Test
  public void testProcessElement() throws Exception {
    MockOutputReceiver receiver = new MockOutputReceiver();
    long startOffset = fakePulsarReader.getStartTimestamp();
    long endOffset = fakePulsarReader.getEndTimestamp();
    OffsetRangeTracker tracker = new OffsetRangeTracker(new OffsetRange(startOffset, endOffset));
    PulsarSourceDescriptor descriptor =
        PulsarSourceDescriptor.of(TOPIC, startOffset, endOffset, null, SERVICE_URL, ADMIN_URL);
    DoFn.ProcessContinuation result =
        dofnInstance.processElement(descriptor, tracker, null, (DoFn.OutputReceiver) receiver);
    int expectedResultWithoutCountingLastOffset = NUMBEROFMESSAGES - 1;
    assertEquals(DoFn.ProcessContinuation.stop(), result);
    assertEquals(expectedResultWithoutCountingLastOffset, receiver.getOutputs().size());
  }

  @Test
  public void testProcessElementWhenEndMessageIdIsDefined() throws Exception {
    MockOutputReceiver receiver = new MockOutputReceiver();
    OffsetRangeTracker tracker = new OffsetRangeTracker(new OffsetRange(0L, Long.MAX_VALUE));
    MessageId endMessageId = DefaultImplementation.newMessageId(50L, 50L, 50);
    DoFn.ProcessContinuation result =
        dofnInstance.processElement(
            PulsarSourceDescriptor.of(TOPIC, null, null, endMessageId, SERVICE_URL, ADMIN_URL),
            tracker,
            null,
            (DoFn.OutputReceiver) receiver);
    assertEquals(DoFn.ProcessContinuation.stop(), result);
    assertEquals(50, receiver.getOutputs().size());
  }

  @Test
  public void testProcessElementWithEmptyRecords() throws Exception {
    MockOutputReceiver receiver = new MockOutputReceiver();
    fakePulsarReader.emptyMockRecords();
    OffsetRangeTracker tracker = new OffsetRangeTracker(new OffsetRange(0L, Long.MAX_VALUE));
    DoFn.ProcessContinuation result =
        dofnInstance.processElement(
            PulsarSourceDescriptor.of(TOPIC, null, null, null, SERVICE_URL, ADMIN_URL),
            tracker,
            null,
            (DoFn.OutputReceiver) receiver);
    assertEquals(DoFn.ProcessContinuation.resume(), result);
    assertTrue(receiver.getOutputs().isEmpty());
  }

  @Test
  public void testProcessElementWhenHasReachedEndTopic() throws Exception {
    MockOutputReceiver receiver = new MockOutputReceiver();
    fakePulsarReader.setReachedEndOfTopic(true);
    OffsetRangeTracker tracker = new OffsetRangeTracker(new OffsetRange(0L, Long.MAX_VALUE));
    DoFn.ProcessContinuation result =
        dofnInstance.processElement(
            PulsarSourceDescriptor.of(TOPIC, null, null, null, SERVICE_URL, ADMIN_URL),
            tracker,
            null,
            (DoFn.OutputReceiver) receiver);
    assertEquals(DoFn.ProcessContinuation.stop(), result);
  }

  private static class MockOutputReceiver implements DoFn.OutputReceiver<PulsarMessage> {

    private final List<PulsarMessage> records = new ArrayList<>();

    @Override
    public void output(PulsarMessage output) {}

    @Override
    public void outputWithTimestamp(
        PulsarMessage output, @UnknownKeyFor @NonNull @Initialized Instant timestamp) {
      records.add(output);
    }

    public List<PulsarMessage> getOutputs() {
      return records;
    }
  }
}
