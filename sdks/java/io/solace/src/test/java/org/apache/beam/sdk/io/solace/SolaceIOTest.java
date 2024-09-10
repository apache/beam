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
package org.apache.beam.sdk.io.solace;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.Destination;
import com.solacesystems.jcsmp.Queue;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.impl.ReplicationGroupMessageIdImpl;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.io.solace.SolaceIO.Read;
import org.apache.beam.sdk.io.solace.SolaceIO.Read.Configuration;
import org.apache.beam.sdk.io.solace.broker.SessionServiceFactory;
import org.apache.beam.sdk.io.solace.data.Solace;
import org.apache.beam.sdk.io.solace.data.Solace.Record;
import org.apache.beam.sdk.io.solace.data.SolaceDataUtils;
import org.apache.beam.sdk.io.solace.data.SolaceDataUtils.SimpleRecord;
import org.apache.beam.sdk.io.solace.read.SolaceCheckpointMark;
import org.apache.beam.sdk.io.solace.read.UnboundedSolaceSource;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SolaceIOTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private Read<Record> getDefaultRead() {
    return SolaceIO.read()
        .from(Solace.Queue.fromName("queue"))
        .withSempClientFactory(MockSempClientFactory.getDefaultMock())
        .withSessionServiceFactory(MockSessionServiceFactory.getDefaultMock())
        .withMaxNumConnections(1);
  }

  private Read<Record> getDefaultReadForTopic() {
    return SolaceIO.read()
        .from(Solace.Topic.fromName("topic"))
        .withSempClientFactory(MockSempClientFactory.getDefaultMock())
        .withSessionServiceFactory(MockSessionServiceFactory.getDefaultMock())
        .withMaxNumConnections(1);
  }

  private static BytesXMLMessage getOrNull(Integer index, List<BytesXMLMessage> messages) {
    return index != null && index < messages.size() ? messages.get(index) : null;
  }

  private static UnboundedSolaceSource<Record> getSource(Read<Record> spec, TestPipeline pipeline) {
    Configuration<Record> configuration = spec.configurationBuilder.build();
    return new UnboundedSolaceSource<>(
        configuration.getQueue(),
        configuration.getSempClientFactory(),
        configuration.getSessionServiceFactory(),
        configuration.getMaxNumConnections(),
        configuration.getDeduplicateRecords(),
        spec.inferCoder(pipeline, configuration.getTypeDescriptor()),
        configuration.getTimestampFn(),
        configuration.getWatermarkIdleDurationThreshold(),
        configuration.getParseFn());
  }

  @Test
  public void testReadMessages() {
    // Broker that creates input data
    MockSessionService mockClientService =
        new MockSessionService(
            index -> {
              List<BytesXMLMessage> messages =
                  ImmutableList.of(
                      SolaceDataUtils.getBytesXmlMessage("payload_test0", "450"),
                      SolaceDataUtils.getBytesXmlMessage("payload_test1", "451"),
                      SolaceDataUtils.getBytesXmlMessage("payload_test2", "452"));
              return getOrNull(index, messages);
            },
            3);

    SessionServiceFactory fakeSessionServiceFactory =
        new MockSessionServiceFactory(mockClientService);

    // Expected data
    List<Solace.Record> expected = new ArrayList<>();
    expected.add(SolaceDataUtils.getSolaceRecord("payload_test0", "450"));
    expected.add(SolaceDataUtils.getSolaceRecord("payload_test1", "451"));
    expected.add(SolaceDataUtils.getSolaceRecord("payload_test2", "452"));

    // Run the pipeline
    PCollection<Solace.Record> events =
        pipeline.apply(
            "Read from Solace",
            getDefaultRead().withSessionServiceFactory(fakeSessionServiceFactory));

    // Assert results
    PAssert.that(events).containsInAnyOrder(expected);
    pipeline.run();
  }

  @Test
  public void testReadMessagesWithDeduplication() {
    // Broker that creates input data
    MockSessionService mockClientService =
        new MockSessionService(
            index -> {
              List<BytesXMLMessage> messages =
                  ImmutableList.of(
                      SolaceDataUtils.getBytesXmlMessage("payload_test0", "450"),
                      SolaceDataUtils.getBytesXmlMessage("payload_test1", "451"),
                      SolaceDataUtils.getBytesXmlMessage("payload_test2", "451"));
              return getOrNull(index, messages);
            },
            3);

    SessionServiceFactory fakeSessionServiceFactory =
        new MockSessionServiceFactory(mockClientService);

    // Expected data
    List<Solace.Record> expected = new ArrayList<>();
    expected.add(SolaceDataUtils.getSolaceRecord("payload_test0", "450"));
    expected.add(SolaceDataUtils.getSolaceRecord("payload_test1", "451"));

    // Run the pipeline
    PCollection<Solace.Record> events =
        pipeline.apply(
            "Read from Solace",
            getDefaultRead()
                .withSessionServiceFactory(fakeSessionServiceFactory)
                .withDeduplicateRecords(true));
    // Assert results
    PAssert.that(events).containsInAnyOrder(expected);
    pipeline.run();
  }

  @Test
  public void testReadMessagesWithoutDeduplication() {
    // Broker that creates input data
    MockSessionService mockClientService =
        new MockSessionService(
            index -> {
              List<BytesXMLMessage> messages =
                  ImmutableList.of(
                      SolaceDataUtils.getBytesXmlMessage("payload_test0", "450"),
                      SolaceDataUtils.getBytesXmlMessage("payload_test1", "451"),
                      SolaceDataUtils.getBytesXmlMessage("payload_test2", "451"));
              return getOrNull(index, messages);
            },
            3);
    SessionServiceFactory fakeSessionServiceFactory =
        new MockSessionServiceFactory(mockClientService);

    // Expected data
    List<Solace.Record> expected = new ArrayList<>();
    expected.add(SolaceDataUtils.getSolaceRecord("payload_test0", "450"));
    expected.add(SolaceDataUtils.getSolaceRecord("payload_test1", "451"));
    expected.add(SolaceDataUtils.getSolaceRecord("payload_test2", "451"));

    // Run the pipeline

    PCollection<Solace.Record> events =
        pipeline.apply(
            "Read from Solace",
            getDefaultRead().withSessionServiceFactory(fakeSessionServiceFactory));
    // Assert results
    PAssert.that(events).containsInAnyOrder(expected);
    pipeline.run();
  }

  @Test
  public void testReadMessagesWithDeduplicationOnReplicationGroupMessageId() {
    // Broker that creates input data
    MockSessionService mockClientService =
        new MockSessionService(
            index -> {
              List<BytesXMLMessage> messages =
                  ImmutableList.of(
                      SolaceDataUtils.getBytesXmlMessage(
                          "payload_test0", null, null, new ReplicationGroupMessageIdImpl(2L, 1L)),
                      SolaceDataUtils.getBytesXmlMessage(
                          "payload_test1", null, null, new ReplicationGroupMessageIdImpl(2L, 2L)),
                      SolaceDataUtils.getBytesXmlMessage(
                          "payload_test2", null, null, new ReplicationGroupMessageIdImpl(2L, 2L)));
              return getOrNull(index, messages);
            },
            3);

    SessionServiceFactory fakeSessionServiceFactory =
        new MockSessionServiceFactory(mockClientService);

    // Expected data
    List<Solace.Record> expected = new ArrayList<>();
    expected.add(
        SolaceDataUtils.getSolaceRecord(
            "payload_test0", null, new ReplicationGroupMessageIdImpl(2L, 1L)));
    expected.add(
        SolaceDataUtils.getSolaceRecord(
            "payload_test1", null, new ReplicationGroupMessageIdImpl(2L, 2L)));

    // Run the pipeline
    PCollection<Solace.Record> events =
        pipeline.apply(
            "Read from Solace",
            getDefaultRead()
                .withSessionServiceFactory(fakeSessionServiceFactory)
                .withDeduplicateRecords(true));
    // Assert results
    PAssert.that(events).containsInAnyOrder(expected);
    pipeline.run();
  }

  @Test
  public void testReadWithCoderAndParseFnAndTimestampFn() {
    // Broker that creates input data
    MockSessionService mockClientService =
        new MockSessionService(
            index -> {
              List<BytesXMLMessage> messages =
                  ImmutableList.of(
                      SolaceDataUtils.getBytesXmlMessage("payload_test0", "450"),
                      SolaceDataUtils.getBytesXmlMessage("payload_test1", "451"),
                      SolaceDataUtils.getBytesXmlMessage("payload_test2", "452"));
              return getOrNull(index, messages);
            },
            3);
    SessionServiceFactory fakeSessionServiceFactory =
        new MockSessionServiceFactory(mockClientService);

    // Expected data
    List<SimpleRecord> expected = new ArrayList<>();
    expected.add(new SimpleRecord("payload_test0", "450"));
    expected.add(new SimpleRecord("payload_test1", "451"));
    expected.add(new SimpleRecord("payload_test2", "452"));

    // Run the pipeline
    PCollection<SimpleRecord> events =
        pipeline.apply(
            "Read from Solace",
            SolaceIO.read(
                    TypeDescriptor.of(SimpleRecord.class),
                    input ->
                        new SimpleRecord(
                            new String(input.getBytes(), StandardCharsets.UTF_8),
                            input.getApplicationMessageId()),
                    input -> Instant.ofEpochMilli(1708100477061L))
                .from(Solace.Queue.fromName("queue"))
                .withSempClientFactory(MockSempClientFactory.getDefaultMock())
                .withSessionServiceFactory(fakeSessionServiceFactory)
                .withMaxNumConnections(1));

    // Assert results
    PAssert.that(events).containsInAnyOrder(expected);
    pipeline.run();
  }

  @Test
  public void testNoQueueAndTopicSet() {
    Read<Record> spec = SolaceIO.read();
    assertThrows(IllegalStateException.class, () -> spec.validate(pipeline.getOptions()));
  }

  @Test
  public void testSplitsForExclusiveQueue() throws Exception {
    MockSempClient mockSempClient =
        MockSempClient.builder().setIsQueueNonExclusiveFn((q) -> false).build();

    Read<Record> spec =
        SolaceIO.read()
            .from(Solace.Queue.fromName("queue"))
            .withSempClientFactory(new MockSempClientFactory(mockSempClient))
            .withSessionServiceFactory(MockSessionServiceFactory.getDefaultMock());

    int desiredNumSplits = 5;

    UnboundedSolaceSource<Record> initialSource = getSource(spec, pipeline);
    List<UnboundedSolaceSource<Record>> splits =
        initialSource.split(desiredNumSplits, PipelineOptionsFactory.create());
    assertEquals(1, splits.size());
  }

  @Test
  public void testSplitsForNonExclusiveQueueWithMaxNumConnections() throws Exception {
    Read<Record> spec = getDefaultRead().withMaxNumConnections(3);

    int desiredNumSplits = 5;

    UnboundedSolaceSource<Record> initialSource = getSource(spec, pipeline);
    List<UnboundedSolaceSource<Record>> splits =
        initialSource.split(desiredNumSplits, PipelineOptionsFactory.create());
    assertEquals(3, splits.size());
  }

  @Test
  public void testSplitsForNonExclusiveQueueWithMaxNumConnectionsRespectDesired() throws Exception {
    Read<Record> spec = getDefaultRead().withMaxNumConnections(10);
    int desiredNumSplits = 5;

    UnboundedSolaceSource<Record> initialSource = getSource(spec, pipeline);
    List<UnboundedSolaceSource<Record>> splits =
        initialSource.split(desiredNumSplits, PipelineOptionsFactory.create());
    assertEquals(5, splits.size());
  }

  @Test
  public void testCreateQueueForTopic() {
    AtomicInteger createQueueForTopicFnCounter = new AtomicInteger(0);
    MockSempClient mockSempClient =
        MockSempClient.builder()
            .setCreateQueueForTopicFn((q) -> createQueueForTopicFnCounter.incrementAndGet())
            .build();

    Read<Record> spec =
        getDefaultReadForTopic().withSempClientFactory(new MockSempClientFactory(mockSempClient));
    spec.expand(PBegin.in(TestPipeline.create()));
    // check if createQueueForTopic was executed
    assertEquals(1, createQueueForTopicFnCounter.get());
  }

  @Test
  public void testCheckpointMark() throws Exception {
    AtomicInteger countConsumedMessages = new AtomicInteger(0);
    AtomicInteger countAckMessages = new AtomicInteger(0);

    // Broker that creates input data
    MockSessionService mockClientService =
        new MockSessionService(
            index -> {
              List<BytesXMLMessage> messages = new ArrayList<>();
              for (int i = 0; i < 10; i++) {
                messages.add(
                    SolaceDataUtils.getBytesXmlMessage(
                        "payload_test" + i, "45" + i, (num) -> countAckMessages.incrementAndGet()));
              }
              countConsumedMessages.incrementAndGet();
              return getOrNull(index, messages);
            },
            10);

    SessionServiceFactory fakeSessionServiceFactory =
        new MockSessionServiceFactory(mockClientService);
    Read<Record> spec = getDefaultRead().withSessionServiceFactory(fakeSessionServiceFactory);

    UnboundedSolaceSource<Record> initialSource = getSource(spec, pipeline);
    UnboundedReader<Record> reader =
        initialSource.createReader(PipelineOptionsFactory.create(), null);

    // start the reader and move to the first record
    assertTrue(reader.start());

    // consume 3 messages (NB: start already consumed the first message)
    for (int i = 0; i < 3; i++) {
      assertTrue(String.format("Failed at %d-th message", i), reader.advance());
    }

    // check if 4 messages were consumed
    assertEquals(4, countConsumedMessages.get());

    // check if no messages were acknowledged yet
    assertEquals(0, countAckMessages.get());

    // finalize the checkpoint
    reader.getCheckpointMark().finalizeCheckpoint();

    // check if messages were acknowledged
    assertEquals(4, countAckMessages.get());
  }

  @Test
  public void testCheckpointMarkAndFinalizeSeparately() throws Exception {
    AtomicInteger countConsumedMessages = new AtomicInteger(0);
    AtomicInteger countAckMessages = new AtomicInteger(0);

    // Broker that creates input data
    MockSessionService mockClientService =
        new MockSessionService(
            index -> {
              List<BytesXMLMessage> messages = new ArrayList<>();
              for (int i = 0; i < 10; i++) {
                messages.add(
                    SolaceDataUtils.getBytesXmlMessage(
                        "payload_test" + i, "45" + i, (num) -> countAckMessages.incrementAndGet()));
              }
              countConsumedMessages.incrementAndGet();
              return getOrNull(index, messages);
            },
            10);
    SessionServiceFactory fakeSessionServiceFactory =
        new MockSessionServiceFactory(mockClientService);

    Read<Record> spec =
        getDefaultRead()
            .withSessionServiceFactory(fakeSessionServiceFactory)
            .withMaxNumConnections(4);

    UnboundedSolaceSource<Record> initialSource = getSource(spec, pipeline);

    UnboundedReader<Record> reader =
        initialSource.createReader(PipelineOptionsFactory.create(), null);

    // start the reader and move to the first record
    assertTrue(reader.start());

    // consume 3 messages (NB: start already consumed the first message)
    for (int i = 0; i < 3; i++) {
      assertTrue(String.format("Failed at %d-th message", i), reader.advance());
    }

    // create checkpoint but don't finalize yet
    CheckpointMark checkpointMark = reader.getCheckpointMark();

    // consume 2 more messages
    reader.advance();
    reader.advance();

    // check if messages are still not acknowledged
    assertEquals(0, countAckMessages.get());

    // acknowledge from the first checkpoint
    checkpointMark.finalizeCheckpoint();

    // only messages from the first checkpoint are acknowledged
    assertEquals(4, countAckMessages.get());
  }

  @Test
  public void testCheckpointMarkSafety() throws Exception {

    final int messagesToProcess = 100;

    AtomicInteger countConsumedMessages = new AtomicInteger(0);
    AtomicInteger countAckMessages = new AtomicInteger(0);

    // Broker that creates input data
    MockSessionService mockClientService =
        new MockSessionService(
            index -> {
              List<BytesXMLMessage> messages = new ArrayList<>();
              for (int i = 0; i < messagesToProcess; i++) {
                messages.add(
                    SolaceDataUtils.getBytesXmlMessage(
                        "payload_test" + i, "45" + i, (num) -> countAckMessages.incrementAndGet()));
              }
              countConsumedMessages.incrementAndGet();
              return getOrNull(index, messages);
            },
            10);

    SessionServiceFactory fakeSessionServiceFactory =
        new MockSessionServiceFactory(mockClientService);
    Read<Record> spec =
        getDefaultRead()
            .withSessionServiceFactory(fakeSessionServiceFactory)
            .withMaxNumConnections(4);

    UnboundedSolaceSource<Record> initialSource = getSource(spec, pipeline);

    UnboundedReader<Record> reader =
        initialSource.createReader(PipelineOptionsFactory.create(), null);

    // start the reader and move to the first record
    assertTrue(reader.start());

    // consume half the messages (NB: start already consumed the first message)
    for (int i = 0; i < (messagesToProcess / 2) - 1; i++) {
      assertTrue(reader.advance());
    }

    // the messages are still pending in the queue (no ACK yet)
    assertEquals(0, countAckMessages.get());

    // we finalize the checkpoint for the already-processed messages while simultaneously
    // consuming the remainder of messages from the queue
    Thread runner =
        new Thread(
            () -> {
              try {
                for (int i = 0; i < messagesToProcess / 2; i++) {
                  assertTrue(reader.advance());
                }
              } catch (IOException ex) {
                throw new RuntimeException(ex);
              }
            });
    runner.start();
    reader.getCheckpointMark().finalizeCheckpoint();

    // Concurrency issues would cause an exception to be thrown before this method exits,
    // failing the test
    runner.join();
  }

  @Test
  public void testDefaultCoder() {
    Coder<SolaceCheckpointMark> coder =
        new UnboundedSolaceSource<>(null, null, null, 0, false, null, null, null, null)
            .getCheckpointMarkCoder();
    CoderProperties.coderSerializable(coder);
  }

  @Test
  public void testDestinationTopicQueueCreation() {
    String topicName = "some-topic";
    String queueName = "some-queue";
    Topic topic = SolaceIO.topicFromName(topicName);
    Queue queue = SolaceIO.queueFromName(queueName);

    Destination dest = topic;
    assertTrue(dest instanceof Topic);
    assertFalse(dest instanceof Queue);
    assertEquals(topicName, dest.getName());

    dest = queue;
    assertTrue(dest instanceof Queue);
    assertFalse(dest instanceof Topic);
    assertEquals(queueName, dest.getName());

    Record r = SolaceDataUtils.getSolaceRecord("payload_test0", "450");
    dest = SolaceIO.convertToJcsmpDestination(r.getDestination());
    assertTrue(dest instanceof Topic);
    assertFalse(dest instanceof Queue);
  }

  @Test
  public void testTopicEncoding() {
    MockSessionService mockClientService =
        new MockSessionService(
            index -> {
              List<BytesXMLMessage> messages =
                  ImmutableList.of(
                      SolaceDataUtils.getBytesXmlMessage("payload_test0", "450"),
                      SolaceDataUtils.getBytesXmlMessage("payload_test1", "451"),
                      SolaceDataUtils.getBytesXmlMessage("payload_test2", "452"));
              return getOrNull(index, messages);
            },
            3);

    SessionServiceFactory fakeSessionServiceFactory =
        new MockSessionServiceFactory(mockClientService);

    // Run
    PCollection<Solace.Record> events =
        pipeline.apply(
            "Read from Solace",
            getDefaultRead().withSessionServiceFactory(fakeSessionServiceFactory));

    // Run the pipeline
    PCollection<Boolean> destAreTopics =
        events.apply(
            MapElements.into(TypeDescriptors.booleans())
                .via(
                    r -> {
                      Destination dest = SolaceIO.convertToJcsmpDestination(r.getDestination());
                      return dest instanceof Topic;
                    }));

    List<Boolean> expected = ImmutableList.of(true, true, true);

    // Assert results
    PAssert.that(destAreTopics).containsInAnyOrder(expected);
    pipeline.run();
  }
}
