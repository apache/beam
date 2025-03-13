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
package org.apache.beam.sdk.io.gcp.pubsublite.internal;

import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.example;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.internal.CheckedApiException;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.Progress;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.math.DoubleMath;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Spy;

@RunWith(JUnit4.class)
@SuppressWarnings("initialization.fields.uninitialized")
public class PerSubscriptionPartitionSdfTest {

  private static final OffsetByteRange RESTRICTION =
      OffsetByteRange.of(new OffsetRange(1, Long.MAX_VALUE), 0);
  private static final SubscriptionPartition PARTITION =
      SubscriptionPartition.of(example(SubscriptionPath.class), example(Partition.class));

  @Mock SerializableFunction<SubscriptionPartition, InitialOffsetReader> offsetReaderFactory;

  @Mock ManagedFactory<TopicBacklogReader> backlogReaderFactory;
  @Mock TopicBacklogReader backlogReader;

  @Mock
  SerializableBiFunction<TopicBacklogReader, OffsetByteRange, TrackerWithProgress> trackerFactory;

  @Mock SubscriptionPartitionProcessorFactory processorFactory;
  @Mock ManagedFactory<BlockingCommitter> committerFactory;

  @Mock InitialOffsetReader initialOffsetReader;
  @Spy TrackerWithProgress tracker;
  @Mock OutputReceiver<SequencedMessage> output;
  @Mock SubscriptionPartitionProcessor processor;
  @Mock BlockingCommitter committer;

  PerSubscriptionPartitionSdf sdf;

  @Before
  public void setUp() {
    initMocks(this);
    when(offsetReaderFactory.apply(any())).thenReturn(initialOffsetReader);
    when(processorFactory.newProcessor(any(), any(), any())).thenReturn(processor);
    when(trackerFactory.apply(any(), any())).thenReturn(tracker);
    when(committerFactory.create(any())).thenReturn(committer);
    when(tracker.currentRestriction()).thenReturn(RESTRICTION);
    when(backlogReaderFactory.create(any())).thenReturn(backlogReader);
    sdf =
        new PerSubscriptionPartitionSdf(
            backlogReaderFactory,
            committerFactory,
            offsetReaderFactory,
            trackerFactory,
            processorFactory);
  }

  @Test
  public void getInitialRestrictionReadSuccess() {
    when(initialOffsetReader.read()).thenReturn(example(Offset.class));
    OffsetByteRange range = sdf.getInitialRestriction(PARTITION);
    assertEquals(example(Offset.class).value(), range.getRange().getFrom());
    assertEquals(Long.MAX_VALUE, range.getRange().getTo());
    assertEquals(0, range.getByteCount());
    verify(offsetReaderFactory).apply(PARTITION);
  }

  @Test
  public void getInitialRestrictionReadFailure() {
    when(initialOffsetReader.read()).thenThrow(new CheckedApiException(Code.INTERNAL).underlying);
    assertThrows(ApiException.class, () -> sdf.getInitialRestriction(PARTITION));
  }

  @Test
  public void newTrackerCallsFactory() {
    assertSame(tracker, sdf.newTracker(PARTITION, RESTRICTION));
    verify(trackerFactory).apply(backlogReader, RESTRICTION);
  }

  @Test
  public void tearDownClosesBacklogReaderFactory() throws Exception {
    sdf.teardown();
    verify(backlogReaderFactory).close();
  }

  @Test
  @SuppressWarnings("argument")
  public void process() throws Exception {
    when(processor.run()).thenReturn(ProcessContinuation.resume());
    when(processorFactory.newProcessor(any(), any(), any()))
        .thenAnswer(
            args -> {
              @Nonnull
              RestrictionTracker<OffsetRange, OffsetByteProgress> wrapped = args.getArgument(1);
              when(tracker.tryClaim(any())).thenReturn(true).thenReturn(false);
              assertTrue(wrapped.tryClaim(OffsetByteProgress.of(example(Offset.class), 123)));
              assertFalse(wrapped.tryClaim(OffsetByteProgress.of(Offset.of(333333), 123)));
              return processor;
            });
    doReturn(Optional.of(example(Offset.class))).when(processor).lastClaimed();
    assertEquals(ProcessContinuation.resume(), sdf.processElement(tracker, PARTITION, output));
    verify(processorFactory).newProcessor(eq(PARTITION), any(), eq(output));
    InOrder order = inOrder(processor);
    order.verify(processor).run();
    order.verify(processor).lastClaimed();
    InOrder order2 = inOrder(committerFactory, committer);
    order2.verify(committerFactory).create(PARTITION);
    order2.verify(committer).commitOffset(Offset.of(example(Offset.class).value() + 1));
  }

  private static final class NoopManagedFactory<T extends AutoCloseable>
      implements ManagedFactory<T> {

    @Override
    public T create(SubscriptionPartition subscriptionPartition) {
      return null;
    }

    @Override
    public void close() {}
  }

  @Test
  @SuppressWarnings("return")
  public void dofnIsSerializable() throws Exception {
    ObjectOutputStream output = new ObjectOutputStream(new ByteArrayOutputStream());
    output.writeObject(
        new PerSubscriptionPartitionSdf(
            new NoopManagedFactory<>(),
            new NoopManagedFactory<>(),
            (x) -> null,
            (x, y) -> null,
            (x, y, z) -> null));
  }

  @Test
  public void getProgressUnboundedRangeDelegates() {
    Progress progress = Progress.from(0, 0.2);
    when(tracker.getProgress()).thenReturn(progress);
    assertTrue(
        DoubleMath.fuzzyEquals(
            progress.getWorkRemaining(), sdf.getSize(PARTITION, RESTRICTION), .0001));
    verify(tracker).getProgress();
  }

  @Test
  public void getProgressBoundedReturnsBytes() {
    assertTrue(
        DoubleMath.fuzzyEquals(
            123.0,
            sdf.getSize(PARTITION, OffsetByteRange.of(new OffsetRange(87, 8000), 123)),
            .0001));
    verifyNoInteractions(tracker);
  }
}
