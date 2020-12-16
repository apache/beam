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
package org.apache.beam.sdk.io.gcp.pubsublite;

import static com.google.cloud.pubsublite.internal.testing.UnitTestExamples.example;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.internal.CheckedApiException;
import com.google.cloud.pubsublite.internal.testing.FakeApiService;
import com.google.cloud.pubsublite.internal.wire.Committer;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nonnull;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn.BundleFinalizer;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.stubbing.Answer;

@RunWith(JUnit4.class)
@SuppressWarnings("initialization.fields.uninitialized")
public class PerSubscriptionPartitionSdfTest {
  private static final Duration MAX_SLEEP_TIME =
      Duration.standardMinutes(10).plus(Duration.millis(10));
  private static final OffsetRange RESTRICTION = new OffsetRange(1, Long.MAX_VALUE);
  private static final SubscriptionPartition PARTITION =
      SubscriptionPartition.of(example(SubscriptionPath.class), example(Partition.class));

  @Mock SerializableFunction<SubscriptionPartition, InitialOffsetReader> offsetReaderFactory;

  @Mock
  SerializableBiFunction<
          SubscriptionPartition, OffsetRange, RestrictionTracker<OffsetRange, OffsetByteProgress>>
      trackerFactory;

  @Mock SubscriptionPartitionProcessorFactory processorFactory;
  @Mock SerializableFunction<SubscriptionPartition, Committer> committerFactory;

  @Mock InitialOffsetReader initialOffsetReader;
  @Spy RestrictionTracker<OffsetRange, OffsetByteProgress> tracker;
  @Mock OutputReceiver<SequencedMessage> output;
  @Mock BundleFinalizer finalizer;
  @Mock SubscriptionPartitionProcessor processor;

  abstract static class FakeCommitter extends FakeApiService implements Committer {}

  @Spy FakeCommitter committer;

  PerSubscriptionPartitionSdf sdf;

  @Before
  public void setUp() {
    initMocks(this);
    when(offsetReaderFactory.apply(any())).thenReturn(initialOffsetReader);
    when(processorFactory.newProcessor(any(), any(), any())).thenReturn(processor);
    when(trackerFactory.apply(any(), any())).thenReturn(tracker);
    when(committerFactory.apply(any())).thenReturn(committer);
    when(tracker.currentRestriction()).thenReturn(RESTRICTION);
    sdf =
        new PerSubscriptionPartitionSdf(
            MAX_SLEEP_TIME,
            offsetReaderFactory,
            trackerFactory,
            processorFactory,
            committerFactory);
  }

  @Test
  public void getInitialRestrictionReadSuccess() {
    when(initialOffsetReader.read()).thenReturn(example(Offset.class));
    OffsetRange range = sdf.getInitialRestriction(PARTITION);
    assertEquals(example(Offset.class).value(), range.getFrom());
    assertEquals(Long.MAX_VALUE, range.getTo());
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
    verify(trackerFactory).apply(PARTITION, RESTRICTION);
  }

  @Test
  @SuppressWarnings("argument.type.incompatible")
  public void process() throws Exception {
    when(processor.waitForCompletion(MAX_SLEEP_TIME)).thenReturn(ProcessContinuation.resume());
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
    AtomicReference<BundleFinalizer.Callback> callbackRef = new AtomicReference<>(null);
    doAnswer(
            (Answer<Void>)
                args -> {
                  callbackRef.set(args.getArgument(1));
                  return null;
                })
        .when(finalizer)
        .afterBundleCommit(any(), any());
    doReturn(Optional.of(example(Offset.class))).when(processor).lastClaimed();
    assertEquals(
        ProcessContinuation.resume(), sdf.processElement(tracker, PARTITION, output, finalizer));
    verify(processorFactory).newProcessor(eq(PARTITION), any(), eq(output));
    InOrder order = inOrder(processor);
    order.verify(processor).start();
    order.verify(processor).waitForCompletion(MAX_SLEEP_TIME);
    order.verify(processor).lastClaimed();
    order.verify(processor).close();
    verify(finalizer).afterBundleCommit(eq(Instant.ofEpochMilli(Long.MAX_VALUE)), any());
    assertTrue(callbackRef.get() != null);
    when(committer.commitOffset(any())).thenReturn(ApiFutures.immediateFuture(null));
    callbackRef.get().onBundleSuccess();
    InOrder order2 = inOrder(committerFactory, committer);
    order2.verify(committer).startAsync();
    order2.verify(committer).awaitRunning();
    order2.verify(committer).commitOffset(Offset.of(example(Offset.class).value() + 1));
    order2.verify(committer).stopAsync();
    order2.verify(committer).awaitTerminated();
  }

  @Test
  @SuppressWarnings("return.type.incompatible")
  public void dofnIsSerializable() throws Exception {
    ObjectOutputStream output = new ObjectOutputStream(new ByteArrayOutputStream());
    output.writeObject(
        new PerSubscriptionPartitionSdf(
            MAX_SLEEP_TIME, x -> null, (x, y) -> null, (x, y, z) -> null, (x) -> null));
  }
}
