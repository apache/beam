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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.cloud.pubsublite.Offset;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.internal.CheckedApiException;
import com.google.cloud.pubsublite.proto.SequencedMessage;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Spy;

@RunWith(JUnit4.class)
@SuppressWarnings("initialization.fields.uninitialized")
public class PerPartitionSdfTest {
  private final Duration MAX_SLEEP_TIME = Duration.standardMinutes(10).plus(Duration.millis(10));
  private final Duration SLEEP_CYCLE_TIME = Duration.millis(50);
  private final OffsetRange RESTRICTION = new OffsetRange(1, Long.MAX_VALUE);

  @Mock SerializableFunction<Partition, InitialOffsetReader> offsetReaderFactory;

  @Mock
  SerializableBiFunction<
          Partition, OffsetRange, RestrictionTracker<OffsetRange, OffsetByteProgress>>
      trackerFactory;

  @Mock PartitionProcessorFactory processorFactory;

  @Mock InitialOffsetReader initialOffsetReader;
  @Spy RestrictionTracker<OffsetRange, OffsetByteProgress> tracker;
  @Mock OutputReceiver<SequencedMessage> output;
  @Mock PartitionProcessor processor;

  PerPartitionSdf sdf;

  @Before
  public void setUp() {
    initMocks(this);
    when(offsetReaderFactory.apply(any())).thenReturn(initialOffsetReader);
    when(processorFactory.newProcessor(any(), any(), any())).thenReturn(processor);
    when(trackerFactory.apply(any(), any())).thenReturn(tracker);
    when(tracker.currentRestriction()).thenReturn(RESTRICTION);
    sdf =
        new PerPartitionSdf(MAX_SLEEP_TIME, offsetReaderFactory, trackerFactory, processorFactory);
  }

  @Test
  public void getInitialRestrictionReadSuccess() {
    when(initialOffsetReader.read()).thenReturn(example(Offset.class));
    OffsetRange range = sdf.getInitialRestriction(example(Partition.class));
    assertEquals(example(Offset.class).value(), range.getFrom());
    assertEquals(Long.MAX_VALUE, range.getTo());
    verify(offsetReaderFactory).apply(example(Partition.class));
  }

  @Test
  public void getInitialRestrictionReadFailure() {
    when(initialOffsetReader.read()).thenThrow(new CheckedApiException(Code.INTERNAL).underlying);
    assertThrows(ApiException.class, () -> sdf.getInitialRestriction(example(Partition.class)));
  }

  @Test
  public void newTrackerCallsFactory() {
    assertSame(tracker, sdf.newTracker(example(Partition.class), RESTRICTION));
    verify(trackerFactory).apply(example(Partition.class), RESTRICTION);
  }

  @Test
  public void processUsesProcessor() throws Exception {
    when(processor.waitForCompletion(MAX_SLEEP_TIME)).thenReturn(ProcessContinuation.resume());
    assertEquals(
        ProcessContinuation.resume(),
        sdf.processElement(tracker, example(Partition.class), output));
    verify(processorFactory).newProcessor(example(Partition.class), tracker, output);
    InOrder order = inOrder(processor);
    order.verify(processor).start();
    order.verify(processor).waitForCompletion(MAX_SLEEP_TIME);
    order.verify(processor).close();
  }

  @Test
  @SuppressWarnings("return.type.incompatible")
  public void dofnIsSerializable() throws Exception {
    ObjectOutputStream output = new ObjectOutputStream(new ByteArrayOutputStream());
    output.writeObject(
        new PerPartitionSdf(MAX_SLEEP_TIME, x -> null, (x, y) -> null, (x, y, z) -> null));
  }
}
