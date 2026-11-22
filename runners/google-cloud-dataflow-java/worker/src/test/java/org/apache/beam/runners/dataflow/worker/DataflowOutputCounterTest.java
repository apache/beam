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
package org.apache.beam.runners.dataflow.worker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.dataflow.worker.counters.CounterName;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.util.ValueInEmptyWindows;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DataflowOutputCounter}. */
@RunWith(JUnit4.class)
public class DataflowOutputCounterTest {
  private static final String OUTPUT_NAME = "test_output";
  private CounterSet counterSet;
  private NameContext nameContext;

  @Before
  public void setUp() {
    counterSet = new CounterSet();
    nameContext = NameContext.create("stage", "original", "system", OUTPUT_NAME);
  }

  private long getElementCount() {
    return (Long)
        counterSet
            .getExistingCounter(
                CounterName.named(DataflowOutputCounter.getElementCounterName(OUTPUT_NAME)))
            .getAggregate();
  }

  @Test
  public void testBatchOutputCounterWithEmptyWindows() throws Exception {
    DataflowOutputCounter batchCounter =
        DataflowOutputCounter.create(OUTPUT_NAME, counterSet, nameContext, false);

    ValueInEmptyWindows<KV<String, String>> shuffleValue =
        new ValueInEmptyWindows<>(KV.of("key", "value"));
    batchCounter.update(shuffleValue);

    assertEquals(1L, getElementCount());
  }

  @Test
  public void testStreamingOutputCounterWithKeyedWorkItem() throws Exception {
    DataflowOutputCounter streamingCounter =
        DataflowOutputCounter.create(OUTPUT_NAME, counterSet, nameContext, true);

    IntervalWindow w1 = new IntervalWindow(new Instant(0), new Instant(10));
    IntervalWindow w2 = new IntervalWindow(new Instant(10), new Instant(20));
    KeyedWorkItem<String, String> kwi = mock(KeyedWorkItem.class);
    WindowedValue<String> element1 = WindowedValues.valueInGlobalWindow("v1");
    WindowedValue<String> element2 =
        WindowedValues.timestampedValueInGlobalWindow("v2", new Instant(5));
    WindowedValue<String> element3 =
        WindowedValues.of("v3", new Instant(5), w1, PaneInfo.NO_FIRING);
    WindowedValue<String> element4 =
        WindowedValues.of("v4", new Instant(5), Arrays.asList(w1, w2), PaneInfo.NO_FIRING);
    WindowedValue<String> element5 = new ValueInEmptyWindows<>("v5");
    doReturn(Arrays.asList(element1, element2, element3, element4, element5))
        .when(kwi)
        .elementWindowsIterable();

    ValueInEmptyWindows<KeyedWorkItem<String, String>> streamingValue =
        new ValueInEmptyWindows<>(kwi);
    streamingCounter.update(streamingValue);

    // 1 + 1 + 1 + 2 + 1 (empty-window fallback) = 6
    assertEquals(6L, getElementCount());
  }

  @Test
  public void testStreamingNonKeyedWorkItemEmptyWindowsFallback() throws Exception {
    DataflowOutputCounter streamingCounter =
        DataflowOutputCounter.create(OUTPUT_NAME, counterSet, nameContext, true);
    streamingCounter.update(new ValueInEmptyWindows<>("not-a-kwi"));
    assertEquals(1L, getElementCount());
  }

  @Test
  public void testStandardWindowedValueCounting() throws Exception {
    DataflowOutputCounter counter =
        DataflowOutputCounter.create(OUTPUT_NAME, counterSet, nameContext, false);

    IntervalWindow w1 = new IntervalWindow(new Instant(0), new Instant(10));
    IntervalWindow w2 = new IntervalWindow(new Instant(10), new Instant(20));
    IntervalWindow w3 = new IntervalWindow(new Instant(20), new Instant(30));

    // All three concrete single-window fast paths.
    counter.update(WindowedValues.valueInGlobalWindow("v1"));
    counter.update(WindowedValues.timestampedValueInGlobalWindow("v2", new Instant(42)));
    counter.update(WindowedValues.of("v3", new Instant(5), w1, PaneInfo.NO_FIRING));
    assertEquals(3L, getElementCount());

    // Multi-window must NOT hit single-window fast path.
    counter.update(
        WindowedValues.of("v4", new Instant(5), Arrays.asList(w1, w2, w3), PaneInfo.NO_FIRING));
    assertEquals(6L, getElementCount());
  }

  @Test
  public void testForeignSingleWindowedValueAndNullRejections() throws Exception {
    DataflowOutputCounter counter =
        DataflowOutputCounter.create(OUTPUT_NAME, counterSet, nameContext, false);

    WindowedValues.SingleWindowedValue notAWindowedValue =
        new WindowedValues.SingleWindowedValue() {
          @Override
          public BoundedWindow getWindow() {
            return GlobalWindow.INSTANCE;
          }
        };
    assertThrows(ClassCastException.class, () -> counter.update(notAWindowedValue));
    assertThrows(NullPointerException.class, () -> counter.update(null));
  }
}
