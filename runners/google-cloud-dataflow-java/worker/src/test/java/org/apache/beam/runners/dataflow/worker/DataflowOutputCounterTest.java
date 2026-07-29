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
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.dataflow.worker.counters.CounterName;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.util.ValueInEmptyWindows;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
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

  @Test
  public void testBatchOutputCounterWithEmptyWindows() throws Exception {
    DataflowOutputCounter batchCounter =
        DataflowOutputCounter.create(OUTPUT_NAME, counterSet, nameContext, false);

    ValueInEmptyWindows<KV<String, String>> shuffleValue =
        new ValueInEmptyWindows<>(KV.of("key", "value"));
    batchCounter.update(shuffleValue);

    long elementCount =
        (Long)
            counterSet
                .getExistingCounter(
                    CounterName.named(DataflowOutputCounter.getElementCounterName(OUTPUT_NAME)))
                .getAggregate();
    assertEquals(1L, elementCount);
  }

  @Test
  public void testStreamingOutputCounterWithKeyedWorkItem() throws Exception {
    DataflowOutputCounter streamingCounter =
        DataflowOutputCounter.create(OUTPUT_NAME, counterSet, nameContext, true);

    KeyedWorkItem<String, String> kwi = mock(KeyedWorkItem.class);
    WindowedValue<String> element1 = WindowedValues.valueInGlobalWindow("v1");
    WindowedValue<String> element2 = WindowedValues.valueInGlobalWindow("v2");
    doReturn(Arrays.asList(element1, element2)).when(kwi).elementWindowsIterable();

    ValueInEmptyWindows<KeyedWorkItem<String, String>> streamingValue =
        new ValueInEmptyWindows<>(kwi);
    streamingCounter.update(streamingValue);

    long elementCount =
        (Long)
            counterSet
                .getExistingCounter(
                    CounterName.named(DataflowOutputCounter.getElementCounterName(OUTPUT_NAME)))
                .getAggregate();
    assertEquals(2L, elementCount);
  }

  @Test
  public void testStandardWindowedValueCounting() throws Exception {
    DataflowOutputCounter counter =
        DataflowOutputCounter.create(OUTPUT_NAME, counterSet, nameContext, false);

    WindowedValue<String> standardValue = WindowedValues.valueInGlobalWindow("v1");
    counter.update(standardValue);

    long elementCount =
        (Long)
            counterSet
                .getExistingCounter(
                    CounterName.named(DataflowOutputCounter.getElementCounterName(OUTPUT_NAME)))
                .getAggregate();
    assertEquals(1L, elementCount);
  }
}
