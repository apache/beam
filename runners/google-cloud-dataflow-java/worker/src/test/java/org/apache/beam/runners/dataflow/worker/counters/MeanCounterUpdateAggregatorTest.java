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
package org.apache.beam.runners.dataflow.worker.counters;

import static org.apache.beam.runners.dataflow.worker.counters.DataflowCounterUpdateExtractor.longToSplitInt;
import static org.apache.beam.runners.dataflow.worker.counters.DataflowCounterUpdateExtractor.splitIntToLong;
import static org.junit.Assert.assertEquals;

import com.google.api.services.dataflow.model.CounterMetadata;
import com.google.api.services.dataflow.model.CounterStructuredNameAndMetadata;
import com.google.api.services.dataflow.model.CounterUpdate;
import com.google.api.services.dataflow.model.IntegerMean;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.dataflow.worker.MetricsToCounterUpdateConverter.Kind;
import org.junit.Before;
import org.junit.Test;

public class MeanCounterUpdateAggregatorTest {

  private List<CounterUpdate> counterUpdates;
  private MeanCounterUpdateAggregator aggregator;

  @Before
  public void setUp() {
    counterUpdates = new ArrayList<>();
    aggregator = new MeanCounterUpdateAggregator();
    for (int i = 0; i < 10; i++) {
      counterUpdates.add(
          new CounterUpdate()
              .setStructuredNameAndMetadata(
                  new CounterStructuredNameAndMetadata()
                      .setMetadata(new CounterMetadata().setKind(Kind.MEAN.toString())))
              .setIntegerMean(
                  new IntegerMean().setSum(longToSplitInt((long) i)).setCount(longToSplitInt(1L))));
    }
  }

  @Test
  public void testAggregate() {
    CounterUpdate combined = aggregator.aggregate(counterUpdates);
    assertEquals(45L, splitIntToLong(combined.getIntegerMean().getSum()));
    assertEquals(10L, splitIntToLong(combined.getIntegerMean().getCount()));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testAggregateWithNullIntegerMean() {
    counterUpdates.get(0).setIntegerMean(null);
    aggregator.aggregate(counterUpdates);
  }
}
