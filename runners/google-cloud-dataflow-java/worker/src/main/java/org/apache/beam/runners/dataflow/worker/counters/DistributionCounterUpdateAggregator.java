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

import com.google.api.services.dataflow.model.CounterUpdate;
import com.google.api.services.dataflow.model.DistributionUpdate;
import java.util.List;

public class DistributionCounterUpdateAggregator implements CounterUpdateAggregator {

  @Override
  public CounterUpdate aggregate(List<CounterUpdate> counterUpdates) {

    if (counterUpdates == null || counterUpdates.isEmpty()) {
      return null;
    }
    if (counterUpdates.stream().anyMatch(c -> c.getDistribution() == null)) {
      throw new UnsupportedOperationException(
          "Aggregating DISTRIBUTION counter updates over non-distribution type is not implemented.");
    }
    CounterUpdate initial = counterUpdates.remove(0);
    return counterUpdates.stream()
        .reduce(
            initial,
            (first, second) ->
                first.setDistribution(
                    new DistributionUpdate()
                        .setCount(
                            longToSplitInt(
                                splitIntToLong(first.getDistribution().getCount())
                                    + splitIntToLong(second.getDistribution().getCount())))
                        .setMax(
                            longToSplitInt(
                                Math.max(
                                    splitIntToLong(first.getDistribution().getMax()),
                                    splitIntToLong(second.getDistribution().getMax()))))
                        .setMin(
                            longToSplitInt(
                                Math.min(
                                    splitIntToLong(first.getDistribution().getMin()),
                                    splitIntToLong(second.getDistribution().getMin()))))
                        .setSum(
                            longToSplitInt(
                                splitIntToLong(first.getDistribution().getSum())
                                    + splitIntToLong(second.getDistribution().getSum())))));
  }
}
