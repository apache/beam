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
import java.util.List;

public class SumCounterUpdateAggregator implements CounterUpdateAggregator {

  @Override
  public CounterUpdate aggregate(List<CounterUpdate> counterUpdates) {
    if (counterUpdates == null || counterUpdates.isEmpty()) {
      return null;
    }
    if (counterUpdates.stream().anyMatch(c -> c.getInteger() == null)) {
      throw new UnsupportedOperationException(
          "Aggregating SUM counter updates over non-integer type is not implemented.");
    }

    CounterUpdate initial = counterUpdates.remove(0);
    return counterUpdates.stream()
        .reduce(
            initial,
            (first, second) ->
                first.setInteger(
                    longToSplitInt(
                        splitIntToLong(first.getInteger()) + splitIntToLong(second.getInteger()))));
  }
}
