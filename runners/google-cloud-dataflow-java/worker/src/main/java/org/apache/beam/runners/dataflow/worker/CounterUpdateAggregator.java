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

import com.google.api.services.dataflow.model.CounterUpdate;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.runners.dataflow.worker.MetricsToCounterUpdateConverter.Kind;

/**
 * CounterUpdateAggregator performs aggregation over a list of CounterUpdate and return combined
 * result.
 */
interface CounterUpdateAggregator {

  /**
   * Implementation of aggregate function should provide logic to take the list of CounterUpdates
   * and return single combined CounterUpdate object. Reporting the aggregated result to Dataflow
   * should have same effect as reporting the elements in list individually to Dataflow.
   *
   * @param counterUpdates CounterUpdates to aggregate.
   * @return Aggregated CounterUpdate.
   */
  CounterUpdate aggregate(List<CounterUpdate> counterUpdates);

  /**
   * CounterUpdate {@link
   * org.apache.beam.runners.dataflow.worker.MetricsToCounterUpdateConverter.Kind kind}
   */
  Kind getKind();

  /**
   * Check whether the aggregator is able to perform aggregation on the kind of CounterUpdate.
   *
   * @param counterUpdate the counterUpdate object to check.
   * @return true if the aggregator can perform aggregation over these type of CounterUpdate.
   */
  default boolean isCorrespondingCounterUpdate(CounterUpdate counterUpdate) {
    return (counterUpdate.getStructuredNameAndMetadata() != null
            && counterUpdate.getStructuredNameAndMetadata().getMetadata() != null
            && getKind()
                .toString()
                .equals(counterUpdate.getStructuredNameAndMetadata().getMetadata().getKind()))
        || (counterUpdate.getNameAndKind() != null
            && getKind().toString().equals(counterUpdate.getNameAndKind().getKind()));
  }

  static List<CounterUpdateAggregator> getAllAvailableCounterUpdateAggregators() {
    return Arrays.asList(
        new SumCounterUpdateAggregator(),
        new MeanCounterUpdateAggregator(),
        new DistributionCounterUpdateAggregator());
  }
}
