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

import static org.apache.beam.runners.dataflow.worker.counters.DataflowCounterUpdateExtractor.longToSplitInt;

import com.google.api.services.dataflow.model.CounterMetadata;
import com.google.api.services.dataflow.model.CounterStructuredName;
import com.google.api.services.dataflow.model.CounterStructuredNameAndMetadata;
import com.google.api.services.dataflow.model.CounterUpdate;
import com.google.api.services.dataflow.model.DistributionUpdate;
import org.apache.beam.runners.core.metrics.DistributionData;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;

/** Convertor from Metrics to {@link CounterUpdate} protos. */
public class MetricsToCounterUpdateConverter {

  private MetricsToCounterUpdateConverter() {}

  /** Well-defined {@code origin} strings for use in {@link CounterUpdate} protos. */
  public enum Origin {
    USER("USER");

    private final String origin;

    Origin(String origin) {
      this.origin = origin;
    }

    @Override
    public String toString() {
      return origin;
    }
  }

  /** Well-defined {@code kind} strings for use in {@link CounterUpdate} protos. */
  public enum Kind {
    DISTRIBUTION("DISTRIBUTION"),
    MEAN("MEAN"),
    SUM("SUM");

    private final String kind;

    Kind(String kind) {
      this.kind = kind;
    }

    @Override
    public String toString() {
      return kind;
    }
  }

  public static CounterUpdate fromCounter(MetricKey key, boolean isCumulative, long update) {
    CounterStructuredNameAndMetadata name = structuredNameAndMetadata(key, Kind.SUM);

    return new CounterUpdate()
        .setStructuredNameAndMetadata(name)
        .setCumulative(isCumulative)
        .setInteger(longToSplitInt(update));
  }

  public static CounterUpdate fromDistribution(
      MetricKey key, boolean isCumulative, DistributionData update) {
    CounterStructuredNameAndMetadata name = structuredNameAndMetadata(key, Kind.DISTRIBUTION);

    DistributionUpdate distributionUpdateProto = new DistributionUpdate();
    distributionUpdateProto
        .setMin(longToSplitInt(update.min()))
        .setMax(longToSplitInt(update.max()))
        .setCount(longToSplitInt(update.count()))
        .setSum(longToSplitInt(update.sum()));
    return new CounterUpdate()
        .setStructuredNameAndMetadata(name)
        .setCumulative(isCumulative)
        .setDistribution(distributionUpdateProto);
  }

  private static CounterStructuredNameAndMetadata structuredNameAndMetadata(
      MetricKey metricKey, Kind kind) {
    MetricName metricName = metricKey.metricName();
    CounterStructuredNameAndMetadata name = new CounterStructuredNameAndMetadata();
    name.setMetadata(new CounterMetadata().setKind(kind.toString()));
    name.setName(
        new CounterStructuredName()
            .setName(metricName.getName())
            .setOriginalStepName(metricKey.stepName())
            .setOrigin(Origin.USER.toString())
            .setOriginNamespace(metricName.getNamespace()));
    return name;
  }
}
