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

import com.google.api.services.dataflow.model.BoundedTrie;
import com.google.api.services.dataflow.model.BoundedTrieNode;
import com.google.api.services.dataflow.model.CounterMetadata;
import com.google.api.services.dataflow.model.CounterStructuredName;
import com.google.api.services.dataflow.model.CounterStructuredNameAndMetadata;
import com.google.api.services.dataflow.model.CounterUpdate;
import com.google.api.services.dataflow.model.DistributionUpdate;
import com.google.api.services.dataflow.model.IntegerGauge;
import com.google.api.services.dataflow.model.StringList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.model.pipeline.v1.MetricsApi;
import org.apache.beam.runners.core.metrics.BoundedTrieData;
import org.apache.beam.runners.core.metrics.DistributionData;
import org.apache.beam.runners.core.metrics.StringSetData;
import org.apache.beam.sdk.metrics.MetricKey;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;

/** Convertor from Metrics to {@link CounterUpdate} protos. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
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
    SUM("SUM"),
    LATEST_VALUE("LATEST_VALUE"),
    SET("SET");

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

  public static CounterUpdate fromGauge(
      MetricKey key, long update, org.joda.time.Instant timestamp) {
    CounterStructuredNameAndMetadata name = structuredNameAndMetadata(key, Kind.LATEST_VALUE);

    IntegerGauge integerGaugeProto = new IntegerGauge();
    integerGaugeProto.setValue(longToSplitInt(update)).setTimestamp(timestamp.toString());

    return new CounterUpdate()
        .setStructuredNameAndMetadata(name)
        .setCumulative(false)
        .setIntegerGauge(integerGaugeProto);
  }

  public static CounterUpdate fromStringSet(
      MetricKey key, boolean isCumulative, StringSetData stringSetData) {
    CounterStructuredNameAndMetadata name = structuredNameAndMetadata(key, Kind.SET);

    StringList stringList = new StringList();
    stringList.setElements(new ArrayList<>(stringSetData.stringSet()));

    return new CounterUpdate()
        .setStructuredNameAndMetadata(name)
        .setCumulative(isCumulative)
        .setStringList(stringList);
  }

  public static CounterUpdate fromBoundedTrie(
      MetricKey key, boolean isCumulative, BoundedTrieData boundedTrieData) {
    // BoundedTrie uses SET kind metric aggregation which tracks unique strings as a trie.
    CounterStructuredNameAndMetadata name = structuredNameAndMetadata(key, Kind.SET);
    BoundedTrie counterUpdateTrie = getBoundedTrie(boundedTrieData.toProto());

    return new CounterUpdate()
        .setStructuredNameAndMetadata(name)
        .setCumulative(isCumulative)
        .setBoundedTrie(counterUpdateTrie);
  }

  /**
   * Converts from org.apache.beam.model.pipeline.v1.BoundedTrie to
   * com.google.api.services.dataflow.model.BoundedTrie. This is because even though Dataflow
   * CounterUpdate uses org.apache.beam.model.pipeline.v1.BoundedTrieNode in it's definition when
   * the google-api client is generated the package is renamed.
   *
   * @param trie org.apache.beam.model.pipeline.v1.BoundedTrie to be converted
   * @return converted com.google.api.services.dataflow.model.BoundedTrie.
   */
  @VisibleForTesting
  static BoundedTrie getBoundedTrie(MetricsApi.BoundedTrie trie) {
    BoundedTrie counterUpdateTrie = new BoundedTrie();
    counterUpdateTrie.setBound(trie.getBound());
    counterUpdateTrie.setSingleton(
        trie.getSingletonList().isEmpty() ? null : trie.getSingletonList());
    counterUpdateTrie.setRoot(trie.hasRoot() ? getBoundedTrieNode(trie.getRoot()) : null);
    return counterUpdateTrie;
  }

  private static BoundedTrieNode getBoundedTrieNode(MetricsApi.BoundedTrieNode node) {
    BoundedTrieNode boundedTrieNode = new BoundedTrieNode();
    boundedTrieNode.setTruncated(node.getTruncated());
    Map<String, BoundedTrieNode> children = new HashMap<>();
    node.getChildrenMap().forEach((key, value) -> children.put(key, getBoundedTrieNode(value)));
    boundedTrieNode.setChildren(children);
    return boundedTrieNode;
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
