package org.apache.beam.sdk.metrics;

import org.apache.beam.sdk.lineage.LineageReporter;
import org.apache.beam.sdk.metrics.Metrics.MetricsFlag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;

public class MetricsLineageReporter implements LineageReporter {

  private final Metric metric;

  public MetricsLineageReporter(final Lineage.Type type) {
    if (MetricsFlag.lineageRollupEnabled()) {
      this.metric = Metrics.boundedTrie(
          Lineage.LINEAGE_NAMESPACE,
          type == Lineage.Type.SOURCE ? Lineage.Type.SOURCEV2.toString() : Lineage.Type.SINKV2.toString());
    } else {
      this.metric = Metrics.stringSet(Lineage.LINEAGE_NAMESPACE, type.toString());
    }
  }

  @Override
  public void add(final Iterable<String> rollupSegments) {
    ImmutableList<String> segments = ImmutableList.copyOf(rollupSegments);
    if (MetricsFlag.lineageRollupEnabled()) {
      ((BoundedTrie) this.metric).add(segments);
    } else {
      ((StringSet) this.metric).add(String.join("", segments));
    }
  }

  @Override
  public void add(final String system, final String subtype, final Iterable<String> segments,
                  final String lastSegmentSep) {
    add(Lineage.getFQNParts(system, subtype, segments, lastSegmentSep));
  }
}
