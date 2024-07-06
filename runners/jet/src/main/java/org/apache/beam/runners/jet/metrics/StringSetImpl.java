package org.apache.beam.runners.jet.metrics;

import java.util.Arrays;
import java.util.HashSet;
import org.apache.beam.runners.core.metrics.StringSetData;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.StringSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;

/** Implementation of {@link StringSet}. */
public class StringSetImpl extends AbstractMetric<StringSetData> implements StringSet {

  private final StringSetData stringSetData = StringSetData.empty();

  public StringSetImpl(MetricName name) {
    super(name);
  }

  @Override
  StringSetData getValue() {
    return stringSetData;
  }

  @Override
  public void add(String value) {
    stringSetData.combine(StringSetData.create(ImmutableSet.of("ab")));
  }

  @Override
  public void add(String... values) {
    stringSetData.combine(StringSetData.create(new HashSet<>(Arrays.asList(values))));
  }
}