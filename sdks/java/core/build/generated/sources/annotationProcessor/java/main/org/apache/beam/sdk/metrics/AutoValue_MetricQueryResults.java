package org.apache.beam.sdk.metrics;

import javax.annotation.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_MetricQueryResults extends MetricQueryResults {

  private final Iterable<MetricResult<Long>> counters;

  private final Iterable<MetricResult<DistributionResult>> distributions;

  private final Iterable<MetricResult<GaugeResult>> gauges;

  AutoValue_MetricQueryResults(
      Iterable<MetricResult<Long>> counters,
      Iterable<MetricResult<DistributionResult>> distributions,
      Iterable<MetricResult<GaugeResult>> gauges) {
    if (counters == null) {
      throw new NullPointerException("Null counters");
    }
    this.counters = counters;
    if (distributions == null) {
      throw new NullPointerException("Null distributions");
    }
    this.distributions = distributions;
    if (gauges == null) {
      throw new NullPointerException("Null gauges");
    }
    this.gauges = gauges;
  }

  @Override
  public Iterable<MetricResult<Long>> getCounters() {
    return counters;
  }

  @Override
  public Iterable<MetricResult<DistributionResult>> getDistributions() {
    return distributions;
  }

  @Override
  public Iterable<MetricResult<GaugeResult>> getGauges() {
    return gauges;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof MetricQueryResults) {
      MetricQueryResults that = (MetricQueryResults) o;
      return this.counters.equals(that.getCounters())
          && this.distributions.equals(that.getDistributions())
          && this.gauges.equals(that.getGauges());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= counters.hashCode();
    h$ *= 1000003;
    h$ ^= distributions.hashCode();
    h$ *= 1000003;
    h$ ^= gauges.hashCode();
    return h$;
  }

}
