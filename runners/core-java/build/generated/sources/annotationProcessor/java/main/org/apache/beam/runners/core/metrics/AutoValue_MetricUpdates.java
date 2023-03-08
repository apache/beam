package org.apache.beam.runners.core.metrics;

import javax.annotation.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_MetricUpdates extends MetricUpdates {

  private final Iterable<MetricUpdates.MetricUpdate<Long>> counterUpdates;

  private final Iterable<MetricUpdates.MetricUpdate<DistributionData>> distributionUpdates;

  private final Iterable<MetricUpdates.MetricUpdate<GaugeData>> gaugeUpdates;

  AutoValue_MetricUpdates(
      Iterable<MetricUpdates.MetricUpdate<Long>> counterUpdates,
      Iterable<MetricUpdates.MetricUpdate<DistributionData>> distributionUpdates,
      Iterable<MetricUpdates.MetricUpdate<GaugeData>> gaugeUpdates) {
    if (counterUpdates == null) {
      throw new NullPointerException("Null counterUpdates");
    }
    this.counterUpdates = counterUpdates;
    if (distributionUpdates == null) {
      throw new NullPointerException("Null distributionUpdates");
    }
    this.distributionUpdates = distributionUpdates;
    if (gaugeUpdates == null) {
      throw new NullPointerException("Null gaugeUpdates");
    }
    this.gaugeUpdates = gaugeUpdates;
  }

  @Override
  public Iterable<MetricUpdates.MetricUpdate<Long>> counterUpdates() {
    return counterUpdates;
  }

  @Override
  public Iterable<MetricUpdates.MetricUpdate<DistributionData>> distributionUpdates() {
    return distributionUpdates;
  }

  @Override
  public Iterable<MetricUpdates.MetricUpdate<GaugeData>> gaugeUpdates() {
    return gaugeUpdates;
  }

  @Override
  public String toString() {
    return "MetricUpdates{"
        + "counterUpdates=" + counterUpdates + ", "
        + "distributionUpdates=" + distributionUpdates + ", "
        + "gaugeUpdates=" + gaugeUpdates
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof MetricUpdates) {
      MetricUpdates that = (MetricUpdates) o;
      return this.counterUpdates.equals(that.counterUpdates())
          && this.distributionUpdates.equals(that.distributionUpdates())
          && this.gaugeUpdates.equals(that.gaugeUpdates());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= counterUpdates.hashCode();
    h$ *= 1000003;
    h$ ^= distributionUpdates.hashCode();
    h$ *= 1000003;
    h$ ^= gaugeUpdates.hashCode();
    return h$;
  }

}
