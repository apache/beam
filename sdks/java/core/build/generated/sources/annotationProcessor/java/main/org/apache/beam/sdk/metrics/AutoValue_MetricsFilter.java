package org.apache.beam.sdk.metrics;

import javax.annotation.Generated;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_MetricsFilter extends MetricsFilter {

  private final ImmutableSet<String> immutableSteps;

  private final ImmutableSet<MetricNameFilter> immutableNames;

  private AutoValue_MetricsFilter(
      ImmutableSet<String> immutableSteps,
      ImmutableSet<MetricNameFilter> immutableNames) {
    this.immutableSteps = immutableSteps;
    this.immutableNames = immutableNames;
  }

  @Override
  protected ImmutableSet<String> immutableSteps() {
    return immutableSteps;
  }

  @Override
  protected ImmutableSet<MetricNameFilter> immutableNames() {
    return immutableNames;
  }

  @Override
  public String toString() {
    return "MetricsFilter{"
        + "immutableSteps=" + immutableSteps + ", "
        + "immutableNames=" + immutableNames
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof MetricsFilter) {
      MetricsFilter that = (MetricsFilter) o;
      return this.immutableSteps.equals(that.immutableSteps())
          && this.immutableNames.equals(that.immutableNames());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= immutableSteps.hashCode();
    h$ *= 1000003;
    h$ ^= immutableNames.hashCode();
    return h$;
  }

  static final class Builder extends MetricsFilter.Builder {
    private ImmutableSet.Builder<String> immutableStepsBuilder$;
    private ImmutableSet<String> immutableSteps;
    private ImmutableSet.Builder<MetricNameFilter> immutableNamesBuilder$;
    private ImmutableSet<MetricNameFilter> immutableNames;
    Builder() {
    }
    @Override
    protected ImmutableSet.Builder<String> immutableStepsBuilder() {
      if (immutableStepsBuilder$ == null) {
        immutableStepsBuilder$ = ImmutableSet.builder();
      }
      return immutableStepsBuilder$;
    }
    @Override
    protected ImmutableSet.Builder<MetricNameFilter> immutableNamesBuilder() {
      if (immutableNamesBuilder$ == null) {
        immutableNamesBuilder$ = ImmutableSet.builder();
      }
      return immutableNamesBuilder$;
    }
    @Override
    public MetricsFilter build() {
      if (immutableStepsBuilder$ != null) {
        this.immutableSteps = immutableStepsBuilder$.build();
      } else if (this.immutableSteps == null) {
        this.immutableSteps = ImmutableSet.of();
      }
      if (immutableNamesBuilder$ != null) {
        this.immutableNames = immutableNamesBuilder$.build();
      } else if (this.immutableNames == null) {
        this.immutableNames = ImmutableSet.of();
      }
      return new AutoValue_MetricsFilter(
          this.immutableSteps,
          this.immutableNames);
    }
  }

}
