package org.apache.beam.sdk.transforms;

import javax.annotation.Generated;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.hash.HashCode;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_Watch_PollingGrowthState<TerminationStateT> extends Watch.PollingGrowthState<TerminationStateT> {

  private final ImmutableMap<HashCode, Instant> completed;

  private final @Nullable Instant pollWatermark;

  private final TerminationStateT terminationState;

  AutoValue_Watch_PollingGrowthState(
      ImmutableMap<HashCode, Instant> completed,
      @Nullable Instant pollWatermark,
      TerminationStateT terminationState) {
    if (completed == null) {
      throw new NullPointerException("Null completed");
    }
    this.completed = completed;
    this.pollWatermark = pollWatermark;
    if (terminationState == null) {
      throw new NullPointerException("Null terminationState");
    }
    this.terminationState = terminationState;
  }

  @Override
  public ImmutableMap<HashCode, Instant> getCompleted() {
    return completed;
  }

  @Override
  public @Nullable Instant getPollWatermark() {
    return pollWatermark;
  }

  @Override
  public TerminationStateT getTerminationState() {
    return terminationState;
  }

  @Override
  public String toString() {
    return "PollingGrowthState{"
        + "completed=" + completed + ", "
        + "pollWatermark=" + pollWatermark + ", "
        + "terminationState=" + terminationState
        + "}";
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof Watch.PollingGrowthState) {
      Watch.PollingGrowthState<?> that = (Watch.PollingGrowthState<?>) o;
      return this.completed.equals(that.getCompleted())
          && (this.pollWatermark == null ? that.getPollWatermark() == null : this.pollWatermark.equals(that.getPollWatermark()))
          && this.terminationState.equals(that.getTerminationState());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= completed.hashCode();
    h$ *= 1000003;
    h$ ^= (pollWatermark == null) ? 0 : pollWatermark.hashCode();
    h$ *= 1000003;
    h$ ^= terminationState.hashCode();
    return h$;
  }

}
