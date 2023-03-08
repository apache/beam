package org.apache.beam.sdk.transforms;

import javax.annotation.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_Watch_NonPollingGrowthState<OutputT> extends Watch.NonPollingGrowthState<OutputT> {

  private final Watch.Growth.PollResult<OutputT> pending;

  AutoValue_Watch_NonPollingGrowthState(
      Watch.Growth.PollResult<OutputT> pending) {
    if (pending == null) {
      throw new NullPointerException("Null pending");
    }
    this.pending = pending;
  }

  @Override
  public Watch.Growth.PollResult<OutputT> getPending() {
    return pending;
  }

  @Override
  public String toString() {
    return "NonPollingGrowthState{"
        + "pending=" + pending
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof Watch.NonPollingGrowthState) {
      Watch.NonPollingGrowthState<?> that = (Watch.NonPollingGrowthState<?>) o;
      return this.pending.equals(that.getPending());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= pending.hashCode();
    return h$;
  }

}
