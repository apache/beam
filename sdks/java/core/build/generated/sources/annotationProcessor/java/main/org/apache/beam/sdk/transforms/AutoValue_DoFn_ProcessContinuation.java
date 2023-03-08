package org.apache.beam.sdk.transforms;

import javax.annotation.Generated;
import org.joda.time.Duration;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_DoFn_ProcessContinuation extends DoFn.ProcessContinuation {

  private final boolean shouldResume;

  private final Duration resumeDelay;

  AutoValue_DoFn_ProcessContinuation(
      boolean shouldResume,
      Duration resumeDelay) {
    this.shouldResume = shouldResume;
    if (resumeDelay == null) {
      throw new NullPointerException("Null resumeDelay");
    }
    this.resumeDelay = resumeDelay;
  }

  @Override
  public boolean shouldResume() {
    return shouldResume;
  }

  @Override
  public Duration resumeDelay() {
    return resumeDelay;
  }

  @Override
  public String toString() {
    return "ProcessContinuation{"
        + "shouldResume=" + shouldResume + ", "
        + "resumeDelay=" + resumeDelay
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof DoFn.ProcessContinuation) {
      DoFn.ProcessContinuation that = (DoFn.ProcessContinuation) o;
      return this.shouldResume == that.shouldResume()
          && this.resumeDelay.equals(that.resumeDelay());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= shouldResume ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= resumeDelay.hashCode();
    return h$;
  }

}
