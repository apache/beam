package org.apache.beam.runners.core;

import javax.annotation.Generated;
import org.apache.beam.sdk.transforms.DoFn;
import org.joda.time.Instant;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_InMemoryBundleFinalizer_Finalization extends InMemoryBundleFinalizer.Finalization {

  private final Instant expiryTime;

  private final DoFn.BundleFinalizer.Callback callback;

  AutoValue_InMemoryBundleFinalizer_Finalization(
      Instant expiryTime,
      DoFn.BundleFinalizer.Callback callback) {
    if (expiryTime == null) {
      throw new NullPointerException("Null expiryTime");
    }
    this.expiryTime = expiryTime;
    if (callback == null) {
      throw new NullPointerException("Null callback");
    }
    this.callback = callback;
  }

  @Override
  public Instant getExpiryTime() {
    return expiryTime;
  }

  @Override
  public DoFn.BundleFinalizer.Callback getCallback() {
    return callback;
  }

  @Override
  public String toString() {
    return "Finalization{"
        + "expiryTime=" + expiryTime + ", "
        + "callback=" + callback
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof InMemoryBundleFinalizer.Finalization) {
      InMemoryBundleFinalizer.Finalization that = (InMemoryBundleFinalizer.Finalization) o;
      return this.expiryTime.equals(that.getExpiryTime())
          && this.callback.equals(that.getCallback());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= expiryTime.hashCode();
    h$ *= 1000003;
    h$ ^= callback.hashCode();
    return h$;
  }

}
