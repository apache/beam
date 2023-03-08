package org.apache.beam.sdk.runners;

import javax.annotation.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_PTransformOverride extends PTransformOverride {

  private final PTransformMatcher matcher;

  private final PTransformOverrideFactory<?, ?, ?> overrideFactory;

  AutoValue_PTransformOverride(
      PTransformMatcher matcher,
      PTransformOverrideFactory<?, ?, ?> overrideFactory) {
    if (matcher == null) {
      throw new NullPointerException("Null matcher");
    }
    this.matcher = matcher;
    if (overrideFactory == null) {
      throw new NullPointerException("Null overrideFactory");
    }
    this.overrideFactory = overrideFactory;
  }

  @Override
  public PTransformMatcher getMatcher() {
    return matcher;
  }

  @Override
  public PTransformOverrideFactory<?, ?, ?> getOverrideFactory() {
    return overrideFactory;
  }

  @Override
  public String toString() {
    return "PTransformOverride{"
        + "matcher=" + matcher + ", "
        + "overrideFactory=" + overrideFactory
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof PTransformOverride) {
      PTransformOverride that = (PTransformOverride) o;
      return this.matcher.equals(that.getMatcher())
          && this.overrideFactory.equals(that.getOverrideFactory());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= matcher.hashCode();
    h$ *= 1000003;
    h$ ^= overrideFactory.hashCode();
    return h$;
  }

}
