package org.apache.beam.runners.core.construction.graph;

import javax.annotation.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_SplittableParDoExpander_SizedReplacement extends SplittableParDoExpander.SizedReplacement {

  private final boolean drain;

  private AutoValue_SplittableParDoExpander_SizedReplacement(
      boolean drain) {
    this.drain = drain;
  }

  @Override
  boolean isDrain() {
    return drain;
  }

  @Override
  public String toString() {
    return "SizedReplacement{"
        + "drain=" + drain
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof SplittableParDoExpander.SizedReplacement) {
      SplittableParDoExpander.SizedReplacement that = (SplittableParDoExpander.SizedReplacement) o;
      return this.drain == that.isDrain();
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= drain ? 1231 : 1237;
    return h$;
  }

  static final class Builder extends SplittableParDoExpander.SizedReplacement.Builder {
    private Boolean drain;
    Builder() {
    }
    @Override
    SplittableParDoExpander.SizedReplacement.Builder setDrain(boolean drain) {
      this.drain = drain;
      return this;
    }
    @Override
    SplittableParDoExpander.SizedReplacement build() {
      if (this.drain == null) {
        String missing = " drain";
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_SplittableParDoExpander_SizedReplacement(
          this.drain);
    }
  }

}
