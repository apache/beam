package org.apache.beam.sdk.transforms.splittabledofn;

import javax.annotation.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_RestrictionTracker_Progress extends RestrictionTracker.Progress {

  private final double workCompleted;

  private final double workRemaining;

  AutoValue_RestrictionTracker_Progress(
      double workCompleted,
      double workRemaining) {
    this.workCompleted = workCompleted;
    this.workRemaining = workRemaining;
  }

  @Override
  public double getWorkCompleted() {
    return workCompleted;
  }

  @Override
  public double getWorkRemaining() {
    return workRemaining;
  }

  @Override
  public String toString() {
    return "Progress{"
        + "workCompleted=" + workCompleted + ", "
        + "workRemaining=" + workRemaining
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof RestrictionTracker.Progress) {
      RestrictionTracker.Progress that = (RestrictionTracker.Progress) o;
      return Double.doubleToLongBits(this.workCompleted) == Double.doubleToLongBits(that.getWorkCompleted())
          && Double.doubleToLongBits(this.workRemaining) == Double.doubleToLongBits(that.getWorkRemaining());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (int) ((Double.doubleToLongBits(workCompleted) >>> 32) ^ Double.doubleToLongBits(workCompleted));
    h$ *= 1000003;
    h$ ^= (int) ((Double.doubleToLongBits(workRemaining) >>> 32) ^ Double.doubleToLongBits(workRemaining));
    return h$;
  }

}
