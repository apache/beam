package org.apache.beam.sdk.transforms.splittabledofn;

import javax.annotation.Generated;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_RestrictionTracker_TruncateResult<RestrictionT> extends RestrictionTracker.TruncateResult<RestrictionT> {

  private final RestrictionT truncatedRestriction;

  AutoValue_RestrictionTracker_TruncateResult(
      RestrictionT truncatedRestriction) {
    this.truncatedRestriction = truncatedRestriction;
  }

  @Override
  public RestrictionT getTruncatedRestriction() {
    return truncatedRestriction;
  }

  @Override
  public String toString() {
    return "TruncateResult{"
        + "truncatedRestriction=" + truncatedRestriction
        + "}";
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof RestrictionTracker.TruncateResult) {
      RestrictionTracker.TruncateResult<?> that = (RestrictionTracker.TruncateResult<?>) o;
      return (this.truncatedRestriction == null ? that.getTruncatedRestriction() == null : this.truncatedRestriction.equals(that.getTruncatedRestriction()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (truncatedRestriction == null) ? 0 : truncatedRestriction.hashCode();
    return h$;
  }

}
