package org.apache.beam.sdk.transforms.splittabledofn;

import javax.annotation.Generated;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_SplitResult<RestrictionT> extends SplitResult<RestrictionT> {

  private final RestrictionT primary;

  private final RestrictionT residual;

  AutoValue_SplitResult(
      RestrictionT primary,
      RestrictionT residual) {
    this.primary = primary;
    this.residual = residual;
  }

  @Override
  public RestrictionT getPrimary() {
    return primary;
  }

  @Override
  public RestrictionT getResidual() {
    return residual;
  }

  @Override
  public String toString() {
    return "SplitResult{"
        + "primary=" + primary + ", "
        + "residual=" + residual
        + "}";
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof SplitResult) {
      SplitResult<?> that = (SplitResult<?>) o;
      return (this.primary == null ? that.getPrimary() == null : this.primary.equals(that.getPrimary()))
          && (this.residual == null ? that.getResidual() == null : this.residual.equals(that.getResidual()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (primary == null) ? 0 : primary.hashCode();
    h$ *= 1000003;
    h$ ^= (residual == null) ? 0 : residual.hashCode();
    return h$;
  }

}
