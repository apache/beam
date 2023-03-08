package org.apache.beam.sdk.transforms.reflect;

import javax.annotation.Generated;
import org.apache.beam.sdk.transforms.DoFn;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_StableInvokerNamingStrategy extends StableInvokerNamingStrategy {

  private final Class<? extends DoFn<?, ?>> fnClass;

  private final @Nullable String suffix;

  AutoValue_StableInvokerNamingStrategy(
      Class<? extends DoFn<?, ?>> fnClass,
      @Nullable String suffix) {
    if (fnClass == null) {
      throw new NullPointerException("Null fnClass");
    }
    this.fnClass = fnClass;
    this.suffix = suffix;
  }

  @Override
  public Class<? extends DoFn<?, ?>> getFnClass() {
    return fnClass;
  }

  @Override
  public @Nullable String getSuffix() {
    return suffix;
  }

  @Override
  public String toString() {
    return "StableInvokerNamingStrategy{"
        + "fnClass=" + fnClass + ", "
        + "suffix=" + suffix
        + "}";
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof StableInvokerNamingStrategy) {
      StableInvokerNamingStrategy that = (StableInvokerNamingStrategy) o;
      return this.fnClass.equals(that.getFnClass())
          && (this.suffix == null ? that.getSuffix() == null : this.suffix.equals(that.getSuffix()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= fnClass.hashCode();
    h$ *= 1000003;
    h$ ^= (suffix == null) ? 0 : suffix.hashCode();
    return h$;
  }

}
