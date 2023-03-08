package org.apache.beam.sdk.options;

import javax.annotation.Generated;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_ProxyInvocationHandler_BoundValue extends ProxyInvocationHandler.BoundValue {

  private final @Nullable Object value;

  private final boolean default0;

  AutoValue_ProxyInvocationHandler_BoundValue(
      @Nullable Object value,
      boolean default0) {
    this.value = value;
    this.default0 = default0;
  }

  @Override
  @Nullable Object getValue() {
    return value;
  }

  @Override
  boolean isDefault() {
    return default0;
  }

  @Override
  public String toString() {
    return "BoundValue{"
        + "value=" + value + ", "
        + "default=" + default0
        + "}";
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof ProxyInvocationHandler.BoundValue) {
      ProxyInvocationHandler.BoundValue that = (ProxyInvocationHandler.BoundValue) o;
      return (this.value == null ? that.getValue() == null : this.value.equals(that.getValue()))
          && this.default0 == that.isDefault();
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (value == null) ? 0 : value.hashCode();
    h$ *= 1000003;
    h$ ^= default0 ? 1231 : 1237;
    return h$;
  }

}
