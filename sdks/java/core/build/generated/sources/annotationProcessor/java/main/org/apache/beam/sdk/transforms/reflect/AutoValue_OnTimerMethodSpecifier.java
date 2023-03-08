package org.apache.beam.sdk.transforms.reflect;

import javax.annotation.Generated;
import org.apache.beam.sdk.transforms.DoFn;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_OnTimerMethodSpecifier extends OnTimerMethodSpecifier {

  private final Class<? extends DoFn<?, ?>> fnClass;

  private final String timerId;

  AutoValue_OnTimerMethodSpecifier(
      Class<? extends DoFn<?, ?>> fnClass,
      String timerId) {
    if (fnClass == null) {
      throw new NullPointerException("Null fnClass");
    }
    this.fnClass = fnClass;
    if (timerId == null) {
      throw new NullPointerException("Null timerId");
    }
    this.timerId = timerId;
  }

  @Override
  public Class<? extends DoFn<?, ?>> fnClass() {
    return fnClass;
  }

  @Override
  public String timerId() {
    return timerId;
  }

  @Override
  public String toString() {
    return "OnTimerMethodSpecifier{"
        + "fnClass=" + fnClass + ", "
        + "timerId=" + timerId
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof OnTimerMethodSpecifier) {
      OnTimerMethodSpecifier that = (OnTimerMethodSpecifier) o;
      return this.fnClass.equals(that.fnClass())
          && this.timerId.equals(that.timerId());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= fnClass.hashCode();
    h$ *= 1000003;
    h$ ^= timerId.hashCode();
    return h$;
  }

}
