package org.apache.beam.sdk.options;

import javax.annotation.Generated;
import org.apache.beam.sdk.transforms.display.DisplayData;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_ProxyInvocationHandler_DisplayDataValue extends ProxyInvocationHandler.DisplayDataValue {

  private final Object value;

  private final DisplayData.Type type;

  AutoValue_ProxyInvocationHandler_DisplayDataValue(
      Object value,
      DisplayData.Type type) {
    if (value == null) {
      throw new NullPointerException("Null value");
    }
    this.value = value;
    if (type == null) {
      throw new NullPointerException("Null type");
    }
    this.type = type;
  }

  @Override
  Object getValue() {
    return value;
  }

  @Override
  DisplayData.Type getType() {
    return type;
  }

  @Override
  public String toString() {
    return "DisplayDataValue{"
        + "value=" + value + ", "
        + "type=" + type
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof ProxyInvocationHandler.DisplayDataValue) {
      ProxyInvocationHandler.DisplayDataValue that = (ProxyInvocationHandler.DisplayDataValue) o;
      return this.value.equals(that.getValue())
          && this.type.equals(that.getType());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= value.hashCode();
    h$ *= 1000003;
    h$ ^= type.hashCode();
    return h$;
  }

}
