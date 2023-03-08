package org.apache.beam.runners.core.construction;

import javax.annotation.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_ModelCoders_WindowedValueCoderComponents extends ModelCoders.WindowedValueCoderComponents {

  private final String elementCoderId;

  private final String windowCoderId;

  AutoValue_ModelCoders_WindowedValueCoderComponents(
      String elementCoderId,
      String windowCoderId) {
    if (elementCoderId == null) {
      throw new NullPointerException("Null elementCoderId");
    }
    this.elementCoderId = elementCoderId;
    if (windowCoderId == null) {
      throw new NullPointerException("Null windowCoderId");
    }
    this.windowCoderId = windowCoderId;
  }

  @Override
  public String elementCoderId() {
    return elementCoderId;
  }

  @Override
  public String windowCoderId() {
    return windowCoderId;
  }

  @Override
  public String toString() {
    return "WindowedValueCoderComponents{"
        + "elementCoderId=" + elementCoderId + ", "
        + "windowCoderId=" + windowCoderId
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof ModelCoders.WindowedValueCoderComponents) {
      ModelCoders.WindowedValueCoderComponents that = (ModelCoders.WindowedValueCoderComponents) o;
      return this.elementCoderId.equals(that.elementCoderId())
          && this.windowCoderId.equals(that.windowCoderId());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= elementCoderId.hashCode();
    h$ *= 1000003;
    h$ ^= windowCoderId.hashCode();
    return h$;
  }

}
