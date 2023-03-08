package org.apache.beam.runners.core.construction;

import javax.annotation.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_ModelCoders_KvCoderComponents extends ModelCoders.KvCoderComponents {

  private final String keyCoderId;

  private final String valueCoderId;

  AutoValue_ModelCoders_KvCoderComponents(
      String keyCoderId,
      String valueCoderId) {
    if (keyCoderId == null) {
      throw new NullPointerException("Null keyCoderId");
    }
    this.keyCoderId = keyCoderId;
    if (valueCoderId == null) {
      throw new NullPointerException("Null valueCoderId");
    }
    this.valueCoderId = valueCoderId;
  }

  @Override
  public String keyCoderId() {
    return keyCoderId;
  }

  @Override
  public String valueCoderId() {
    return valueCoderId;
  }

  @Override
  public String toString() {
    return "KvCoderComponents{"
        + "keyCoderId=" + keyCoderId + ", "
        + "valueCoderId=" + valueCoderId
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof ModelCoders.KvCoderComponents) {
      ModelCoders.KvCoderComponents that = (ModelCoders.KvCoderComponents) o;
      return this.keyCoderId.equals(that.keyCoderId())
          && this.valueCoderId.equals(that.valueCoderId());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= keyCoderId.hashCode();
    h$ *= 1000003;
    h$ ^= valueCoderId.hashCode();
    return h$;
  }

}
