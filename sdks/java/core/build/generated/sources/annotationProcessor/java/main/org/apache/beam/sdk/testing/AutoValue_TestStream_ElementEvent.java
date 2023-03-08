package org.apache.beam.sdk.testing;

import javax.annotation.Generated;
import org.apache.beam.sdk.values.TimestampedValue;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_TestStream_ElementEvent<T> extends TestStream.ElementEvent<T> {

  private final TestStream.EventType type;

  private final Iterable<TimestampedValue<T>> elements;

  AutoValue_TestStream_ElementEvent(
      TestStream.EventType type,
      Iterable<TimestampedValue<T>> elements) {
    if (type == null) {
      throw new NullPointerException("Null type");
    }
    this.type = type;
    if (elements == null) {
      throw new NullPointerException("Null elements");
    }
    this.elements = elements;
  }

  @Override
  public TestStream.EventType getType() {
    return type;
  }

  @Override
  public Iterable<TimestampedValue<T>> getElements() {
    return elements;
  }

  @Override
  public String toString() {
    return "ElementEvent{"
        + "type=" + type + ", "
        + "elements=" + elements
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof TestStream.ElementEvent) {
      TestStream.ElementEvent<?> that = (TestStream.ElementEvent<?>) o;
      return this.type.equals(that.getType())
          && this.elements.equals(that.getElements());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= type.hashCode();
    h$ *= 1000003;
    h$ ^= elements.hashCode();
    return h$;
  }

}
