package org.apache.beam.sdk.transforms;

import javax.annotation.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_WithFailures_ExceptionElement<T> extends WithFailures.ExceptionElement<T> {

  private final T element;

  private final Exception exception;

  AutoValue_WithFailures_ExceptionElement(
      T element,
      Exception exception) {
    if (element == null) {
      throw new NullPointerException("Null element");
    }
    this.element = element;
    if (exception == null) {
      throw new NullPointerException("Null exception");
    }
    this.exception = exception;
  }

  @Override
  public T element() {
    return element;
  }

  @Override
  public Exception exception() {
    return exception;
  }

  @Override
  public String toString() {
    return "ExceptionElement{"
        + "element=" + element + ", "
        + "exception=" + exception
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof WithFailures.ExceptionElement) {
      WithFailures.ExceptionElement<?> that = (WithFailures.ExceptionElement<?>) o;
      return this.element.equals(that.element())
          && this.exception.equals(that.exception());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= element.hashCode();
    h$ *= 1000003;
    h$ ^= exception.hashCode();
    return h$;
  }

}
