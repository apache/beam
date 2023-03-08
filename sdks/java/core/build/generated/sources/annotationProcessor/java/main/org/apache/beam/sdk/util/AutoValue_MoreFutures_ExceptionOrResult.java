package org.apache.beam.sdk.util;

import javax.annotation.Generated;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_MoreFutures_ExceptionOrResult<T> extends MoreFutures.ExceptionOrResult<T> {

  private final MoreFutures.ExceptionOrResult.IsException isException;

  private final T getResult;

  private final @Nullable Throwable getException;

  AutoValue_MoreFutures_ExceptionOrResult(
      MoreFutures.ExceptionOrResult.IsException isException,
      T getResult,
      @Nullable Throwable getException) {
    if (isException == null) {
      throw new NullPointerException("Null isException");
    }
    this.isException = isException;
    this.getResult = getResult;
    this.getException = getException;
  }

  @Override
  public MoreFutures.ExceptionOrResult.IsException isException() {
    return isException;
  }

  @Override
  public T getResult() {
    return getResult;
  }

  @Override
  public @Nullable Throwable getException() {
    return getException;
  }

  @Override
  public String toString() {
    return "ExceptionOrResult{"
        + "isException=" + isException + ", "
        + "getResult=" + getResult + ", "
        + "getException=" + getException
        + "}";
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof MoreFutures.ExceptionOrResult) {
      MoreFutures.ExceptionOrResult<?> that = (MoreFutures.ExceptionOrResult<?>) o;
      return this.isException.equals(that.isException())
          && (this.getResult == null ? that.getResult() == null : this.getResult.equals(that.getResult()))
          && (this.getException == null ? that.getException() == null : this.getException.equals(that.getException()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= isException.hashCode();
    h$ *= 1000003;
    h$ ^= (getResult == null) ? 0 : getResult.hashCode();
    h$ *= 1000003;
    h$ ^= (getException == null) ? 0 : getException.hashCode();
    return h$;
  }

}
