package org.apache.beam.sdk.io.fs;

import java.io.IOException;
import javax.annotation.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_MatchResult_Failure extends MatchResult.Failure {

  private final MatchResult.Status status;

  private final IOException getException;

  AutoValue_MatchResult_Failure(
      MatchResult.Status status,
      IOException getException) {
    if (status == null) {
      throw new NullPointerException("Null status");
    }
    this.status = status;
    if (getException == null) {
      throw new NullPointerException("Null getException");
    }
    this.getException = getException;
  }

  @Override
  public MatchResult.Status status() {
    return status;
  }

  @Override
  IOException getException() {
    return getException;
  }

  @Override
  public String toString() {
    return "Failure{"
        + "status=" + status + ", "
        + "getException=" + getException
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof MatchResult.Failure) {
      MatchResult.Failure that = (MatchResult.Failure) o;
      return this.status.equals(that.status())
          && this.getException.equals(that.getException());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= status.hashCode();
    h$ *= 1000003;
    h$ ^= getException.hashCode();
    return h$;
  }

}
