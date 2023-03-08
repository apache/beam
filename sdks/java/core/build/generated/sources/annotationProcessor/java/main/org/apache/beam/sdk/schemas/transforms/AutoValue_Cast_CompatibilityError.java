package org.apache.beam.sdk.schemas.transforms;

import java.util.List;
import javax.annotation.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_Cast_CompatibilityError extends Cast.CompatibilityError {

  private final List<String> path;

  private final String message;

  AutoValue_Cast_CompatibilityError(
      List<String> path,
      String message) {
    if (path == null) {
      throw new NullPointerException("Null path");
    }
    this.path = path;
    if (message == null) {
      throw new NullPointerException("Null message");
    }
    this.message = message;
  }

  @Override
  public List<String> path() {
    return path;
  }

  @Override
  public String message() {
    return message;
  }

  @Override
  public String toString() {
    return "CompatibilityError{"
        + "path=" + path + ", "
        + "message=" + message
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof Cast.CompatibilityError) {
      Cast.CompatibilityError that = (Cast.CompatibilityError) o;
      return this.path.equals(that.path())
          && this.message.equals(that.message());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= path.hashCode();
    h$ *= 1000003;
    h$ ^= message.hashCode();
    return h$;
  }

}
