package org.apache.beam.sdk.io.fs;

import java.util.List;
import javax.annotation.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_MatchResult_Success extends MatchResult.Success {

  private final MatchResult.Status status;

  private final List<MatchResult.Metadata> getMetadata;

  AutoValue_MatchResult_Success(
      MatchResult.Status status,
      List<MatchResult.Metadata> getMetadata) {
    if (status == null) {
      throw new NullPointerException("Null status");
    }
    this.status = status;
    if (getMetadata == null) {
      throw new NullPointerException("Null getMetadata");
    }
    this.getMetadata = getMetadata;
  }

  @Override
  public MatchResult.Status status() {
    return status;
  }

  @Override
  List<MatchResult.Metadata> getMetadata() {
    return getMetadata;
  }

  @Override
  public String toString() {
    return "Success{"
        + "status=" + status + ", "
        + "getMetadata=" + getMetadata
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof MatchResult.Success) {
      MatchResult.Success that = (MatchResult.Success) o;
      return this.status.equals(that.status())
          && this.getMetadata.equals(that.getMetadata());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= status.hashCode();
    h$ *= 1000003;
    h$ ^= getMetadata.hashCode();
    return h$;
  }

}
