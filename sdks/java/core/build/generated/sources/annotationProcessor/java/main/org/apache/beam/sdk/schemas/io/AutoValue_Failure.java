package org.apache.beam.sdk.schemas.io;

import java.util.Arrays;
import javax.annotation.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_Failure extends Failure {

  private final byte[] payload;

  private final String error;

  private AutoValue_Failure(
      byte[] payload,
      String error) {
    this.payload = payload;
    this.error = error;
  }

  @SuppressWarnings("mutable")
  @Override
  public byte[] getPayload() {
    return payload;
  }

  @Override
  public String getError() {
    return error;
  }

  @Override
  public String toString() {
    return "Failure{"
        + "payload=" + Arrays.toString(payload) + ", "
        + "error=" + error
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof Failure) {
      Failure that = (Failure) o;
      return Arrays.equals(this.payload, (that instanceof AutoValue_Failure) ? ((AutoValue_Failure) that).payload : that.getPayload())
          && this.error.equals(that.getError());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= Arrays.hashCode(payload);
    h$ *= 1000003;
    h$ ^= error.hashCode();
    return h$;
  }

  static final class Builder extends Failure.Builder {
    private byte[] payload;
    private String error;
    Builder() {
    }
    @Override
    public Failure.Builder setPayload(byte[] payload) {
      if (payload == null) {
        throw new NullPointerException("Null payload");
      }
      this.payload = payload;
      return this;
    }
    @Override
    public Failure.Builder setError(String error) {
      if (error == null) {
        throw new NullPointerException("Null error");
      }
      this.error = error;
      return this;
    }
    @Override
    public Failure build() {
      if (this.payload == null
          || this.error == null) {
        StringBuilder missing = new StringBuilder();
        if (this.payload == null) {
          missing.append(" payload");
        }
        if (this.error == null) {
          missing.append(" error");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_Failure(
          this.payload,
          this.error);
    }
  }

}
