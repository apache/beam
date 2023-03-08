package org.apache.beam.sdk.fn.data;

import javax.annotation.Generated;
import org.apache.beam.sdk.coders.Coder;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_DataEndpoint<T> extends DataEndpoint<T> {

  private final String transformId;

  private final Coder<T> coder;

  private final FnDataReceiver<T> receiver;

  AutoValue_DataEndpoint(
      String transformId,
      Coder<T> coder,
      FnDataReceiver<T> receiver) {
    if (transformId == null) {
      throw new NullPointerException("Null transformId");
    }
    this.transformId = transformId;
    if (coder == null) {
      throw new NullPointerException("Null coder");
    }
    this.coder = coder;
    if (receiver == null) {
      throw new NullPointerException("Null receiver");
    }
    this.receiver = receiver;
  }

  @Override
  public String getTransformId() {
    return transformId;
  }

  @Override
  public Coder<T> getCoder() {
    return coder;
  }

  @Override
  public FnDataReceiver<T> getReceiver() {
    return receiver;
  }

  @Override
  public String toString() {
    return "DataEndpoint{"
        + "transformId=" + transformId + ", "
        + "coder=" + coder + ", "
        + "receiver=" + receiver
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof DataEndpoint) {
      DataEndpoint<?> that = (DataEndpoint<?>) o;
      return this.transformId.equals(that.getTransformId())
          && this.coder.equals(that.getCoder())
          && this.receiver.equals(that.getReceiver());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= transformId.hashCode();
    h$ *= 1000003;
    h$ ^= coder.hashCode();
    h$ *= 1000003;
    h$ ^= receiver.hashCode();
    return h$;
  }

}
