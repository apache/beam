package org.apache.beam.sdk.fn.windowing;

import javax.annotation.Generated;
import org.apache.beam.vendor.grpc.v1p48p1.com.google.protobuf.ByteString;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_EncodedBoundedWindow extends EncodedBoundedWindow {

  private final ByteString encodedWindow;

  AutoValue_EncodedBoundedWindow(
      ByteString encodedWindow) {
    if (encodedWindow == null) {
      throw new NullPointerException("Null encodedWindow");
    }
    this.encodedWindow = encodedWindow;
  }

  @Override
  public ByteString getEncodedWindow() {
    return encodedWindow;
  }

  @Override
  public String toString() {
    return "EncodedBoundedWindow{"
        + "encodedWindow=" + encodedWindow
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof EncodedBoundedWindow) {
      EncodedBoundedWindow that = (EncodedBoundedWindow) o;
      return this.encodedWindow.equals(that.getEncodedWindow());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= encodedWindow.hashCode();
    return h$;
  }

}
