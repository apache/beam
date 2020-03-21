package org.apache.beam.sdk.io.gcp.healthcare;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;

public class HealthcareIOErrorCoder<T> extends CustomCoder<HealthcareIOError<T>> {
  private final Coder<T> originalCoder;
  private static final NullableCoder<String> STRING_CODER = NullableCoder.of(StringUtf8Coder.of());

  HealthcareIOErrorCoder(Coder<T> originalCoder) {
    this.originalCoder = NullableCoder.of(originalCoder);
  }

  @Override
  public void encode(HealthcareIOError<T> value, OutputStream outStream)
      throws CoderException, IOException {

    originalCoder.encode(value.getDataResource(), outStream);

    STRING_CODER.encode(value.getErrorMessage(), outStream);
    STRING_CODER.encode(value.getStackTrace(), outStream);
}

  @Override
  public HealthcareIOError<T> decode(InputStream inStream) throws CoderException, IOException {
    T dataResource = originalCoder.decode(inStream);
    String errorMessage = STRING_CODER.decode(inStream);
    String stackTrace = STRING_CODER.decode(inStream);
    return new HealthcareIOError<>(dataResource, errorMessage, stackTrace);
  }
}
