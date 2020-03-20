package org.apache.beam.sdk.io.gcp.healthcare;

import java.io.IOException;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.tuple.Pair;

/**
 * Class for capturing errors on IO operations on Google Cloud Healthcare APIs resources.
 * */
public class HealthcareIOError<T> {
  public T dataResource;
  public IOException error;

  private HealthcareIOError(T dataResource, IOException error) {
    this.dataResource = dataResource;
    this.error = error;
  }

  public IOException getError() {
    return error;
  }

  public T getDataResource() {
    return dataResource;
  }

  static <T> HealthcareIOError<T> of(T dataResource, IOException error) {
    return new HealthcareIOError(dataResource, error);
  }
}
