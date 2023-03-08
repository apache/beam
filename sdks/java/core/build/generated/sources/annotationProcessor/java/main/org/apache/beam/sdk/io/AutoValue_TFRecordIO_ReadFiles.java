package org.apache.beam.sdk.io;

import javax.annotation.Generated;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_TFRecordIO_ReadFiles extends TFRecordIO.ReadFiles {

  private AutoValue_TFRecordIO_ReadFiles() {
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof TFRecordIO.ReadFiles) {
      return true;
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    return h$;
  }

  @Override
  TFRecordIO.ReadFiles.Builder toBuilder() {
    return new Builder(this);
  }

  static final class Builder extends TFRecordIO.ReadFiles.Builder {
    Builder() {
    }
    private Builder(TFRecordIO.ReadFiles source) {
    }
    @Override
    TFRecordIO.ReadFiles build() {
      return new AutoValue_TFRecordIO_ReadFiles();
    }
  }

}
