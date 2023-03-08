package org.apache.beam.sdk.io.fs;

import javax.annotation.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_CreateOptions_StandardCreateOptions extends CreateOptions.StandardCreateOptions {

  private final String mimeType;

  private final Boolean expectFileToNotExist;

  private AutoValue_CreateOptions_StandardCreateOptions(
      String mimeType,
      Boolean expectFileToNotExist) {
    this.mimeType = mimeType;
    this.expectFileToNotExist = expectFileToNotExist;
  }

  @Override
  public String mimeType() {
    return mimeType;
  }

  @Override
  public Boolean expectFileToNotExist() {
    return expectFileToNotExist;
  }

  @Override
  public String toString() {
    return "StandardCreateOptions{"
        + "mimeType=" + mimeType + ", "
        + "expectFileToNotExist=" + expectFileToNotExist
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof CreateOptions.StandardCreateOptions) {
      CreateOptions.StandardCreateOptions that = (CreateOptions.StandardCreateOptions) o;
      return this.mimeType.equals(that.mimeType())
          && this.expectFileToNotExist.equals(that.expectFileToNotExist());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= mimeType.hashCode();
    h$ *= 1000003;
    h$ ^= expectFileToNotExist.hashCode();
    return h$;
  }

  static final class Builder extends CreateOptions.StandardCreateOptions.Builder {
    private String mimeType;
    private Boolean expectFileToNotExist;
    Builder() {
    }
    @Override
    public CreateOptions.StandardCreateOptions.Builder setMimeType(String mimeType) {
      if (mimeType == null) {
        throw new NullPointerException("Null mimeType");
      }
      this.mimeType = mimeType;
      return this;
    }
    @Override
    public CreateOptions.StandardCreateOptions.Builder setExpectFileToNotExist(Boolean expectFileToNotExist) {
      if (expectFileToNotExist == null) {
        throw new NullPointerException("Null expectFileToNotExist");
      }
      this.expectFileToNotExist = expectFileToNotExist;
      return this;
    }
    @Override
    public CreateOptions.StandardCreateOptions build() {
      if (this.mimeType == null
          || this.expectFileToNotExist == null) {
        StringBuilder missing = new StringBuilder();
        if (this.mimeType == null) {
          missing.append(" mimeType");
        }
        if (this.expectFileToNotExist == null) {
          missing.append(" expectFileToNotExist");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_CreateOptions_StandardCreateOptions(
          this.mimeType,
          this.expectFileToNotExist);
    }
  }

}
