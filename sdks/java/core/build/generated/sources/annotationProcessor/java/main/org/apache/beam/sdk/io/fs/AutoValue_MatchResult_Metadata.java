package org.apache.beam.sdk.io.fs;

import javax.annotation.Generated;
import org.apache.beam.sdk.annotations.Experimental;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_MatchResult_Metadata extends MatchResult.Metadata {

  private final ResourceId resourceId;

  private final long sizeBytes;

  private final boolean isReadSeekEfficient;

  private final @Nullable String checksum;

  private final long lastModifiedMillis;

  private AutoValue_MatchResult_Metadata(
      ResourceId resourceId,
      long sizeBytes,
      boolean isReadSeekEfficient,
      @Nullable String checksum,
      long lastModifiedMillis) {
    this.resourceId = resourceId;
    this.sizeBytes = sizeBytes;
    this.isReadSeekEfficient = isReadSeekEfficient;
    this.checksum = checksum;
    this.lastModifiedMillis = lastModifiedMillis;
  }

  @Override
  public ResourceId resourceId() {
    return resourceId;
  }

  @Override
  public long sizeBytes() {
    return sizeBytes;
  }

  @Override
  public boolean isReadSeekEfficient() {
    return isReadSeekEfficient;
  }

  @Override
  public @Nullable String checksum() {
    return checksum;
  }

  @Experimental(Experimental.Kind.FILESYSTEM)
  @Override
  public long lastModifiedMillis() {
    return lastModifiedMillis;
  }

  @Override
  public String toString() {
    return "Metadata{"
        + "resourceId=" + resourceId + ", "
        + "sizeBytes=" + sizeBytes + ", "
        + "isReadSeekEfficient=" + isReadSeekEfficient + ", "
        + "checksum=" + checksum + ", "
        + "lastModifiedMillis=" + lastModifiedMillis
        + "}";
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof MatchResult.Metadata) {
      MatchResult.Metadata that = (MatchResult.Metadata) o;
      return this.resourceId.equals(that.resourceId())
          && this.sizeBytes == that.sizeBytes()
          && this.isReadSeekEfficient == that.isReadSeekEfficient()
          && (this.checksum == null ? that.checksum() == null : this.checksum.equals(that.checksum()))
          && this.lastModifiedMillis == that.lastModifiedMillis();
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= resourceId.hashCode();
    h$ *= 1000003;
    h$ ^= (int) ((sizeBytes >>> 32) ^ sizeBytes);
    h$ *= 1000003;
    h$ ^= isReadSeekEfficient ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= (checksum == null) ? 0 : checksum.hashCode();
    h$ *= 1000003;
    h$ ^= (int) ((lastModifiedMillis >>> 32) ^ lastModifiedMillis);
    return h$;
  }

  static final class Builder extends MatchResult.Metadata.Builder {
    private ResourceId resourceId;
    private Long sizeBytes;
    private Boolean isReadSeekEfficient;
    private @Nullable String checksum;
    private Long lastModifiedMillis;
    Builder() {
    }
    @Override
    public MatchResult.Metadata.Builder setResourceId(ResourceId resourceId) {
      if (resourceId == null) {
        throw new NullPointerException("Null resourceId");
      }
      this.resourceId = resourceId;
      return this;
    }
    @Override
    public MatchResult.Metadata.Builder setSizeBytes(long sizeBytes) {
      this.sizeBytes = sizeBytes;
      return this;
    }
    @Override
    public MatchResult.Metadata.Builder setIsReadSeekEfficient(boolean isReadSeekEfficient) {
      this.isReadSeekEfficient = isReadSeekEfficient;
      return this;
    }
    @Override
    public MatchResult.Metadata.Builder setChecksum(String checksum) {
      this.checksum = checksum;
      return this;
    }
    @Override
    public MatchResult.Metadata.Builder setLastModifiedMillis(long lastModifiedMillis) {
      this.lastModifiedMillis = lastModifiedMillis;
      return this;
    }
    @Override
    public MatchResult.Metadata build() {
      if (this.resourceId == null
          || this.sizeBytes == null
          || this.isReadSeekEfficient == null
          || this.lastModifiedMillis == null) {
        StringBuilder missing = new StringBuilder();
        if (this.resourceId == null) {
          missing.append(" resourceId");
        }
        if (this.sizeBytes == null) {
          missing.append(" sizeBytes");
        }
        if (this.isReadSeekEfficient == null) {
          missing.append(" isReadSeekEfficient");
        }
        if (this.lastModifiedMillis == null) {
          missing.append(" lastModifiedMillis");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_MatchResult_Metadata(
          this.resourceId,
          this.sizeBytes,
          this.isReadSeekEfficient,
          this.checksum,
          this.lastModifiedMillis);
    }
  }

}
