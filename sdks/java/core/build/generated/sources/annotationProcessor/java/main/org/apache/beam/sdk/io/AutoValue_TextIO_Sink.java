package org.apache.beam.sdk.io;

import javax.annotation.Generated;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_TextIO_Sink extends TextIO.Sink {

  private final @Nullable String header;

  private final @Nullable String footer;

  private AutoValue_TextIO_Sink(
      @Nullable String header,
      @Nullable String footer) {
    this.header = header;
    this.footer = footer;
  }

  @Override
  @Nullable String getHeader() {
    return header;
  }

  @Override
  @Nullable String getFooter() {
    return footer;
  }

  @Override
  public String toString() {
    return "Sink{"
        + "header=" + header + ", "
        + "footer=" + footer
        + "}";
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof TextIO.Sink) {
      TextIO.Sink that = (TextIO.Sink) o;
      return (this.header == null ? that.getHeader() == null : this.header.equals(that.getHeader()))
          && (this.footer == null ? that.getFooter() == null : this.footer.equals(that.getFooter()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (header == null) ? 0 : header.hashCode();
    h$ *= 1000003;
    h$ ^= (footer == null) ? 0 : footer.hashCode();
    return h$;
  }

  @Override
  TextIO.Sink.Builder toBuilder() {
    return new Builder(this);
  }

  static final class Builder extends TextIO.Sink.Builder {
    private @Nullable String header;
    private @Nullable String footer;
    Builder() {
    }
    private Builder(TextIO.Sink source) {
      this.header = source.getHeader();
      this.footer = source.getFooter();
    }
    @Override
    TextIO.Sink.Builder setHeader(String header) {
      this.header = header;
      return this;
    }
    @Override
    TextIO.Sink.Builder setFooter(String footer) {
      this.footer = footer;
      return this;
    }
    @Override
    TextIO.Sink build() {
      return new AutoValue_TextIO_Sink(
          this.header,
          this.footer);
    }
  }

}
