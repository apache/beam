package org.apache.beam.sdk.util;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.function.Function;
import java.util.function.Predicate;
import javax.annotation.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_RowJsonValueExtractors_ValidatingValueExtractor<W> extends RowJsonValueExtractors.ValidatingValueExtractor<W> {

  private final Predicate<JsonNode> validator;

  private final Function<JsonNode, W> extractor;

  private AutoValue_RowJsonValueExtractors_ValidatingValueExtractor(
      Predicate<JsonNode> validator,
      Function<JsonNode, W> extractor) {
    this.validator = validator;
    this.extractor = extractor;
  }

  @Override
  Predicate<JsonNode> validator() {
    return validator;
  }

  @Override
  Function<JsonNode, W> extractor() {
    return extractor;
  }

  @Override
  public String toString() {
    return "ValidatingValueExtractor{"
        + "validator=" + validator + ", "
        + "extractor=" + extractor
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof RowJsonValueExtractors.ValidatingValueExtractor) {
      RowJsonValueExtractors.ValidatingValueExtractor<?> that = (RowJsonValueExtractors.ValidatingValueExtractor<?>) o;
      return this.validator.equals(that.validator())
          && this.extractor.equals(that.extractor());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= validator.hashCode();
    h$ *= 1000003;
    h$ ^= extractor.hashCode();
    return h$;
  }

  static final class Builder<W> extends RowJsonValueExtractors.ValidatingValueExtractor.Builder<W> {
    private Predicate<JsonNode> validator;
    private Function<JsonNode, W> extractor;
    Builder() {
    }
    @Override
    RowJsonValueExtractors.ValidatingValueExtractor.Builder<W> setValidator(Predicate<JsonNode> validator) {
      if (validator == null) {
        throw new NullPointerException("Null validator");
      }
      this.validator = validator;
      return this;
    }
    @Override
    RowJsonValueExtractors.ValidatingValueExtractor.Builder<W> setExtractor(Function<JsonNode, W> extractor) {
      if (extractor == null) {
        throw new NullPointerException("Null extractor");
      }
      this.extractor = extractor;
      return this;
    }
    @Override
    RowJsonValueExtractors.ValidatingValueExtractor<W> build() {
      if (this.validator == null
          || this.extractor == null) {
        StringBuilder missing = new StringBuilder();
        if (this.validator == null) {
          missing.append(" validator");
        }
        if (this.extractor == null) {
          missing.append(" extractor");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_RowJsonValueExtractors_ValidatingValueExtractor<W>(
          this.validator,
          this.extractor);
    }
  }

}
