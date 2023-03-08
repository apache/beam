package org.apache.beam.sdk.runners;

import javax.annotation.Generated;
import org.apache.beam.sdk.values.TaggedPValue;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_PTransformOverrideFactory_ReplacementOutput extends PTransformOverrideFactory.ReplacementOutput {

  private final TaggedPValue original;

  private final TaggedPValue replacement;

  AutoValue_PTransformOverrideFactory_ReplacementOutput(
      TaggedPValue original,
      TaggedPValue replacement) {
    if (original == null) {
      throw new NullPointerException("Null original");
    }
    this.original = original;
    if (replacement == null) {
      throw new NullPointerException("Null replacement");
    }
    this.replacement = replacement;
  }

  @Override
  public TaggedPValue getOriginal() {
    return original;
  }

  @Override
  public TaggedPValue getReplacement() {
    return replacement;
  }

  @Override
  public String toString() {
    return "ReplacementOutput{"
        + "original=" + original + ", "
        + "replacement=" + replacement
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof PTransformOverrideFactory.ReplacementOutput) {
      PTransformOverrideFactory.ReplacementOutput that = (PTransformOverrideFactory.ReplacementOutput) o;
      return this.original.equals(that.getOriginal())
          && this.replacement.equals(that.getReplacement());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= original.hashCode();
    h$ *= 1000003;
    h$ ^= replacement.hashCode();
    return h$;
  }

}
