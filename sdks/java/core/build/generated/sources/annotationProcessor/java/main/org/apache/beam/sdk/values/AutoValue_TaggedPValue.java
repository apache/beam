package org.apache.beam.sdk.values;

import javax.annotation.Generated;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_TaggedPValue extends TaggedPValue {

  private final TupleTag<?> tag;

  private final PCollection<?> value;

  AutoValue_TaggedPValue(
      TupleTag<?> tag,
      PCollection<?> value) {
    if (tag == null) {
      throw new NullPointerException("Null tag");
    }
    this.tag = tag;
    if (value == null) {
      throw new NullPointerException("Null value");
    }
    this.value = value;
  }

  @Override
  public TupleTag<?> getTag() {
    return tag;
  }

  @Override
  public PCollection<?> getValue() {
    return value;
  }

  @Override
  public String toString() {
    return "TaggedPValue{"
        + "tag=" + tag + ", "
        + "value=" + value
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof TaggedPValue) {
      TaggedPValue that = (TaggedPValue) o;
      return this.tag.equals(that.getTag())
          && this.value.equals(that.getValue());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= tag.hashCode();
    h$ *= 1000003;
    h$ ^= value.hashCode();
    return h$;
  }

}
