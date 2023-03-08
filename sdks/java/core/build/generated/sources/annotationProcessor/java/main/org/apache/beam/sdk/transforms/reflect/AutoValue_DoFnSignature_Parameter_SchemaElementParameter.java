package org.apache.beam.sdk.transforms.reflect;

import javax.annotation.Generated;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_DoFnSignature_Parameter_SchemaElementParameter extends DoFnSignature.Parameter.SchemaElementParameter {

  private final TypeDescriptor<?> elementT;

  private final @Nullable String fieldAccessString;

  private final int index;

  private AutoValue_DoFnSignature_Parameter_SchemaElementParameter(
      TypeDescriptor<?> elementT,
      @Nullable String fieldAccessString,
      int index) {
    this.elementT = elementT;
    this.fieldAccessString = fieldAccessString;
    this.index = index;
  }

  @Override
  public TypeDescriptor<?> elementT() {
    return elementT;
  }

  @Override
  public @Nullable String fieldAccessString() {
    return fieldAccessString;
  }

  @Override
  public int index() {
    return index;
  }

  @Override
  public String toString() {
    return "SchemaElementParameter{"
        + "elementT=" + elementT + ", "
        + "fieldAccessString=" + fieldAccessString + ", "
        + "index=" + index
        + "}";
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof DoFnSignature.Parameter.SchemaElementParameter) {
      DoFnSignature.Parameter.SchemaElementParameter that = (DoFnSignature.Parameter.SchemaElementParameter) o;
      return this.elementT.equals(that.elementT())
          && (this.fieldAccessString == null ? that.fieldAccessString() == null : this.fieldAccessString.equals(that.fieldAccessString()))
          && this.index == that.index();
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= elementT.hashCode();
    h$ *= 1000003;
    h$ ^= (fieldAccessString == null) ? 0 : fieldAccessString.hashCode();
    h$ *= 1000003;
    h$ ^= index;
    return h$;
  }

  @Override
  public DoFnSignature.Parameter.SchemaElementParameter.Builder toBuilder() {
    return new Builder(this);
  }

  static final class Builder extends DoFnSignature.Parameter.SchemaElementParameter.Builder {
    private TypeDescriptor<?> elementT;
    private @Nullable String fieldAccessString;
    private Integer index;
    Builder() {
    }
    private Builder(DoFnSignature.Parameter.SchemaElementParameter source) {
      this.elementT = source.elementT();
      this.fieldAccessString = source.fieldAccessString();
      this.index = source.index();
    }
    @Override
    public DoFnSignature.Parameter.SchemaElementParameter.Builder setElementT(TypeDescriptor<?> elementT) {
      if (elementT == null) {
        throw new NullPointerException("Null elementT");
      }
      this.elementT = elementT;
      return this;
    }
    @Override
    public DoFnSignature.Parameter.SchemaElementParameter.Builder setFieldAccessString(@Nullable String fieldAccessString) {
      this.fieldAccessString = fieldAccessString;
      return this;
    }
    @Override
    public DoFnSignature.Parameter.SchemaElementParameter.Builder setIndex(int index) {
      this.index = index;
      return this;
    }
    @Override
    public DoFnSignature.Parameter.SchemaElementParameter build() {
      if (this.elementT == null
          || this.index == null) {
        StringBuilder missing = new StringBuilder();
        if (this.elementT == null) {
          missing.append(" elementT");
        }
        if (this.index == null) {
          missing.append(" index");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_DoFnSignature_Parameter_SchemaElementParameter(
          this.elementT,
          this.fieldAccessString,
          this.index);
    }
  }

}
