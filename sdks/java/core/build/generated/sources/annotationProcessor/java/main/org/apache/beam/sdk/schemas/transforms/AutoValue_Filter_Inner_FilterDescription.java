package org.apache.beam.sdk.schemas.transforms;

import javax.annotation.Generated;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.checkerframework.checker.nullness.qual.Nullable;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_Filter_Inner_FilterDescription<FieldT> extends Filter.Inner.FilterDescription<FieldT> {

  private final FieldAccessDescriptor fieldAccessDescriptor;

  private final SerializableFunction<FieldT, Boolean> predicate;

  private final @Nullable Schema selectedSchema;

  private final boolean selectsSingleField;

  private final @Nullable Schema inputSchema;

  private AutoValue_Filter_Inner_FilterDescription(
      FieldAccessDescriptor fieldAccessDescriptor,
      SerializableFunction<FieldT, Boolean> predicate,
      @Nullable Schema selectedSchema,
      boolean selectsSingleField,
      @Nullable Schema inputSchema) {
    this.fieldAccessDescriptor = fieldAccessDescriptor;
    this.predicate = predicate;
    this.selectedSchema = selectedSchema;
    this.selectsSingleField = selectsSingleField;
    this.inputSchema = inputSchema;
  }

  @Override
  FieldAccessDescriptor getFieldAccessDescriptor() {
    return fieldAccessDescriptor;
  }

  @Override
  SerializableFunction<FieldT, Boolean> getPredicate() {
    return predicate;
  }

  @Override
  @Nullable Schema getSelectedSchema() {
    return selectedSchema;
  }

  @Override
  boolean getSelectsSingleField() {
    return selectsSingleField;
  }

  @Override
  @Nullable Schema getInputSchema() {
    return inputSchema;
  }

  @Override
  public String toString() {
    return "FilterDescription{"
        + "fieldAccessDescriptor=" + fieldAccessDescriptor + ", "
        + "predicate=" + predicate + ", "
        + "selectedSchema=" + selectedSchema + ", "
        + "selectsSingleField=" + selectsSingleField + ", "
        + "inputSchema=" + inputSchema
        + "}";
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof Filter.Inner.FilterDescription) {
      Filter.Inner.FilterDescription<?> that = (Filter.Inner.FilterDescription<?>) o;
      return this.fieldAccessDescriptor.equals(that.getFieldAccessDescriptor())
          && this.predicate.equals(that.getPredicate())
          && (this.selectedSchema == null ? that.getSelectedSchema() == null : this.selectedSchema.equals(that.getSelectedSchema()))
          && this.selectsSingleField == that.getSelectsSingleField()
          && (this.inputSchema == null ? that.getInputSchema() == null : this.inputSchema.equals(that.getInputSchema()));
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= fieldAccessDescriptor.hashCode();
    h$ *= 1000003;
    h$ ^= predicate.hashCode();
    h$ *= 1000003;
    h$ ^= (selectedSchema == null) ? 0 : selectedSchema.hashCode();
    h$ *= 1000003;
    h$ ^= selectsSingleField ? 1231 : 1237;
    h$ *= 1000003;
    h$ ^= (inputSchema == null) ? 0 : inputSchema.hashCode();
    return h$;
  }

  @Override
  Filter.Inner.FilterDescription.Builder<FieldT> toBuilder() {
    return new Builder<FieldT>(this);
  }

  static final class Builder<FieldT> extends Filter.Inner.FilterDescription.Builder<FieldT> {
    private FieldAccessDescriptor fieldAccessDescriptor;
    private SerializableFunction<FieldT, Boolean> predicate;
    private @Nullable Schema selectedSchema;
    private Boolean selectsSingleField;
    private @Nullable Schema inputSchema;
    Builder() {
    }
    private Builder(Filter.Inner.FilterDescription<FieldT> source) {
      this.fieldAccessDescriptor = source.getFieldAccessDescriptor();
      this.predicate = source.getPredicate();
      this.selectedSchema = source.getSelectedSchema();
      this.selectsSingleField = source.getSelectsSingleField();
      this.inputSchema = source.getInputSchema();
    }
    @Override
    Filter.Inner.FilterDescription.Builder<FieldT> setFieldAccessDescriptor(FieldAccessDescriptor fieldAccessDescriptor) {
      if (fieldAccessDescriptor == null) {
        throw new NullPointerException("Null fieldAccessDescriptor");
      }
      this.fieldAccessDescriptor = fieldAccessDescriptor;
      return this;
    }
    @Override
    Filter.Inner.FilterDescription.Builder<FieldT> setPredicate(SerializableFunction<FieldT, Boolean> predicate) {
      if (predicate == null) {
        throw new NullPointerException("Null predicate");
      }
      this.predicate = predicate;
      return this;
    }
    @Override
    Filter.Inner.FilterDescription.Builder<FieldT> setSelectedSchema(@Nullable Schema selectedSchema) {
      this.selectedSchema = selectedSchema;
      return this;
    }
    @Override
    Filter.Inner.FilterDescription.Builder<FieldT> setSelectsSingleField(boolean selectsSingleField) {
      this.selectsSingleField = selectsSingleField;
      return this;
    }
    @Override
    Filter.Inner.FilterDescription.Builder<FieldT> setInputSchema(@Nullable Schema inputSchema) {
      this.inputSchema = inputSchema;
      return this;
    }
    @Override
    Filter.Inner.FilterDescription<FieldT> build() {
      if (this.fieldAccessDescriptor == null
          || this.predicate == null
          || this.selectsSingleField == null) {
        StringBuilder missing = new StringBuilder();
        if (this.fieldAccessDescriptor == null) {
          missing.append(" fieldAccessDescriptor");
        }
        if (this.predicate == null) {
          missing.append(" predicate");
        }
        if (this.selectsSingleField == null) {
          missing.append(" selectsSingleField");
        }
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_Filter_Inner_FilterDescription<FieldT>(
          this.fieldAccessDescriptor,
          this.predicate,
          this.selectedSchema,
          this.selectsSingleField,
          this.inputSchema);
    }
  }

}
