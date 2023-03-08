package org.apache.beam.sdk.schemas.transforms;

import java.util.List;
import javax.annotation.Generated;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.CombineFns;
import org.checkerframework.checker.nullness.qual.Nullable;

@SuppressWarnings("rawtypes")
@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_SchemaAggregateFn_Inner extends SchemaAggregateFn.Inner {

  private final @Nullable Schema inputSchema;

  private final @Nullable Schema outputSchema;

  private final CombineFns.@Nullable ComposedCombineFn composedCombineFn;

  private final List<SchemaAggregateFn.Inner.FieldAggregation> fieldAggregations;

  private AutoValue_SchemaAggregateFn_Inner(
      @Nullable Schema inputSchema,
      @Nullable Schema outputSchema,
      CombineFns.@Nullable ComposedCombineFn composedCombineFn,
      List<SchemaAggregateFn.Inner.FieldAggregation> fieldAggregations) {
    this.inputSchema = inputSchema;
    this.outputSchema = outputSchema;
    this.composedCombineFn = composedCombineFn;
    this.fieldAggregations = fieldAggregations;
  }

  @Override
  @Nullable Schema getInputSchema() {
    return inputSchema;
  }

  @Override
  @Nullable Schema getOutputSchema() {
    return outputSchema;
  }

  @Override
  CombineFns.@Nullable ComposedCombineFn getComposedCombineFn() {
    return composedCombineFn;
  }

  @Override
  List<SchemaAggregateFn.Inner.FieldAggregation> getFieldAggregations() {
    return fieldAggregations;
  }

  @Override
  public String toString() {
    return "Inner{"
        + "inputSchema=" + inputSchema + ", "
        + "outputSchema=" + outputSchema + ", "
        + "composedCombineFn=" + composedCombineFn + ", "
        + "fieldAggregations=" + fieldAggregations
        + "}";
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof SchemaAggregateFn.Inner) {
      SchemaAggregateFn.Inner that = (SchemaAggregateFn.Inner) o;
      return (this.inputSchema == null ? that.getInputSchema() == null : this.inputSchema.equals(that.getInputSchema()))
          && (this.outputSchema == null ? that.getOutputSchema() == null : this.outputSchema.equals(that.getOutputSchema()))
          && (this.composedCombineFn == null ? that.getComposedCombineFn() == null : this.composedCombineFn.equals(that.getComposedCombineFn()))
          && this.fieldAggregations.equals(that.getFieldAggregations());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (inputSchema == null) ? 0 : inputSchema.hashCode();
    h$ *= 1000003;
    h$ ^= (outputSchema == null) ? 0 : outputSchema.hashCode();
    h$ *= 1000003;
    h$ ^= (composedCombineFn == null) ? 0 : composedCombineFn.hashCode();
    h$ *= 1000003;
    h$ ^= fieldAggregations.hashCode();
    return h$;
  }

  @Override
  SchemaAggregateFn.Inner.Builder toBuilder() {
    return new Builder(this);
  }

  @SuppressWarnings("rawtypes")
  static final class Builder extends SchemaAggregateFn.Inner.Builder {
    private @Nullable Schema inputSchema;
    private @Nullable Schema outputSchema;
    private CombineFns.@Nullable ComposedCombineFn composedCombineFn;
    private List<SchemaAggregateFn.Inner.FieldAggregation> fieldAggregations;
    Builder() {
    }
    private Builder(SchemaAggregateFn.Inner source) {
      this.inputSchema = source.getInputSchema();
      this.outputSchema = source.getOutputSchema();
      this.composedCombineFn = source.getComposedCombineFn();
      this.fieldAggregations = source.getFieldAggregations();
    }
    @Override
    SchemaAggregateFn.Inner.Builder setInputSchema(@Nullable Schema inputSchema) {
      this.inputSchema = inputSchema;
      return this;
    }
    @Override
    SchemaAggregateFn.Inner.Builder setOutputSchema(@Nullable Schema outputSchema) {
      this.outputSchema = outputSchema;
      return this;
    }
    @Override
    SchemaAggregateFn.Inner.Builder setComposedCombineFn(CombineFns.@Nullable ComposedCombineFn composedCombineFn) {
      this.composedCombineFn = composedCombineFn;
      return this;
    }
    @Override
    SchemaAggregateFn.Inner.Builder setFieldAggregations(List<SchemaAggregateFn.Inner.FieldAggregation> fieldAggregations) {
      if (fieldAggregations == null) {
        throw new NullPointerException("Null fieldAggregations");
      }
      this.fieldAggregations = fieldAggregations;
      return this;
    }
    @Override
    SchemaAggregateFn.Inner build() {
      if (this.fieldAggregations == null) {
        String missing = " fieldAggregations";
        throw new IllegalStateException("Missing required properties:" + missing);
      }
      return new AutoValue_SchemaAggregateFn_Inner(
          this.inputSchema,
          this.outputSchema,
          this.composedCombineFn,
          this.fieldAggregations);
    }
  }

}
