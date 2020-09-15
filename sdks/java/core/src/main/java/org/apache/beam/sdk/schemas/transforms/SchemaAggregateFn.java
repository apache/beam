/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.schemas.transforms;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.FieldTypeDescriptors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.utils.RowSelector;
import org.apache.beam.sdk.schemas.utils.SelectHelpers;
import org.apache.beam.sdk.schemas.utils.SelectHelpers.RowSelectorContainer;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.CombineFns;
import org.apache.beam.sdk.transforms.CombineFns.CoCombineResult;
import org.apache.beam.sdk.transforms.CombineFns.ComposedCombineFn;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;

/** This is the builder used by {@link Group} to build up a composed {@link CombineFn}. */
@Experimental(Kind.SCHEMAS)
class SchemaAggregateFn {
  static Inner create() {
    return new AutoValue_SchemaAggregateFn_Inner.Builder()
        .setFieldAggregations(Lists.newArrayList())
        .build();
  }

  /** Implementation of {@link #create}. */
  @AutoValue
  abstract static class Inner extends CombineFn<Row, Object[], Row> {
    // Represents an aggregation of one or more fields.
    static class FieldAggregation<FieldT, AccumT, OutputT> implements Serializable {
      FieldAccessDescriptor fieldsToAggregate;
      private final boolean aggregateBaseValues;
      // The specification of the output field.
      private final Field outputField;
      // The combine function.
      private final CombineFn<FieldT, AccumT, OutputT> fn;
      // The TupleTag identifying this aggregation element in the composed combine fn.
      private final TupleTag<Object> combineTag;
      // The schema corresponding to the the subset of input fields being aggregated.
      private final @Nullable Schema inputSubSchema;
      private final @Nullable FieldAccessDescriptor flattenedFieldAccessDescriptor;
      // The flattened version of inputSubSchema.
      private final @Nullable Schema flattenedInputSubSchema;
      // The output schema resulting from the aggregation.
      private final Schema aggregationSchema;
      private final boolean needsFlattening;

      FieldAggregation(
          FieldAccessDescriptor fieldsToAggregate,
          boolean aggregateBaseValues,
          Field outputField,
          CombineFn<FieldT, AccumT, OutputT> fn,
          TupleTag<Object> combineTag) {
        this(
            fieldsToAggregate,
            aggregateBaseValues,
            outputField,
            fn,
            combineTag,
            Schema.builder().addField(outputField).build(),
            null);
      }

      FieldAggregation(
          FieldAccessDescriptor fieldsToAggregate,
          boolean aggregateBaseValues,
          Field outputField,
          CombineFn<FieldT, AccumT, OutputT> fn,
          TupleTag<Object> combineTag,
          Schema aggregationSchema,
          @Nullable Schema inputSchema) {
        this.aggregateBaseValues = aggregateBaseValues;
        if (inputSchema != null) {
          this.fieldsToAggregate = fieldsToAggregate.resolve(inputSchema);
          if (aggregateBaseValues) {
            Preconditions.checkArgument(fieldsToAggregate.referencesSingleField());
          }
          this.inputSubSchema = SelectHelpers.getOutputSchema(inputSchema, this.fieldsToAggregate);
          this.flattenedFieldAccessDescriptor =
              SelectHelpers.allLeavesDescriptor(inputSubSchema, SelectHelpers.CONCAT_FIELD_NAMES);
          this.flattenedInputSubSchema =
              SelectHelpers.getOutputSchema(inputSubSchema, flattenedFieldAccessDescriptor);
          this.needsFlattening = !inputSchema.equals(flattenedInputSubSchema);
        } else {
          this.fieldsToAggregate = fieldsToAggregate;
          this.inputSubSchema = null;
          this.flattenedFieldAccessDescriptor = null;
          this.flattenedInputSubSchema = null;
          this.needsFlattening = false;
        }
        this.outputField = outputField;
        this.fn = fn;
        this.combineTag = combineTag;
        this.aggregationSchema = aggregationSchema;
      }

      // The Schema is not necessarily known when the SchemaAggregateFn is created. Once the schema
      // is known, resolve will be called with the proper schema.
      FieldAggregation<FieldT, AccumT, OutputT> resolve(Schema schema) {
        return new FieldAggregation<>(
            fieldsToAggregate,
            aggregateBaseValues,
            outputField,
            fn,
            combineTag,
            aggregationSchema,
            schema);
      }
    }

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setInputSchema(@Nullable Schema inputSchema);

      abstract Builder setOutputSchema(@Nullable Schema outputSchema);

      abstract Builder setComposedCombineFn(@Nullable ComposedCombineFn composedCombineFn);

      abstract Builder setFieldAggregations(List<FieldAggregation> fieldAggregations);

      abstract Inner build();
    }

    abstract @Nullable Schema getInputSchema();

    abstract @Nullable Schema getOutputSchema();

    abstract @Nullable ComposedCombineFn getComposedCombineFn();

    abstract List<FieldAggregation> getFieldAggregations();

    /** Once the schema is known, this function is called by the {@link Group} transform. */
    Inner withSchema(Schema inputSchema) {
      List<FieldAggregation> fieldAggregations =
          getFieldAggregations().stream()
              .map(f -> f.resolve(inputSchema))
              .collect(Collectors.toList());

      ComposedCombineFn composedCombineFn = null;
      for (int i = 0; i < fieldAggregations.size(); ++i) {
        FieldAggregation fieldAggregation = fieldAggregations.get(i);
        SimpleFunction<Row, ?> extractFunction;
        Coder extractOutputCoder;
        if (fieldAggregation.fieldsToAggregate.referencesSingleField()) {
          extractFunction =
              new ExtractSingleFieldFunction(
                  inputSchema, fieldAggregation.aggregateBaseValues, fieldAggregation);

          FieldType fieldType = fieldAggregation.flattenedInputSubSchema.getField(0).getType();
          if (fieldAggregation.aggregateBaseValues) {
            while (fieldType.getTypeName().isLogicalType()) {
              fieldType = fieldType.getLogicalType().getBaseType();
            }
          }
          extractOutputCoder = SchemaCoder.coderForFieldType(fieldType);
        } else {
          extractFunction = new ExtractFieldsFunction(inputSchema, fieldAggregation);
          extractOutputCoder = SchemaCoder.of(fieldAggregation.inputSubSchema);
        }
        if (i == 0) {
          composedCombineFn =
              CombineFns.compose()
                  .with(
                      extractFunction,
                      extractOutputCoder,
                      fieldAggregation.fn,
                      fieldAggregation.combineTag);
        } else {
          composedCombineFn =
              composedCombineFn.with(
                  extractFunction,
                  extractOutputCoder,
                  fieldAggregation.fn,
                  fieldAggregation.combineTag);
        }
      }

      return toBuilder()
          .setInputSchema(inputSchema)
          .setComposedCombineFn(composedCombineFn)
          .setFieldAggregations(fieldAggregations)
          .build();
    }

    /** Aggregate all values of a set of fields into an output field. */
    <CombineInputT, AccumT, CombineOutputT> Inner aggregateFields(
        FieldAccessDescriptor fieldsToAggregate,
        boolean aggregateBaseValues,
        CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
        String outputFieldName) {
      return aggregateFields(
          fieldsToAggregate,
          aggregateBaseValues,
          fn,
          Field.of(outputFieldName, FieldTypeDescriptors.fieldTypeForJavaType(fn.getOutputType())));
    }

    /** Aggregate all values of a set of fields into an output field. */
    <CombineInputT, AccumT, CombineOutputT> Inner aggregateFields(
        FieldAccessDescriptor fieldsToAggregate,
        boolean aggregateBaseValues,
        CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
        Field outputField) {
      List<FieldAggregation> fieldAggregations = getFieldAggregations();
      TupleTag<Object> combineTag = new TupleTag<>(Integer.toString(fieldAggregations.size()));
      FieldAggregation fieldAggregation =
          new FieldAggregation<>(
              fieldsToAggregate, aggregateBaseValues, outputField, fn, combineTag);
      fieldAggregations.add(fieldAggregation);

      return toBuilder()
          .setOutputSchema(getOutputSchema(fieldAggregations))
          .setFieldAggregations(fieldAggregations)
          .build();
    }

    private Schema getOutputSchema(List<FieldAggregation> fieldAggregations) {
      Schema.Builder outputSchema = Schema.builder();
      for (FieldAggregation aggregation : fieldAggregations) {
        outputSchema.addField(aggregation.outputField);
      }
      return outputSchema.build();
    }

    /** Extract a single field from an input {@link Row}. */
    private static class ExtractSingleFieldFunction<OutputT> extends SimpleFunction<Row, OutputT> {
      private final RowSelector rowSelector;
      private final boolean extractBaseValue;
      private final @Nullable RowSelector flatteningSelector;
      private final FieldAggregation fieldAggregation;

      private ExtractSingleFieldFunction(
          Schema inputSchema, boolean extractBaseValue, FieldAggregation fieldAggregation) {
        rowSelector =
            new RowSelectorContainer(inputSchema, fieldAggregation.fieldsToAggregate, true);
        this.extractBaseValue = extractBaseValue;
        flatteningSelector =
            fieldAggregation.needsFlattening
                ? new RowSelectorContainer(
                    fieldAggregation.inputSubSchema,
                    fieldAggregation.flattenedFieldAccessDescriptor,
                    true)
                : null;
        this.fieldAggregation = fieldAggregation;
      }

      @Override
      public OutputT apply(Row row) {
        Row selected = rowSelector.select(row);
        if (fieldAggregation.needsFlattening) {
          selected = flatteningSelector.select(selected);
        }
        if (extractBaseValue
            && selected.getSchema().getField(0).getType().getTypeName().isLogicalType()) {
          return (OutputT) selected.getBaseValue(0, Object.class);
        }
        return selected.getValue(0);
      }
    }

    /** Extract multiple fields from an input {@link Row}. */
    private static class ExtractFieldsFunction extends SimpleFunction<Row, Row> {
      private final RowSelector rowSelector;
      private final FieldAggregation fieldAggregation;

      private ExtractFieldsFunction(Schema inputSchema, FieldAggregation fieldAggregation) {
        rowSelector =
            new RowSelectorContainer(inputSchema, fieldAggregation.fieldsToAggregate, true);
        this.fieldAggregation = fieldAggregation;
      }

      @Override
      public Row apply(Row row) {
        return rowSelector.select(row);
      }
    }

    @Override
    public Object[] createAccumulator() {
      return getComposedCombineFn().createAccumulator();
    }

    @Override
    public Object[] addInput(Object[] accumulator, Row input) {
      return getComposedCombineFn().addInput(accumulator, input);
    }

    @Override
    public Object[] mergeAccumulators(Iterable<Object[]> accumulator) {
      return getComposedCombineFn().mergeAccumulators(accumulator);
    }

    @Override
    public Coder<Object[]> getAccumulatorCoder(CoderRegistry registry, Coder<Row> inputCoder)
        throws CannotProvideCoderException {
      return getComposedCombineFn().getAccumulatorCoder(registry, inputCoder);
    }

    @Override
    public Coder<Row> getDefaultOutputCoder(CoderRegistry registry, Coder<Row> inputCoder) {
      return SchemaCoder.of(getOutputSchema());
    }

    @Override
    public Row extractOutput(Object[] accumulator) {
      // Build a row containing a field for every aggregate that was registered.
      CoCombineResult coCombineResult = getComposedCombineFn().extractOutput(accumulator);
      Row.Builder output = Row.withSchema(getOutputSchema());
      for (FieldAggregation fieldAggregation : getFieldAggregations()) {
        Object aggregate = coCombineResult.get(fieldAggregation.combineTag);
        output.addValue(aggregate);
      }
      return output.build();
    }
  }
}
