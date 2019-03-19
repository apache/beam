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
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.FieldTypeDescriptors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.utils.SelectHelpers;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.CombineFns;
import org.apache.beam.sdk.transforms.CombineFns.CoCombineResult;
import org.apache.beam.sdk.transforms.CombineFns.ComposedCombineFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;

/** This is the builder used by {@link Group} to build up a composed {@link CombineFn}. */
@Experimental(Kind.SCHEMAS)
class SchemaAggregateFn {
  static <T> Inner<T> create() {
    return new AutoValue_SchemaAggregateFn_Inner.Builder<T>()
        .setFieldAggregations(Lists.newArrayList())
        .build();
  }

  /** Implementation of {@link #create}. */
  @AutoValue
  abstract static class Inner<T> extends CombineFn<T, Object[], Row> {
    // Represents an aggregation of one or more fields.
    static class FieldAggregation<FieldT, AccumT, OutputT> implements Serializable {
      FieldAccessDescriptor fieldsToAggregate;
      // The specification of the output field.
      private final Field outputField;
      // The combine function.
      private final CombineFn<FieldT, AccumT, OutputT> fn;
      // The TupleTag identifying this aggregation element in the composed combine fn.
      private final TupleTag<Object> combineTag;
      // The schema corresponding to the the subset of input fields being aggregated.
      @Nullable private final Schema inputSubSchema;
      // The flattened version of inputSubSchema.
      @Nullable private final Schema unnestedInputSubSchema;
      // The output schema resulting from the aggregation.
      private final Schema aggregationSchema;
      private final boolean needsUnnesting;

      FieldAggregation(
          FieldAccessDescriptor fieldsToAggregate,
          Field outputField,
          CombineFn<FieldT, AccumT, OutputT> fn,
          TupleTag<Object> combineTag) {
        this(
            fieldsToAggregate,
            outputField,
            fn,
            combineTag,
            Schema.builder().addField(outputField).build(),
            null);
      }

      FieldAggregation(
          FieldAccessDescriptor fieldsToAggregate,
          Field outputField,
          CombineFn<FieldT, AccumT, OutputT> fn,
          TupleTag<Object> combineTag,
          Schema aggregationSchema,
          @Nullable Schema inputSchema) {
        if (inputSchema != null) {
          this.fieldsToAggregate = fieldsToAggregate.resolve(inputSchema);
          this.inputSubSchema = SelectHelpers.getOutputSchema(inputSchema, this.fieldsToAggregate);
          this.unnestedInputSubSchema = Unnest.getUnnestedSchema(inputSubSchema);
          this.needsUnnesting = !inputSchema.equals(unnestedInputSubSchema);
        } else {
          this.fieldsToAggregate = fieldsToAggregate;
          this.inputSubSchema = null;
          this.unnestedInputSubSchema = null;
          this.needsUnnesting = false;
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
            fieldsToAggregate, outputField, fn, combineTag, aggregationSchema, schema);
      }
    }

    abstract Builder<T> toBuilder();

    @AutoValue.Builder
    abstract static class Builder<T> {
      abstract Builder<T> setInputSchema(@Nullable Schema inputSchema);

      abstract Builder<T> setOutputSchema(@Nullable Schema outputSchema);

      abstract Builder<T> setComposedCombineFn(@Nullable ComposedCombineFn<T> composedCombineFn);

      abstract Builder<T> setFieldAggregations(List<FieldAggregation> fieldAggregations);

      abstract Inner<T> build();
    }

    abstract @Nullable Schema getInputSchema();

    abstract @Nullable Schema getOutputSchema();

    abstract @Nullable ComposedCombineFn<T> getComposedCombineFn();

    abstract List<FieldAggregation> getFieldAggregations();

    /** Once the schema is known, this function is called by the {@link Group} transform. */
    Inner<T> withSchema(Schema inputSchema, SerializableFunction<T, Row> toRowFunction) {
      List<FieldAggregation> fieldAggregations =
          getFieldAggregations().stream()
              .map(f -> f.resolve(inputSchema))
              .collect(Collectors.toList());

      ComposedCombineFn<T> composedCombineFn = null;
      for (int i = 0; i < fieldAggregations.size(); ++i) {
        FieldAggregation fieldAggregation = fieldAggregations.get(i);
        SimpleFunction<T, ?> extractFunction;
        Coder extractOutputCoder;
        if (fieldAggregation.unnestedInputSubSchema.getFieldCount() == 1) {
          extractFunction = new ExtractSingleFieldFunction<>(fieldAggregation, toRowFunction);
          extractOutputCoder =
              RowCoder.coderForFieldType(
                  fieldAggregation.unnestedInputSubSchema.getField(0).getType());
        } else {
          extractFunction = new ExtractFieldsFunction<>(fieldAggregation, toRowFunction);
          extractOutputCoder = RowCoder.of(fieldAggregation.inputSubSchema);
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
    <CombineInputT, AccumT, CombineOutputT> Inner<T> aggregateFields(
        FieldAccessDescriptor fieldsToAggregate,
        CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
        String outputFieldName) {
      return aggregateFields(
          fieldsToAggregate,
          fn,
          Field.of(outputFieldName, FieldTypeDescriptors.fieldTypeForJavaType(fn.getOutputType())));
    }

    /** Aggregate all values of a set of fields into an output field. */
    <CombineInputT, AccumT, CombineOutputT> Inner<T> aggregateFields(
        FieldAccessDescriptor fieldsToAggregate,
        CombineFn<CombineInputT, AccumT, CombineOutputT> fn,
        Field outputField) {
      List<FieldAggregation> fieldAggregations = getFieldAggregations();
      TupleTag<Object> combineTag = new TupleTag<>(Integer.toString(fieldAggregations.size()));
      FieldAggregation fieldAggregation =
          new FieldAggregation<>(fieldsToAggregate, outputField, fn, combineTag);
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
    private static class ExtractSingleFieldFunction<InputT, OutputT>
        extends SimpleFunction<InputT, OutputT> {
      private final FieldAggregation fieldAggregation;
      private final SerializableFunction<InputT, Row> toRowFunction;

      private ExtractSingleFieldFunction(
          FieldAggregation fieldAggregation, SerializableFunction<InputT, Row> toRowFunction) {
        this.fieldAggregation = fieldAggregation;
        this.toRowFunction = toRowFunction;
      }

      @Override
      public OutputT apply(InputT input) {
        Row row = toRowFunction.apply(input);
        Row selected =
            SelectHelpers.selectRow(
                row,
                fieldAggregation.fieldsToAggregate,
                row.getSchema(),
                fieldAggregation.inputSubSchema);
        if (fieldAggregation.needsUnnesting) {
          selected = Unnest.unnestRow(selected, fieldAggregation.unnestedInputSubSchema);
        }
        return selected.getValue(0);
      }
    }

    /** Extract multiple fields from an input {@link Row}. */
    private static class ExtractFieldsFunction<T> extends SimpleFunction<T, Row> {
      private FieldAggregation fieldAggregation;
      private SerializableFunction<T, Row> toRowFunction;

      private ExtractFieldsFunction(
          FieldAggregation fieldAggregation, SerializableFunction<T, Row> toRowFunction) {
        this.fieldAggregation = fieldAggregation;
        this.toRowFunction = toRowFunction;
      }

      @Override
      public Row apply(T input) {
        Row row = toRowFunction.apply(input);
        return SelectHelpers.selectRow(
            row,
            fieldAggregation.fieldsToAggregate,
            row.getSchema(),
            fieldAggregation.inputSubSchema);
      }
    }

    @Override
    public Object[] createAccumulator() {
      return getComposedCombineFn().createAccumulator();
    }

    @Override
    public Object[] addInput(Object[] accumulator, T input) {
      return getComposedCombineFn().addInput(accumulator, input);
    }

    @Override
    public Object[] mergeAccumulators(Iterable<Object[]> accumulator) {
      return getComposedCombineFn().mergeAccumulators(accumulator);
    }

    @Override
    public Coder<Object[]> getAccumulatorCoder(CoderRegistry registry, Coder<T> inputCoder)
        throws CannotProvideCoderException {
      return getComposedCombineFn().getAccumulatorCoder(registry, inputCoder);
    }

    @Override
    public Coder<Row> getDefaultOutputCoder(CoderRegistry registry, Coder<T> inputCoder) {
      return SchemaCoder.of(
          getOutputSchema(), SerializableFunctions.identity(), SerializableFunctions.identity());
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
