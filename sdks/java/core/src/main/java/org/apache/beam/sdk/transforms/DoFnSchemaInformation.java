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
package org.apache.beam.sdk.transforms;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.utils.ByteBuddyUtils.DefaultTypeConversionsFactory;
import org.apache.beam.sdk.schemas.utils.ConvertHelpers;
import org.apache.beam.sdk.schemas.utils.RowSelector;
import org.apache.beam.sdk.schemas.utils.SelectHelpers.RowSelectorContainer;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

/** Represents information about how a DoFn extracts schemas. */
@Experimental(Kind.SCHEMAS)
@AutoValue
@Internal
public abstract class DoFnSchemaInformation implements Serializable {
  /**
   * The schema of the @Element parameter. If the Java type does not match the input PCollection but
   * the schemas are compatible, Beam will automatically convert between the Java types.
   */
  public abstract List<SerializableFunction<?, ?>> getElementConverters();

  /** Create an instance. */
  public static DoFnSchemaInformation create() {
    return new AutoValue_DoFnSchemaInformation.Builder()
        .setElementConverters(Collections.emptyList())
        .build();
  }

  /** The builder object. */
  @AutoValue.Builder
  public abstract static class Builder {
    abstract Builder setElementConverters(List<SerializableFunction<?, ?>> converters);

    abstract DoFnSchemaInformation build();
  }

  public abstract Builder toBuilder();

  /**
   * Specified a parameter that is a selection from an input schema (specified using FieldAccess).
   * This method is called when the input parameter itself has a schema. The input parameter does
   * not need to be a Row. If it is a type with a compatible registered schema, then the conversion
   * will be done automatically.
   *
   * @param inputCoder The coder for the ParDo's input elements.
   * @param selectDescriptor The descriptor describing which field to select.
   * @param selectOutputSchema The schema of the selected parameter.
   * @param parameterCoder The coder for the input parameter to the method.
   * @param unbox If unbox is true, then the select result is a 1-field schema that needs to be
   *     unboxed.
   * @return
   */
  DoFnSchemaInformation withSelectFromSchemaParameter(
      SchemaCoder<?> inputCoder,
      FieldAccessDescriptor selectDescriptor,
      Schema selectOutputSchema,
      SchemaCoder<?> parameterCoder,
      boolean unbox) {
    List<SerializableFunction<?, ?>> converters =
        ImmutableList.<SerializableFunction<?, ?>>builder()
            .addAll(getElementConverters())
            .add(
                ConversionFunction.of(
                    inputCoder.getSchema(),
                    inputCoder.getToRowFunction(),
                    parameterCoder.getFromRowFunction(),
                    selectDescriptor,
                    selectOutputSchema,
                    unbox))
            .build();

    return toBuilder().setElementConverters(converters).build();
  }

  /**
   * Specified a parameter that is a selection from an input schema (specified using FieldAccess).
   * This method is called when the input parameter is a Java type that does not itself have a
   * schema, e.g. long, or String. In this case we expect the selection predicate to return a
   * single-field row with a field of the output type.
   *
   * @param inputCoder The coder for the ParDo's input elements.
   * @param selectDescriptor The descriptor describing which field to select.
   * @param selectOutputSchema The schema of the selected parameter.
   * @param elementT The type of the method's input parameter.
   * @return
   */
  DoFnSchemaInformation withUnboxPrimitiveParameter(
      SchemaCoder inputCoder,
      FieldAccessDescriptor selectDescriptor,
      Schema selectOutputSchema,
      TypeDescriptor<?> elementT) {
    if (selectOutputSchema.getFieldCount() != 1) {
      throw new RuntimeException("Parameter has no schema and the input is not a simple type.");
    }
    FieldType fieldType = selectOutputSchema.getField(0).getType();
    if (fieldType.getTypeName().isCompositeType()) {
      throw new RuntimeException("Parameter has no schema and the input is not a primitive type.");
    }

    List<SerializableFunction<?, ?>> converters =
        ImmutableList.<SerializableFunction<?, ?>>builder()
            .addAll(getElementConverters())
            .add(
                UnboxingConversionFunction.of(
                    inputCoder.getSchema(),
                    inputCoder.getToRowFunction(),
                    selectDescriptor,
                    selectOutputSchema,
                    elementT))
            .build();

    return toBuilder().setElementConverters(converters).build();
  }

  private static class ConversionFunction<InputT, OutputT>
      implements SerializableFunction<InputT, OutputT> {
    private final Schema inputSchema;
    private final SerializableFunction<InputT, Row> toRowFunction;
    private final SerializableFunction<Row, OutputT> fromRowFunction;
    private final FieldAccessDescriptor selectDescriptor;
    private final Schema selectOutputSchema;
    private final boolean unbox;
    private final RowSelector rowSelector;

    private ConversionFunction(
        Schema inputSchema,
        SerializableFunction<InputT, Row> toRowFunction,
        SerializableFunction<Row, OutputT> fromRowFunction,
        FieldAccessDescriptor selectDescriptor,
        Schema selectOutputSchema,
        boolean unbox) {
      this.inputSchema = inputSchema;
      this.toRowFunction = toRowFunction;
      this.fromRowFunction = fromRowFunction;
      this.selectDescriptor = selectDescriptor;
      this.selectOutputSchema = selectOutputSchema;
      this.unbox = unbox;
      this.rowSelector = new RowSelectorContainer(inputSchema, selectDescriptor, true);
    }

    public static <InputT, OutputT> ConversionFunction of(
        Schema inputSchema,
        SerializableFunction<InputT, Row> toRowFunction,
        SerializableFunction<Row, OutputT> fromRowFunction,
        FieldAccessDescriptor selectDescriptor,
        Schema selectOutputSchema,
        boolean unbox) {
      return new ConversionFunction<>(
          inputSchema, toRowFunction, fromRowFunction, selectDescriptor, selectOutputSchema, unbox);
    }

    @Override
    public OutputT apply(InputT input) {
      Row row = toRowFunction.apply(input);
      Row selected = rowSelector.select(row);
      if (unbox) {
        selected = selected.getRow(0);
      }
      return fromRowFunction.apply(selected);
    }
  }

  /**
   * This function is used when the schema is a singleton schema containing a single primitive field
   * and the Java type we are converting to is that of the primitive field.
   */
  private static class UnboxingConversionFunction<InputT, OutputT>
      implements SerializableFunction<InputT, OutputT> {
    private final Schema inputSchema;
    private final SerializableFunction<InputT, Row> toRowFunction;
    private final FieldAccessDescriptor selectDescriptor;
    private final Schema selectOutputSchema;
    private final FieldType primitiveType;
    private final TypeDescriptor<?> primitiveOutputType;
    private transient SerializableFunction<InputT, OutputT> conversionFunction;
    private final RowSelector rowSelector;

    private UnboxingConversionFunction(
        Schema inputSchema,
        SerializableFunction<InputT, Row> toRowFunction,
        FieldAccessDescriptor selectDescriptor,
        Schema selectOutputSchema,
        TypeDescriptor<?> primitiveOutputType) {
      this.inputSchema = inputSchema;
      this.toRowFunction = toRowFunction;
      this.selectDescriptor = selectDescriptor;
      this.selectOutputSchema = selectOutputSchema;
      this.primitiveType = selectOutputSchema.getField(0).getType();
      this.primitiveOutputType = primitiveOutputType;
      this.rowSelector = new RowSelectorContainer(inputSchema, selectDescriptor, true);
    }

    public static <InputT, OutputT> UnboxingConversionFunction of(
        Schema inputSchema,
        SerializableFunction<InputT, Row> toRowFunction,
        FieldAccessDescriptor selectDescriptor,
        Schema selectOutputSchema,
        TypeDescriptor<?> primitiveOutputType) {
      return new UnboxingConversionFunction<>(
          inputSchema, toRowFunction, selectDescriptor, selectOutputSchema, primitiveOutputType);
    }

    @Override
    public OutputT apply(InputT input) {
      Row row = toRowFunction.apply(input);
      Row selected = rowSelector.select(row);
      return getConversionFunction().apply(selected.getValue(0));
    }

    private SerializableFunction<InputT, OutputT> getConversionFunction() {
      if (conversionFunction == null) {
        conversionFunction =
            (SerializableFunction<InputT, OutputT>)
                ConvertHelpers.getConvertPrimitive(
                    primitiveType, primitiveOutputType, new DefaultTypeConversionsFactory());
      }
      return conversionFunction;
    }
  }
}
