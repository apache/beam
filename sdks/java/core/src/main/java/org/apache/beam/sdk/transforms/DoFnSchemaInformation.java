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
import java.util.Map;
import javax.annotation.Nullable;
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
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps;

/** Represents information about how a DoFn extracts schemas. */
@Experimental(Kind.SCHEMAS)
@AutoValue
@Internal
@SuppressWarnings({
  "nullness", // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
  "rawtypes"
})
public abstract class DoFnSchemaInformation implements Serializable {
  /**
   * The schema of the @Element parameter. If the Java type does not match the input PCollection but
   * the schemas are compatible, Beam will automatically convert between the Java types.
   */
  public abstract List<SerializableFunction<?, ?>> getElementConverters();

  public abstract List<SerializableFunction<?, ?>> getKeyConvertersProcessElement();

  public abstract List<SerializableFunction<?, ?>> getKeyConvertersOnWindowExpiration();

  public abstract Map<String, List<SerializableFunction<?, ?>>> getKeyConvertersOnTimer();

  public abstract Map<String, List<SerializableFunction<?, ?>>> getKeyConvertersOnTimerFamily();

  @Nullable
  public abstract FieldAccessDescriptor getKeyFieldsDescriptor();

  /** Create an instance. */
  public static DoFnSchemaInformation create() {
    return new AutoValue_DoFnSchemaInformation.Builder()
        .setKeyFieldsDescriptor(null)
        .setElementConverters(Collections.emptyList())
        .setKeyConvertersProcessElement(Collections.emptyList())
        .setKeyConvertersOnWindowExpiration(Collections.emptyList())
        .setKeyConvertersOnTimer(Collections.emptyMap())
        .setKeyConvertersOnTimerFamily(Collections.emptyMap())
        .build();
  }

  /** The builder object. */
  @AutoValue.Builder
  public abstract static class Builder {
    abstract Builder setElementConverters(List<SerializableFunction<?, ?>> converters);

    abstract Builder setKeyConvertersProcessElement(List<SerializableFunction<?, ?>> converters);

    abstract Builder setKeyConvertersOnWindowExpiration(
        List<SerializableFunction<?, ?>> converters);

    abstract Builder setKeyConvertersOnTimer(
        Map<String, List<SerializableFunction<?, ?>>> converters);

    abstract Builder setKeyConvertersOnTimerFamily(
        Map<String, List<SerializableFunction<?, ?>>> converters);

    abstract Builder setKeyFieldsDescriptor(@Nullable FieldAccessDescriptor fieldAccessDescriptor);

    abstract DoFnSchemaInformation build();
  }

  public abstract Builder toBuilder();

  public DoFnSchemaInformation withStateKeyFieldsDescriptor(FieldAccessDescriptor keyDescriptor) {
    return toBuilder().setKeyFieldsDescriptor(keyDescriptor).build();
  }

  List<SerializableFunction<?, ?>> addConverterParameter(
      List<SerializableFunction<?, ?>> existingConverters,
      Schema schema,
      Schema outputSchema,
      SerializableFunction<?, Row> toRowFunction,
      SerializableFunction<Row, ?> fromRowFunction,
      FieldAccessDescriptor fieldAccessDescriptor,
      boolean unbox) {
    List<SerializableFunction<?, ?>> converters =
        ImmutableList.<SerializableFunction<?, ?>>builder()
            .addAll(existingConverters)
            .add(
                ConversionFunction.of(
                    schema,
                    toRowFunction,
                    fromRowFunction,
                    fieldAccessDescriptor,
                    outputSchema,
                    unbox))
            .build();
    return converters;
  }

  List<SerializableFunction<?, ?>> addUnboxPrimitiveParameter(
      List<SerializableFunction<?, ?>> existingConverters,
      Schema schema,
      Schema outputSchema,
      SerializableFunction<?, Row> toRowFunction,
      FieldAccessDescriptor fieldAccessDescriptor,
      TypeDescriptor<?> elementT) {
    if (outputSchema.getFieldCount() != 1) {
      throw new RuntimeException("Parameter has no schema and the input is not a simple type.");
    }
    FieldType fieldType = outputSchema.getField(0).getType();
    if (fieldType.getTypeName().isCompositeType()) {
      throw new RuntimeException("Parameter has no schema and the input is not a primitive type.");
    }

    List<SerializableFunction<?, ?>> converters =
        ImmutableList.<SerializableFunction<?, ?>>builder()
            .addAll(existingConverters)
            .add(
                UnboxingConversionFunction.of(
                    schema, toRowFunction, fieldAccessDescriptor, outputSchema, elementT))
            .build();
    return converters;
  }

  public DoFnSchemaInformation withProcessElementSchemaKeyParameter(
      Schema keySchema, SchemaCoder<?> parameterCoder, boolean unbox) {
    List<SerializableFunction<?, ?>> converters =
        addConverterParameter(
            getKeyConvertersProcessElement(),
            keySchema,
            keySchema,
            SerializableFunctions.identity(),
            parameterCoder.getFromRowFunction(),
            FieldAccessDescriptor.withAllFields(),
            unbox);
    return toBuilder().setKeyConvertersProcessElement(converters).build();
  }

  public DoFnSchemaInformation withProcessElementUnboxPrimitiveKeyParameter(
      Schema keySchema, TypeDescriptor<?> elementT) {
    List<SerializableFunction<?, ?>> converters =
        addUnboxPrimitiveParameter(
            getKeyConvertersProcessElement(),
            keySchema,
            keySchema,
            SerializableFunctions.identity(),
            FieldAccessDescriptor.withAllFields(),
            elementT);
    return toBuilder().setKeyConvertersProcessElement(converters).build();
  }

  public DoFnSchemaInformation withOnWindowExpirationSchemaKeyParameter(
      Schema keySchema, SchemaCoder<?> parameterCoder, boolean unbox) {
    List<SerializableFunction<?, ?>> converters =
        addConverterParameter(
            getKeyConvertersOnWindowExpiration(),
            keySchema,
            keySchema,
            SerializableFunctions.identity(),
            parameterCoder.getFromRowFunction(),
            FieldAccessDescriptor.withAllFields(),
            unbox);
    return toBuilder().setKeyConvertersOnWindowExpiration(converters).build();
  }

  public DoFnSchemaInformation withOnWindowExpirationUnboxPrimitiveKeyParameter(
      Schema keySchema, TypeDescriptor<?> elementT) {
    List<SerializableFunction<?, ?>> converters =
        addUnboxPrimitiveParameter(
            getKeyConvertersOnWindowExpiration(),
            keySchema,
            keySchema,
            SerializableFunctions.identity(),
            FieldAccessDescriptor.withAllFields(),
            elementT);
    return toBuilder().setKeyConvertersOnWindowExpiration(converters).build();
  }

  public DoFnSchemaInformation withOnTimerSchemaKeyParameter(
      String timerId, Schema keySchema, SchemaCoder<?> parameterCoder, boolean unbox) {
    Map<String, List<SerializableFunction<?, ?>>> onTimerConverters =
        Maps.newHashMap(getKeyConvertersOnTimer());
    List<SerializableFunction<?, ?>> converters =
        addConverterParameter(
            onTimerConverters.computeIfAbsent(timerId, t -> Lists.newArrayList()),
            keySchema,
            keySchema,
            SerializableFunctions.identity(),
            parameterCoder.getFromRowFunction(),
            FieldAccessDescriptor.withAllFields(),
            unbox);
    onTimerConverters.put(timerId, converters);
    return toBuilder().setKeyConvertersOnTimer(onTimerConverters).build();
  }

  public DoFnSchemaInformation withOnTimerUnboxPrimitiveKeyParameter(
      String timerId, Schema keySchema, TypeDescriptor<?> elementT) {
    Map<String, List<SerializableFunction<?, ?>>> onTimerConverters =
        Maps.newHashMap(getKeyConvertersOnTimer());
    List<SerializableFunction<?, ?>> converters =
        addUnboxPrimitiveParameter(
            onTimerConverters.computeIfAbsent(timerId, t -> Lists.newArrayList()),
            keySchema,
            keySchema,
            SerializableFunctions.identity(),
            FieldAccessDescriptor.withAllFields(),
            elementT);
    onTimerConverters.put(timerId, converters);
    return toBuilder().setKeyConvertersOnTimer(onTimerConverters).build();
  }

  public DoFnSchemaInformation withOnTimerFamilySchemaKeyParameter(
      String timerFamily, Schema keySchema, SchemaCoder<?> parameterCoder, boolean unbox) {
    Map<String, List<SerializableFunction<?, ?>>> onTimerFamilyConverters =
        Maps.newHashMap(getKeyConvertersOnTimerFamily());
    List<SerializableFunction<?, ?>> converters =
        addConverterParameter(
            onTimerFamilyConverters.computeIfAbsent(timerFamily, t -> Lists.newArrayList()),
            keySchema,
            keySchema,
            SerializableFunctions.identity(),
            parameterCoder.getFromRowFunction(),
            FieldAccessDescriptor.withAllFields(),
            unbox);
    onTimerFamilyConverters.put(timerFamily, converters);
    return toBuilder().setKeyConvertersOnTimerFamily(onTimerFamilyConverters).build();
  }

  public DoFnSchemaInformation withOnTimerFamilyUnboxPrimitiveKeyParameter(
      String timerFamily, Schema keySchema, TypeDescriptor<?> elementT) {
    Map<String, List<SerializableFunction<?, ?>>> onTimerFamilyConverters =
        Maps.newHashMap(getKeyConvertersOnTimerFamily());
    List<SerializableFunction<?, ?>> converters =
        addUnboxPrimitiveParameter(
            onTimerFamilyConverters.computeIfAbsent(timerFamily, t -> Lists.newArrayList()),
            keySchema,
            keySchema,
            SerializableFunctions.identity(),
            FieldAccessDescriptor.withAllFields(),
            elementT);
    onTimerFamilyConverters.put(timerFamily, converters);
    return toBuilder().setKeyConvertersOnTimerFamily(onTimerFamilyConverters).build();
  }

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
  public DoFnSchemaInformation withSelectFromSchemaElementParameter(
      SchemaCoder<?> inputCoder,
      FieldAccessDescriptor selectDescriptor,
      Schema selectOutputSchema,
      SchemaCoder<?> parameterCoder,
      boolean unbox) {
    List<SerializableFunction<?, ?>> converters =
        addConverterParameter(
            getElementConverters(),
            inputCoder.getSchema(),
            selectOutputSchema,
            inputCoder.getToRowFunction(),
            parameterCoder.getFromRowFunction(),
            selectDescriptor,
            unbox);
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
  public DoFnSchemaInformation withUnboxPrimitiveElementParameter(
      SchemaCoder inputCoder,
      FieldAccessDescriptor selectDescriptor,
      Schema selectOutputSchema,
      TypeDescriptor<?> elementT) {
    List<SerializableFunction<?, ?>> converters =
        addUnboxPrimitiveParameter(
            getElementConverters(),
            inputCoder.getSchema(),
            selectOutputSchema,
            inputCoder.getToRowFunction(),
            selectDescriptor,
            elementT);
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
    private final @Nullable RowSelector rowSelector;

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
