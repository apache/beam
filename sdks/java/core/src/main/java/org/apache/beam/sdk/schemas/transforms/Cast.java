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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.utils.SchemaZipFold;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;

/** Set of utilities for casting rows between schemas. */
@AutoValue
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public abstract class Cast<T> extends PTransform<PCollection<T>, PCollection<Row>> {

  public abstract Schema outputSchema();

  public abstract Validator validator();

  public static <T> Cast<T> of(Schema outputSchema, Validator validator) {
    return new AutoValue_Cast<>(outputSchema, validator);
  }

  public static <T> Cast<T> widening(Schema outputSchema) {
    return new AutoValue_Cast<>(outputSchema, Widening.of());
  }

  public static <T> Cast<T> narrowing(Schema outputSchema) {
    return new AutoValue_Cast<>(outputSchema, Narrowing.of());
  }

  /** Describes compatibility errors during casting. */
  @AutoValue
  public abstract static class CompatibilityError implements Serializable {

    public abstract List<String> path();

    public abstract String message();

    public static CompatibilityError create(List<String> path, String message) {
      return new AutoValue_Cast_CompatibilityError(path, message);
    }
  }

  /** Interface for statically validating casts. */
  public interface Validator extends Serializable {
    List<CompatibilityError> apply(Schema input, Schema output);
  }

  /**
   * Widening changes to type that can represent any possible value of the original type.
   *
   * <p>Standard widening conversions:
   *
   * <ul>
   *   <li>BYTE to INT16, INT32, INT64, FLOAT, DOUBLE, DECIMAL
   *   <li>INT16 to INT32, INT64, FLOAT, DOUBLE, DECIMAL
   *   <li>INT32 to INT64, FLOAT, DOUBLE, DECIMAL
   *   <li>INT64 to FLOAT, DOUBLE, DECIMAL
   *   <li>FLOAT to DOUBLE, DECIMAL
   *   <li>DOUBLE to DECIMAL
   * </ul>
   *
   * <p>Row widening:
   *
   * <ul>
   *   <li>wider schema to schema with a subset of fields
   *   <li>non-nullable fields to nullable fields
   * </ul>
   *
   * <p>Widening doesn't lose information about the overall magnitude in following cases:
   *
   * <ul>
   *   <li>integral type to another integral type
   *   <li>BYTE or INT16 to FLOAT, DOUBLE or DECIMAL
   *   <li>INT32 to DOUBLE
   * </ul>
   *
   * <p>Other conversions to may cause loss of precision.
   */
  public static class Widening implements Validator {
    private final Widening.Fold fold = new Widening.Fold();

    public static Widening of() {
      return new Widening();
    }

    @Override
    public String toString() {
      return "Cast.Widening";
    }

    @Override
    public List<CompatibilityError> apply(final Schema input, final Schema output) {
      return fold.apply(input, output);
    }

    private static class Fold extends SchemaZipFold<List<CompatibilityError>> {

      @Override
      public List<CompatibilityError> accumulate(
          List<CompatibilityError> left, List<CompatibilityError> right) {
        return ImmutableList.<CompatibilityError>builder().addAll(left).addAll(right).build();
      }

      @Override
      public List<CompatibilityError> accept(
          Context context, Optional<Field> left, Optional<Field> right) {
        if (!left.isPresent() && !right.isPresent()) {
          return Collections.emptyList();
        } else if (left.isPresent() && !right.isPresent()) {
          return Collections.emptyList();
        } else if (!left.isPresent() && right.isPresent()) {
          return Collections.singletonList(
              CompatibilityError.create(context.path(), "Field is missing in output schema"));
        } else {
          if (left.get().getType().getNullable() && !right.get().getType().getNullable()) {
            return Collections.singletonList(
                CompatibilityError.create(
                    context.path(), "Can't cast nullable field to non-nullable field"));
          }
        }

        return Collections.emptyList();
      }

      @Override
      public List<CompatibilityError> accept(Context context, FieldType input, FieldType output) {
        TypeName inputType = input.getTypeName();
        TypeName outputType = output.getTypeName();

        boolean supertype = outputType.isSupertypeOf(inputType);

        if (isIntegral(inputType) && isDecimal(outputType)) {
          return Collections.emptyList();
        } else if (!supertype) {
          return Collections.singletonList(
              CompatibilityError.create(
                  context.path(), "Can't cast '" + inputType + "' to '" + outputType + "'"));
        }

        return Collections.emptyList();
      }
    }
  }

  /**
   * Narrowing changes type without guarantee to preserve data.
   *
   * <p>Standard narrowing conversions:
   *
   * <ul>
   *   <li>any conversions of {@link Widening}
   *   <li>conversions the opposite to {@link Widening}
   * </ul>
   *
   * <p>Row narrowing
   *
   * <ul>
   *   <li>wider schema to schema with a subset of fields
   *   <li>non-nullable fields to nullable fields
   *   <li>nullable fields to non-nullable fields
   * </ul>
   */
  public static class Narrowing implements Validator {
    private final Narrowing.Fold fold = new Narrowing.Fold();

    public static Narrowing of() {
      return new Narrowing();
    }

    @Override
    public String toString() {
      return "Cast.Narrowing";
    }

    @Override
    public List<CompatibilityError> apply(final Schema input, final Schema output) {
      return fold.apply(input, output);
    }

    private static class Fold extends SchemaZipFold<List<CompatibilityError>> {

      @Override
      public List<CompatibilityError> accumulate(
          List<CompatibilityError> left, List<CompatibilityError> right) {
        return ImmutableList.<CompatibilityError>builder().addAll(left).addAll(right).build();
      }

      @Override
      public List<CompatibilityError> accept(
          Context context, Optional<Field> left, Optional<Field> right) {

        if (!left.isPresent() && right.isPresent()) {
          return Collections.singletonList(
              CompatibilityError.create(context.path(), "Field is missing in output schema"));
        }

        return Collections.emptyList();
      }

      @Override
      public List<CompatibilityError> accept(Context context, FieldType input, FieldType output) {
        TypeName inputType = input.getTypeName();
        TypeName outputType = output.getTypeName();

        boolean supertype = outputType.isSupertypeOf(inputType);
        boolean subtype = outputType.isSubtypeOf(inputType);

        if (isDecimal(inputType) && isIntegral(outputType)) {
          return Collections.emptyList();
        } else if (!supertype && !subtype) {
          return Collections.singletonList(
              CompatibilityError.create(
                  context.path(), "Can't cast '" + inputType + "' to '" + outputType + "'"));
        }

        return Collections.emptyList();
      }
    }
  }

  /** Checks if type is integral. */
  public static boolean isIntegral(TypeName type) {
    return type == TypeName.BYTE
        || type == TypeName.INT16
        || type == TypeName.INT32
        || type == TypeName.INT64;
  }

  /** Checks if type is decimal. */
  public static boolean isDecimal(TypeName type) {
    return type == TypeName.FLOAT || type == TypeName.DOUBLE || type == TypeName.DECIMAL;
  }

  public void verifyCompatibility(Schema inputSchema) {
    List<CompatibilityError> errors = validator().apply(inputSchema, outputSchema());

    if (!errors.isEmpty()) {
      String reason =
          errors.stream()
              .map(x -> Joiner.on('.').join(x.path()) + ": " + x.message())
              .collect(Collectors.joining("\n\t"));

      throw new IllegalArgumentException(
          "Cast isn't compatible using " + validator() + ":\n\t" + reason);
    }
  }

  @Override
  public PCollection<Row> expand(PCollection<T> input) {
    Schema inputSchema = input.getSchema();

    verifyCompatibility(inputSchema);

    return input
        .apply(
            ParDo.of(
                new DoFn<T, Row>() {
                  // TODO: This should be the same as resolved so that Beam knows which fields
                  // are being accessed. Currently Beam only supports wildcard descriptors.
                  // Once https://github.com/apache/beam/issues/18903 is fixed, fix this.
                  @FieldAccess("filterFields")
                  final FieldAccessDescriptor fieldAccessDescriptor =
                      FieldAccessDescriptor.withAllFields();

                  @ProcessElement
                  public void process(
                      @FieldAccess("filterFields") @Element Row input, OutputReceiver<Row> r) {
                    Row output = castRow(input, inputSchema, outputSchema());
                    r.output(output);
                  }
                }))
        .setRowSchema(outputSchema());
  }

  public static Row castRow(Row input, Schema inputSchema, Schema outputSchema) {
    if (input == null) {
      return null;
    }

    Row.Builder output = Row.withSchema(outputSchema);
    for (int i = 0; i < outputSchema.getFieldCount(); i++) {
      Schema.Field outputField = outputSchema.getField(i);

      int fromFieldIdx = inputSchema.indexOf(outputField.getName());
      Schema.Field inputField = inputSchema.getField(fromFieldIdx);

      Object inputValue = input.getValue(fromFieldIdx);
      Object outputValue = castValue(inputValue, inputField.getType(), outputField.getType());

      output.addValue(outputValue);
    }

    return output.build();
  }

  public static Number castNumber(Number value, TypeName input, TypeName output) {
    if (!input.isNumericType()) {
      throw new RuntimeException("Can't cast non-numeric types: " + input);
    }

    if (!output.isNumericType()) {
      throw new RuntimeException("Can't cast numbers to non-numeric type: " + output);
    }

    if (value == null) {
      return null;
    }

    if (input == output) {
      return value;
    }

    switch (output) {
      case BYTE:
        return value.byteValue();

      case INT16:
        return value.shortValue();

      case INT32:
        return value.intValue();

      case INT64:
        return value.longValue();

      case FLOAT:
        return value.floatValue();

      case DOUBLE:
        return value.doubleValue();

      case DECIMAL:
        switch (input) {
          case BYTE:
          case INT16:
          case INT32:
            return new BigDecimal(value.intValue());

          case INT64:
            return new BigDecimal(value.longValue());

          case FLOAT:
          case DOUBLE:
            return new BigDecimal(value.doubleValue());

          default:
            throw new AssertionError("Unexpected numeric type: " + output);
        }

      default:
        throw new AssertionError("Unexpected numeric type: " + output);
    }
  }

  @SuppressWarnings("unchecked")
  public static Object castValue(Object inputValue, FieldType input, FieldType output) {

    TypeName inputType = input.getTypeName();
    TypeName outputType = output.getTypeName();

    if (inputValue == null) {
      return null;
    }

    switch (inputType) {
      case ROW:
        return castRow((Row) inputValue, input.getRowSchema(), output.getRowSchema());

      case ARRAY:
      case ITERABLE:;
        Iterable<Object> inputValues = (Iterable<Object>) inputValue;
        List<Object> outputValues = new ArrayList<>(Iterables.size(inputValues));

        for (Object elem : inputValues) {
          outputValues.add(
              castValue(elem, input.getCollectionElementType(), output.getCollectionElementType()));
        }

        return outputValues;

      case MAP:
        Map<Object, Object> inputMap = (Map<Object, Object>) inputValue;
        Map<Object, Object> outputMap = Maps.newHashMapWithExpectedSize(inputMap.size());

        for (Map.Entry<Object, Object> entry : inputMap.entrySet()) {
          Object outputKey =
              castValue(entry.getKey(), input.getMapKeyType(), output.getMapKeyType());
          Object outputValue =
              castValue(entry.getValue(), input.getMapValueType(), output.getMapValueType());

          outputMap.put(outputKey, outputValue);
        }

        return outputMap;

      default:
        if (inputType.equals(outputType)) {
          return inputValue;
        }

        if (inputType.isNumericType()) {
          return castNumber((Number) inputValue, inputType, outputType);
        } else {
          throw new IllegalArgumentException("input should be array, map, numeric or row");
        }
    }
  }
}
