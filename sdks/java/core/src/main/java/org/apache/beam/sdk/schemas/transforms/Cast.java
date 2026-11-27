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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.utils.SchemaZipFold;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowWithStorage;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.Cache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.checkerframework.checker.nullness.qual.Nullable;

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

  private static final class SchemaPair {
    final Schema inputSchema;
    final Schema outputSchema;
    final int hash;

    SchemaPair(Schema inputSchema, Schema outputSchema) {
      this.inputSchema = inputSchema;
      this.outputSchema = outputSchema;
      int h = inputSchema.hashCode();
      h = 31 * h + outputSchema.hashCode();
      h = 31 * h + Objects.hashCode(outputSchema.getUUID());
      this.hash = h;
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    @SuppressWarnings("ReferenceEquality")
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof SchemaPair)) {
        return false;
      }
      SchemaPair that = (SchemaPair) o;
      return (inputSchema == that.inputSchema || inputSchema.equals(that.inputSchema))
          && (outputSchema == that.outputSchema || outputSchema.equals(that.outputSchema))
          && Objects.equals(outputSchema.getUUID(), that.outputSchema.getUUID());
    }
  }

  private static final Cache<SchemaPair, RowCastPlan> CAST_PLAN_CACHE =
      CacheBuilder.newBuilder().maximumSize(256).build();

  private interface ValueCaster {
    @Nullable Object cast(@Nullable Object value);
  }

  private static final class RowCastPlan {
    final Schema outputSchema;
    final int[] srcIndices;
    final @Nullable ValueCaster[] fieldCasters;
    final String @Nullable [] nonNullFieldNames;
    final boolean isRewrapOnly;
    final boolean isExactIdentity;
    final boolean hasNullChecks;

    RowCastPlan(
        Schema outputSchema,
        int[] srcIndices,
        @Nullable ValueCaster[] fieldCasters,
        String @Nullable [] nonNullFieldNames,
        boolean isRewrapOnly,
        boolean isExactIdentity,
        boolean hasNullChecks) {
      this.outputSchema = outputSchema;
      this.srcIndices = srcIndices;
      this.fieldCasters = fieldCasters;
      this.nonNullFieldNames = nonNullFieldNames;
      this.isRewrapOnly = isRewrapOnly;
      this.isExactIdentity = isExactIdentity;
      this.hasNullChecks = hasNullChecks;
    }

    Row cast(Row input) {
      if (isExactIdentity) {
        return input;
      }
      int outCount = srcIndices.length;
      if (isRewrapOnly) {
        if (input instanceof RowWithStorage) {
          @Nullable Object[] raw = ((RowWithStorage) input).getRawArray();
          if (raw != null) {
            return Row.attachValues(outputSchema, raw);
          }
          return Row.attachValues(outputSchema, input.getValues());
        }
        Object[] out = new Object[outCount];
        for (int i = 0; i < outCount; i++) {
          out[i] = input.getValue(i);
        }
        return Row.attachValues(outputSchema, out);
      }

      @Nullable Object[] out = new Object[outCount];
      @Nullable Object[] raw =
          input instanceof RowWithStorage ? ((RowWithStorage) input).getRawArray() : null;
      for (int i = 0; i < outCount; i++) {
        Object v = raw != null ? raw[srcIndices[i]] : input.getValue(srcIndices[i]);
        if (v == null) {
          if (hasNullChecks && nonNullFieldNames[i] != null) {
            throw new IllegalArgumentException(
                "Field " + nonNullFieldNames[i] + " is not nullable.");
          }
          out[i] = null;
        } else {
          ValueCaster caster = fieldCasters[i];
          out[i] = caster != null ? caster.cast(v) : v;
        }
      }
      return Row.attachValues(outputSchema, out);
    }

    static RowCastPlan compile(Schema inputSchema, Schema outputSchema) {
      int outCount = outputSchema.getFieldCount();
      int[] srcIndices = new int[outCount];
      @Nullable ValueCaster[] fieldCasters = new ValueCaster[outCount];
      String @Nullable [] nonNullFieldNames = null;
      boolean sameOrder = inputSchema.getFieldCount() == outCount;
      boolean anyCaster = false;
      boolean hasNullChecks = false;

      for (int i = 0; i < outCount; i++) {
        Schema.Field outputField = outputSchema.getField(i);
        int fromIdx = inputSchema.indexOf(outputField.getName());
        srcIndices[i] = fromIdx;
        if (fromIdx != i) {
          sameOrder = false;
        }
        Schema.Field inputField = inputSchema.getField(fromIdx);
        FieldType inType = inputField.getType();
        FieldType outType = outputField.getType();
        if (inType.getNullable() && !outType.getNullable()) {
          if (nonNullFieldNames == null) {
            nonNullFieldNames = new String[outCount];
          }
          nonNullFieldNames[i] = outputField.getName();
          hasNullChecks = true;
        }
        ValueCaster caster = compileValueCaster(inType, outType);
        fieldCasters[i] = caster;
        if (caster != null) {
          anyCaster = true;
        }
      }
      boolean rewrapOnly = sameOrder && !anyCaster && !hasNullChecks;
      boolean exactIdentity =
          rewrapOnly
              && inputSchema.equals(outputSchema)
              && Objects.equals(inputSchema.getUUID(), outputSchema.getUUID());
      return new RowCastPlan(
          outputSchema,
          srcIndices,
          fieldCasters,
          nonNullFieldNames != null ? nonNullFieldNames : new String[0],
          rewrapOnly,
          exactIdentity,
          hasNullChecks);
    }

    @SuppressWarnings("unchecked")
    private static @Nullable ValueCaster compileValueCaster(FieldType input, FieldType output) {
      TypeName inputType = input.getTypeName();
      TypeName outputType = output.getTypeName();
      switch (inputType) {
        case ROW:
          RowCastPlan nestedPlan = compile(input.getRowSchema(), output.getRowSchema());
          if (nestedPlan.isExactIdentity) {
            return null;
          }
          return value -> value == null ? null : nestedPlan.cast((Row) value);
        case ARRAY:
        case ITERABLE:
          {
            FieldType inElem = input.getCollectionElementType();
            FieldType outElem = output.getCollectionElementType();
            ValueCaster elemCaster = compileValueCaster(inElem, outElem);
            boolean checkElemNonNull = inElem.getNullable() && !outElem.getNullable();
            if (elemCaster == null && !checkElemNonNull && inputType == outputType) {
              return null;
            }
            return value -> {
              if (value == null) {
                return null;
              }
              Iterable<Object> inputValues = (Iterable<Object>) value;
              List<Object> outputValues =
                  inputValues instanceof Collection
                      ? new ArrayList<>(((Collection<?>) inputValues).size())
                      : new ArrayList<>();
              for (Object elem : inputValues) {
                if (elem == null) {
                  if (checkElemNonNull) {
                    throw new IllegalArgumentException(
                        "Cannot cast null element to non-nullable collection element.");
                  }
                  outputValues.add(null);
                } else {
                  outputValues.add(elemCaster != null ? elemCaster.cast(elem) : elem);
                }
              }
              return outputValues;
            };
          }
        case MAP:
          {
            FieldType inKey = input.getMapKeyType();
            FieldType outKey = output.getMapKeyType();
            FieldType inVal = input.getMapValueType();
            FieldType outVal = output.getMapValueType();
            ValueCaster keyCaster = compileValueCaster(inKey, outKey);
            ValueCaster valCaster = compileValueCaster(inVal, outVal);
            boolean checkKeyNonNull = inKey.getNullable() && !outKey.getNullable();
            boolean checkValNonNull = inVal.getNullable() && !outVal.getNullable();
            if (keyCaster == null && valCaster == null && !checkKeyNonNull && !checkValNonNull) {
              return null;
            }
            return value -> {
              if (value == null) {
                return null;
              }
              Map<Object, Object> inputMap = (Map<Object, Object>) value;
              Map<Object, Object> outputMap = Maps.newHashMapWithExpectedSize(inputMap.size());
              for (Map.Entry<Object, Object> entry : inputMap.entrySet()) {
                Object k = entry.getKey();
                Object v = entry.getValue();
                Object outK;
                if (k == null) {
                  if (checkKeyNonNull) {
                    throw new IllegalArgumentException(
                        "Cannot cast null map key to non-nullable key type.");
                  }
                  outK = null;
                } else {
                  outK = keyCaster == null ? k : keyCaster.cast(k);
                }
                Object outV;
                if (v == null) {
                  if (checkValNonNull) {
                    throw new IllegalArgumentException(
                        "Cannot cast null map value to non-nullable value type.");
                  }
                  outV = null;
                } else {
                  outV = valCaster == null ? v : valCaster.cast(v);
                }
                outputMap.put(outK, outV);
              }
              return outputMap;
            };
          }
        default:
          if (inputType == outputType) {
            return null;
          }
          if (inputType.isNumericType() && outputType.isNumericType()) {
            return compileNumericCaster(inputType, outputType);
          }
          return value -> castValue(value, input, output);
      }
    }

    private static ValueCaster compileNumericCaster(TypeName inputType, TypeName outputType) {
      switch (outputType) {
        case BYTE:
          return v -> v == null ? null : ((Number) v).byteValue();
        case INT16:
          return v -> v == null ? null : ((Number) v).shortValue();
        case INT32:
          return v -> v == null ? null : ((Number) v).intValue();
        case INT64:
          return v -> v == null ? null : ((Number) v).longValue();
        case FLOAT:
          return v -> v == null ? null : ((Number) v).floatValue();
        case DOUBLE:
          return v -> v == null ? null : ((Number) v).doubleValue();
        case DECIMAL:
          switch (inputType) {
            case BYTE:
            case INT16:
            case INT32:
              return v -> v == null ? null : BigDecimal.valueOf(((Number) v).intValue());
            case INT64:
              return v -> v == null ? null : BigDecimal.valueOf(((Number) v).longValue());
            case FLOAT:
            case DOUBLE:
              // Preserve exact binary IEEE-754 representation (matches BigDecimal(double)).
              return v -> v == null ? null : new BigDecimal(((Number) v).doubleValue());
            default:
              throw new AssertionError("Unexpected numeric type: " + outputType);
          }
        default:
          throw new AssertionError("Unexpected numeric type: " + outputType);
      }
    }
  }

  private static RowCastPlan getCastPlan(Schema inputSchema, Schema outputSchema) {
    SchemaPair key = new SchemaPair(inputSchema, outputSchema);
    RowCastPlan plan = CAST_PLAN_CACHE.getIfPresent(key);
    if (plan == null) {
      plan = RowCastPlan.compile(inputSchema, outputSchema);
      CAST_PLAN_CACHE.put(key, plan);
    }
    return plan;
  }

  @Override
  public PCollection<Row> expand(PCollection<T> input) {
    Schema inputSchema = input.getSchema();
    Schema targetSchema = outputSchema();

    verifyCompatibility(inputSchema);

    @Nullable SerializableFunction<T, Row> toRowFn =
        org.apache.beam.sdk.schemas.SchemaUtils.isIdentityFunction(input.getToRowFunction())
            ? null
            : input.getToRowFunction();

    return input
        .apply(
            ParDo.of(
                new DoFn<T, Row>() {
                  private transient @Nullable RowCastPlan plan;

                  @Setup
                  public void setup() {
                    this.plan = RowCastPlan.compile(inputSchema, targetSchema);
                  }

                  @ProcessElement
                  public void process(@Element T element, OutputReceiver<Row> r) {
                    RowCastPlan localPlan = this.plan;
                    if (localPlan == null) {
                      localPlan = RowCastPlan.compile(inputSchema, targetSchema);
                      this.plan = localPlan;
                    }
                    Row row = toRowFn != null ? toRowFn.apply(element) : (Row) element;
                    r.output(row == null ? null : localPlan.cast(row));
                  }
                }))
        .setRowSchema(targetSchema);
  }

  public static Row castRow(Row input, Schema inputSchema, Schema outputSchema) {
    if (input == null) {
      return null;
    }
    return getCastPlan(inputSchema, outputSchema).cast(input);
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
            return BigDecimal.valueOf(value.intValue());

          case INT64:
            return BigDecimal.valueOf(value.longValue());

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
      case ITERABLE:
        Iterable<Object> inputValues = (Iterable<Object>) inputValue;
        List<Object> outputValues =
            inputValues instanceof Collection
                ? new ArrayList<>(((Collection<?>) inputValues).size())
                : new ArrayList<>();

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
