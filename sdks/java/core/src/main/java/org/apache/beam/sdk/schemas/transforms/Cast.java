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

import static org.apache.beam.sdk.schemas.Schema.TypeName.ARRAY;
import static org.apache.beam.sdk.schemas.Schema.TypeName.INT16;
import static org.apache.beam.sdk.schemas.Schema.TypeName.INT32;
import static org.apache.beam.sdk.schemas.Schema.TypeName.INT64;
import static org.apache.beam.sdk.schemas.Schema.TypeName.MAP;
import static org.apache.beam.sdk.schemas.Schema.TypeName.ROW;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.FieldAccessDescriptor;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

/** Set of utilities for casting rows between schemas. */
@Experimental(Experimental.Kind.SCHEMAS)
@AutoValue
public abstract class Cast<T> extends PTransform<PCollection<T>, PCollection<Row>> {

  public abstract Schema outputSchema();

  public abstract Nullability nullability();

  public abstract Type type();

  public abstract Shape shape();

  /** Builder for {@link Cast}. */
  @AutoValue.Builder
  public abstract static class Builder<T> {

    public abstract Builder<T> outputSchema(Schema schema);

    public abstract Builder<T> nullability(Nullability nullability);

    public abstract Builder<T> type(Type type);

    public abstract Builder<T> shape(Shape shape);

    public abstract Cast<T> build();
  }

  public static <T> Builder<T> builder() {
    return new AutoValue_Cast.Builder<T>();
  }

  public static <T> Cast<T> to(Schema outputSchema) {
    return Cast.<T>builder()
        .outputSchema(outputSchema)
        .nullability(Nullability.IGNORE)
        .type(Type.WIDEN)
        .shape(Shape.PROJECTION)
        .build();
  }

  public List<CompatibilityError> compatibility(Schema inputSchema) {
    return Inference.compatibility(inputSchema, outputSchema(), nullability(), type(), shape());
  }

  public void verifyCompatibility(Schema inputSchema) {
    List<CompatibilityError> errors = compatibility(inputSchema);

    if (!errors.isEmpty()) {
      String reason =
          errors
              .stream()
              .map(x -> Joiner.on('.').join(x.path()) + ": " + x.message())
              .collect(Collectors.joining("\n\t"));

      throw new IllegalArgumentException("Cast isn't compatible:\n\t" + reason);
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
                  // Once BEAM-4457 is fixed, fix this.
                  @FieldAccess("filterFields")
                  final FieldAccessDescriptor fieldAccessDescriptor =
                      FieldAccessDescriptor.withAllFields();

                  @ProcessElement
                  public void process(@FieldAccess("filterFields") Row row, OutputReceiver<Row> r) {
                    r.output(castRow(row, inputSchema, outputSchema()));
                  }
                }))
        .setRowSchema(outputSchema());
  }

  @VisibleForTesting
  static Row castRow(Row input, Schema inputSchema, Schema outputSchema) {
    Row.Builder output = Row.withSchema(outputSchema);
    for (int i = 0; i < outputSchema.getFieldCount(); i++) {
      Schema.Field outputField = outputSchema.getField(i);

      int fromFieldIdx = inputSchema.indexOf(outputField.getName());
      Schema.Field inputField = inputSchema.getField(fromFieldIdx);

      Object inputValue = input.getValue(fromFieldIdx);

      if (inputValue == null) {
        output.addValue(null);
      } else {
        Object value = castObject(inputValue, inputField.getType(), outputField.getType());
        output.addValue(value);
      }
    }

    return output.build();
  }

  @VisibleForTesting
  @SuppressWarnings("unchecked")
  static Object castObject(
      Object inputValue, Schema.FieldType inputFieldType, Schema.FieldType outputFieldType) {

    if (inputFieldType.getTypeName() == ROW) {
      return castRow(
          (Row) inputValue, inputFieldType.getRowSchema(), outputFieldType.getRowSchema());
    } else if (inputFieldType.getTypeName() == ARRAY) {
      List<Object> inputValues = (List<Object>) inputValue;
      List<Object> outputValues = new ArrayList<>(inputValues.size());

      for (Object elem : inputValues) {
        outputValues.add(
            castObject(
                elem,
                inputFieldType.getCollectionElementType(),
                outputFieldType.getCollectionElementType()));
      }

      return outputValues;
    } else if (inputFieldType.getTypeName() == MAP) {
      Map<Object, Object> inputMap = (Map<Object, Object>) inputValue;
      Map<Object, Object> outputMap = Maps.newHashMapWithExpectedSize(inputMap.size());

      for (Map.Entry<Object, Object> entry : inputMap.entrySet()) {
        Object outputKey =
            castObject(
                entry.getKey(), inputFieldType.getMapKeyType(), outputFieldType.getMapKeyType());
        Object outputValue =
            castObject(
                entry.getValue(),
                inputFieldType.getMapValueType(),
                outputFieldType.getMapValueType());

        outputMap.put(outputKey, outputValue);
      }

      return outputMap;
    } else {
      if (inputFieldType.getTypeName().equals(outputFieldType.getTypeName())) {
        return inputValue;
      } else {
        KV<TypeName, TypeName> key =
            KV.of(inputFieldType.getTypeName(), outputFieldType.getTypeName());
        return Inference.ALL_MAP.get(key).apply(inputValue);
      }
    }
  }

  /** Configures casting for nullable fields. */
  public enum Nullability {
    /** Can cast only if nullability is the same. */
    SAME,
    /** Can cast non-nullable fields to nullable. */
    WEAKEN,
    /** Can cast nullable fields to non-nullable and forth. */
    IGNORE
  }

  /** Configures casting of primitive types. */
  public enum Type {
    /** Can cast only if types are the same. */
    SAME,
    /** Can cast only if an output type is a supertype of an input type. */
    WIDEN,
    /** Can cast if an output type is a supertype or subtype of an input type. */
    NARROW
  }

  /** Configures casting of rows. */
  public enum Shape {
    /** Can cast rows only if field names are the same. */
    SAME,
    /** Can cast rows if output field names is subset of input field names. */
    PROJECTION
  }

  /** Describes compatibility errors during casting. */
  @AutoValue
  public abstract static class CompatibilityError {

    public abstract List<String> path();

    public abstract String message();

    public static CompatibilityError create(List<String> path, String message) {
      return new AutoValue_Cast_CompatibilityError(path, message);
    }
  }

  static class Inference {

    @FunctionalInterface
    interface Converter {

      Object apply(Object value);
    }

    @AutoValue
    abstract static class Result<T> {

      abstract List<String> path();

      abstract Optional<T> value();

      abstract List<CompatibilityError> errors();

      static <T> Result<T> create(
          List<String> path, Optional<T> value, List<CompatibilityError> errors) {
        return new AutoValue_Cast_Inference_Result<>(path, value, errors);
      }

      public static <T> Result<T> of(List<String> path, T value) {
        return create(path, Optional.of(value), Collections.emptyList());
      }

      public static <T> Result<T> error(List<String> path, String message) {
        return Result.create(
            path,
            Optional.empty(),
            Collections.singletonList(CompatibilityError.create(path, message)));
      }
    }

    static final List<Nullability> NULLABILITY_ORDER =
        ImmutableList.of(Nullability.SAME, Nullability.WEAKEN, Nullability.IGNORE);

    static final List<Shape> SHAPE_ORDER = ImmutableList.of(Shape.SAME, Shape.PROJECTION);

    static final List<Type> TYPE_ORDER = ImmutableList.of(Type.SAME, Type.WIDEN, Type.NARROW);

    // TODO extend this list

    static final Map<KV<TypeName, TypeName>, Converter> TYPE_WIDEN =
        ImmutableMap.<KV<TypeName, TypeName>, Converter>builder()
            // INT16
            .put(KV.of(INT16, INT32), x -> ((Short) x).intValue())
            .put(KV.of(INT16, INT64), x -> ((Short) x).longValue())
            // INT32
            .put(KV.of(INT32, INT64), x -> ((Integer) x).longValue())
            .build();

    static final Map<KV<TypeName, TypeName>, Converter> TYPE_NARROW =
        ImmutableMap.<KV<TypeName, TypeName>, Converter>builder()
            // -> INT16
            .put(KV.of(INT32, INT16), x -> ((Integer) x).shortValue())
            .put(KV.of(INT64, INT16), x -> ((Long) x).shortValue())
            // -> INT32
            .put(KV.of(INT64, INT32), x -> ((Long) x).intValue())
            .build();

    static final Map<KV<TypeName, TypeName>, Converter> ALL_MAP =
        ImmutableMap.<KV<TypeName, TypeName>, Converter>builder()
            .putAll(TYPE_WIDEN)
            .putAll(TYPE_NARROW)
            .build();

    static List<CompatibilityError> compatibility(
        Schema inputSchema, Schema outputSchema, Nullability nullability, Type type, Shape shape) {

      Stream<CompatibilityError> nullabilityErrors =
          nullability(Collections.emptyList(), inputSchema, outputSchema)
              .flatMap(
                  x -> {
                    if (x.value().isPresent()) {
                      if (greater(x.value().get(), nullability)) {
                        String message =
                            String.format(
                                "Casting requires nullability=%s, limited to %s",
                                x.value().get(), nullability);
                        return Stream.of(CompatibilityError.create(x.path(), message));
                      } else {
                        return Stream.empty();
                      }
                    } else {
                      return x.errors().stream();
                    }
                  });

      Stream<CompatibilityError> typeErrors =
          type(Collections.emptyList(), inputSchema, outputSchema)
              .flatMap(
                  x -> {
                    if (x.value().isPresent()) {
                      if (greater(x.value().get(), type)) {
                        String message =
                            String.format(
                                "Casting requires type=%s, limited to %s", x.value().get(), type);
                        return Stream.of(CompatibilityError.create(x.path(), message));
                      } else {
                        return Stream.empty();
                      }
                    } else {
                      return x.errors().stream();
                    }
                  });

      Stream<CompatibilityError> shapeErrors =
          shape(Collections.emptyList(), inputSchema, outputSchema)
              .flatMap(
                  x -> {
                    if (x.value().isPresent()) {
                      if (greater(x.value().get(), shape)) {
                        String message =
                            String.format(
                                "Casting requires shape=%s, limited to %s", x.value().get(), shape);
                        return Stream.of(CompatibilityError.create(x.path(), message));
                      } else {
                        return Stream.empty();
                      }
                    } else {
                      return x.errors().stream();
                    }
                  });

      return Stream.concat(Stream.concat(nullabilityErrors, typeErrors), shapeErrors)
          .collect(Collectors.toList());
    }

    static boolean greater(Nullability a, Nullability b) {
      return NULLABILITY_ORDER.indexOf(a) > NULLABILITY_ORDER.indexOf(b);
    }

    static boolean greater(Type a, Type b) {
      return TYPE_ORDER.indexOf(a) > TYPE_ORDER.indexOf(b);
    }

    static boolean greater(Shape a, Shape b) {
      return SHAPE_ORDER.indexOf(a) > SHAPE_ORDER.indexOf(b);
    }

    static Stream<Result<Nullability>> nullability(
        List<String> path, Schema inputSchema, Schema outputSchema) {
      List<String> intersection = intersection(inputSchema, outputSchema);

      return intersection
          .stream()
          .flatMap(
              name ->
                  nullability(
                      concat(path, name), inputSchema.getField(name), outputSchema.getField(name)));
    }

    static Stream<Result<Nullability>> nullability(
        List<String> path, Schema.Field inputSchema, Schema.Field outputSchema) {

      Nullability nullability;

      if (inputSchema.getNullable().equals(outputSchema.getNullable())) {
        nullability = Nullability.SAME;
      } else if (!inputSchema.getNullable() && outputSchema.getNullable()) {
        nullability = Nullability.WEAKEN;
      } else if (inputSchema.getNullable() && !outputSchema.getNullable()) {
        nullability = Nullability.IGNORE;
      } else {
        throw new AssertionError("not possible");
      }

      return Stream.concat(
          Stream.of(Result.of(path, nullability)),
          nullability(path, inputSchema.getType(), outputSchema.getType()));
    }

    static Stream<Result<Nullability>> nullability(
        List<String> path, Schema.FieldType inputSchema, Schema.FieldType outputSchema) {
      if (outputSchema.getTypeName() != inputSchema.getTypeName()) {
        return Stream.empty(); // not possible to cast composite type unless type matches
      }

      switch (inputSchema.getTypeName()) {
        case ARRAY:
          return nullability(
              path,
              inputSchema.getCollectionElementType(),
              outputSchema.getCollectionElementType());

        case ROW:
          return nullability(path, inputSchema.getRowSchema(), outputSchema.getRowSchema());

        case MAP:
          return Stream.concat(
              nullability(path, inputSchema.getMapKeyType(), outputSchema.getMapKeyType()),
              nullability(path, inputSchema.getMapValueType(), outputSchema.getMapValueType()));

        default:
          return Stream.empty();
      }
    }

    static List<String> intersection(Schema inputSchema, Schema outputSchema) {
      return outputSchema
          .getFields()
          .stream()
          .map(Schema.Field::getName)
          .filter(inputSchema::hasField)
          .collect(Collectors.toList());
    }

    static Stream<Result<Shape>> shape(List<String> path, Schema inputSchema, Schema outputSchema) {
      List<String> intersection = intersection(inputSchema, outputSchema);

      Shape shape;

      if (intersection.size() == inputSchema.getFieldCount()) {
        shape = Shape.SAME;
      } else {
        shape = Shape.PROJECTION;
      }

      return Stream.concat(
          Stream.of(Result.of(path, shape)),
          intersection
              .stream()
              .flatMap(
                  name ->
                      shape(
                          concat(path, name),
                          inputSchema.getField(name).getType(),
                          outputSchema.getField(name).getType())));
    }

    static Stream<Result<Shape>> shape(
        List<String> path, Schema.FieldType inputSchema, Schema.FieldType outputSchema) {
      switch (inputSchema.getTypeName()) {
        case ARRAY:
          if (outputSchema.getTypeName() == ARRAY) {
            return shape(
                path,
                inputSchema.getCollectionElementType(),
                outputSchema.getCollectionElementType());
          } else {
            return Stream.of();
          }

        case ROW:
          if (outputSchema.getTypeName() == ROW) {
            return shape(path, inputSchema.getRowSchema(), outputSchema.getRowSchema());
          } else {
            return Stream.of();
          }

        case MAP:
          if (outputSchema.getTypeName() == MAP) {
            return Stream.concat(
                shape(path, inputSchema.getMapKeyType(), outputSchema.getMapKeyType()),
                shape(path, inputSchema.getMapValueType(), outputSchema.getMapValueType()));
          } else {
            return Stream.of();
          }

        default:
          return Stream.of();
      }
    }

    static <T> List<T> concat(List<T> a, T b) {
      return ImmutableList.<T>builder().addAll(a).add(b).build();
    }

    static Stream<Result<Type>> type(List<String> path, Schema inputSchema, Schema outputSchema) {
      List<String> intersection = intersection(inputSchema, outputSchema);

      return intersection
          .stream()
          .flatMap(
              name ->
                  type(
                      concat(path, name),
                      inputSchema.getField(name).getType(),
                      outputSchema.getField(name).getType()));
    }

    static Stream<Result<Type>> castError(
        List<String> path, Schema.FieldType from, Schema.FieldType to) {
      String message =
          String.format("Can't cast '%s' to '%s'", from.getTypeName(), to.getTypeName());
      return Stream.of(Result.error(path, message));
    }

    static Stream<Result<Type>> type(
        List<String> path, Schema.FieldType inputSchema, Schema.FieldType outputSchema) {
      switch (inputSchema.getTypeName()) {
        case ARRAY:
          if (outputSchema.getTypeName() == ARRAY) {
            return type(
                path,
                inputSchema.getCollectionElementType(),
                outputSchema.getCollectionElementType());
          } else {
            return castError(path, inputSchema, outputSchema);
          }

        case ROW:
          if (outputSchema.getTypeName() == ROW) {
            return type(path, inputSchema.getRowSchema(), outputSchema.getRowSchema());
          } else {
            return castError(path, inputSchema, outputSchema);
          }

        case MAP:
          if (outputSchema.getTypeName() == MAP) {
            return Stream.concat(
                type(path, inputSchema.getMapKeyType(), outputSchema.getMapKeyType()),
                type(path, inputSchema.getMapValueType(), outputSchema.getMapValueType()));
          } else {
            return castError(path, inputSchema, outputSchema);
          }

        default:
          KV<TypeName, TypeName> inputOutputKv =
              KV.of(inputSchema.getTypeName(), outputSchema.getTypeName());

          if (inputSchema.getTypeName().equals(outputSchema.getTypeName())) {
            return Stream.of(Result.of(path, Type.SAME));
          } else if (TYPE_WIDEN.containsKey(inputOutputKv)) {
            return Stream.of(Result.of(path, Type.WIDEN));
          } else if (TYPE_NARROW.containsKey(inputOutputKv)) {
            return Stream.of(Result.of(path, Type.NARROW));
          } else {
            return castError(path, inputSchema, outputSchema);
          }
      }
    }
  }
}
