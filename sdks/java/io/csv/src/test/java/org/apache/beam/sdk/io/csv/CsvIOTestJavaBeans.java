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
package org.apache.beam.sdk.io.csv;

import static java.util.Objects.requireNonNull;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Instant;

// TODO(https://github.com/apache/beam/issues/24980): replace with common schema-aware classes; see
// task description.
/** Classes and data to drive CsvIO tests. */
class CsvIOTestJavaBeans {

  private static final AutoValueSchema DEFAULT_SCHEMA_PROVIDER = new AutoValueSchema();

  /** Convenience method for {@link AllPrimitiveDataTypes} instantiation. */
  static AllPrimitiveDataTypes allPrimitiveDataTypes(
      Boolean aBoolean,
      Byte aByte,
      BigDecimal aDecimal,
      Double aDouble,
      Float aFloat,
      Short aShort,
      Integer anInteger,
      Long aLong,
      String aString) {
    return new AutoValue_CsvIOTestJavaBeans_AllPrimitiveDataTypes.Builder()
        .setABoolean(aBoolean)
        .setAByte(aByte)
        .setADecimal(aDecimal)
        .setADouble(aDouble)
        .setAFloat(aFloat)
        .setAShort(aShort)
        .setAnInteger(anInteger)
        .setALong(aLong)
        .setAString(aString)
        .build();
  }

  /** Convenience method for {@link NullableAllPrimitiveDataTypes} instantiation. */
  static NullableAllPrimitiveDataTypes nullableAllPrimitiveDataTypes(
      @Nullable Boolean aBoolean,
      @Nullable Double aDouble,
      @Nullable Float aFloat,
      @Nullable Integer anInteger,
      @Nullable Long aLong,
      @Nullable String aString) {
    return new AutoValue_CsvIOTestJavaBeans_NullableAllPrimitiveDataTypes.Builder()
        .setABoolean(aBoolean)
        .setADouble(aDouble)
        .setAFloat(aFloat)
        .setAnInteger(anInteger)
        .setALong(aLong)
        .setAString(aString)
        .build();
  }

  /** Convenience method for {@link TimeContaining} instantiation. */
  static TimeContaining timeContaining(Instant instant, List<Instant> instantList) {
    return new AutoValue_CsvIOTestJavaBeans_TimeContaining.Builder()
        .setInstant(instant)
        .setInstantList(instantList)
        .build();
  }

  private static final TypeDescriptor<AllPrimitiveDataTypes>
      ALL_PRIMITIVE_DATA_TYPES_TYPE_DESCRIPTOR = TypeDescriptor.of(AllPrimitiveDataTypes.class);

  /** The schema for {@link AllPrimitiveDataTypes}. */
  static final Schema ALL_PRIMITIVE_DATA_TYPES_SCHEMA =
      requireNonNull(DEFAULT_SCHEMA_PROVIDER.schemaFor(ALL_PRIMITIVE_DATA_TYPES_TYPE_DESCRIPTOR));

  /**
   * Returns a {@link SerializableFunction} to convert from a {@link AllPrimitiveDataTypes} to a
   * {@link Row}.
   */
  static SerializableFunction<AllPrimitiveDataTypes, Row> allPrimitiveDataTypesToRowFn() {
    return DEFAULT_SCHEMA_PROVIDER.toRowFunction(ALL_PRIMITIVE_DATA_TYPES_TYPE_DESCRIPTOR);
  }

  private static final TypeDescriptor<NullableAllPrimitiveDataTypes>
      NULLABLE_ALL_PRIMITIVE_DATA_TYPES_TYPE_DESCRIPTOR =
          TypeDescriptor.of(NullableAllPrimitiveDataTypes.class);

  /** The schema for {@link NullableAllPrimitiveDataTypes}. */
  static final Schema NULLABLE_ALL_PRIMITIVE_DATA_TYPES_SCHEMA =
      requireNonNull(
          DEFAULT_SCHEMA_PROVIDER.schemaFor(NULLABLE_ALL_PRIMITIVE_DATA_TYPES_TYPE_DESCRIPTOR));

  /**
   * Returns a {@link SerializableFunction} to convert from a {@link NullableAllPrimitiveDataTypes}
   * to a {@link Row}.
   */
  static SerializableFunction<NullableAllPrimitiveDataTypes, Row>
      nullableAllPrimitiveDataTypesToRowFn() {
    return DEFAULT_SCHEMA_PROVIDER.toRowFunction(NULLABLE_ALL_PRIMITIVE_DATA_TYPES_TYPE_DESCRIPTOR);
  }

  private static final TypeDescriptor<TimeContaining> TIME_CONTAINING_TYPE_DESCRIPTOR =
      TypeDescriptor.of(TimeContaining.class);

  /** The schema for {@link TimeContaining}. */
  static final Schema TIME_CONTAINING_SCHEMA =
      requireNonNull(DEFAULT_SCHEMA_PROVIDER.schemaFor(TIME_CONTAINING_TYPE_DESCRIPTOR));

  /**
   * Returns a {@link SerializableFunction} to convert from a {@link TimeContaining} to a {@link
   * Row}.
   */
  static SerializableFunction<TimeContaining, Row> timeContainingToRowFn() {
    return DEFAULT_SCHEMA_PROVIDER.toRowFunction(TIME_CONTAINING_TYPE_DESCRIPTOR);
  }

  /**
   * Contains all primitive Java types i.e. String, Integer, etc and {@link BigDecimal}. The purpose
   * of this class is to test schema-aware PTransforms with flat {@link Schema} {@link Row}s.
   */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class AllPrimitiveDataTypes implements Serializable {

    abstract Boolean getABoolean();

    abstract Byte getAByte();

    abstract BigDecimal getADecimal();

    abstract Double getADouble();

    abstract Float getAFloat();

    abstract Short getAShort();

    abstract Integer getAnInteger();

    abstract Long getALong();

    abstract String getAString();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setABoolean(Boolean value);

      abstract Builder setAByte(Byte value);

      abstract Builder setADecimal(BigDecimal value);

      abstract Builder setADouble(Double value);

      abstract Builder setAFloat(Float value);

      abstract Builder setAShort(Short value);

      abstract Builder setAnInteger(Integer value);

      abstract Builder setALong(Long value);

      abstract Builder setAString(String value);

      abstract AllPrimitiveDataTypes build();
    }
  }

  /**
   * Contains all nullable primitive Java types i.e. String, Integer, etc and {@link BigDecimal}.
   * The purpose of this class is to test schema-aware PTransforms with flat {@link Schema} {@link
   * Row}s.
   */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class NullableAllPrimitiveDataTypes implements Serializable {

    @Nullable
    abstract Boolean getABoolean();

    @Nullable
    abstract Double getADouble();

    @Nullable
    abstract Float getAFloat();

    @Nullable
    abstract Integer getAnInteger();

    @Nullable
    abstract Long getALong();

    @Nullable
    abstract String getAString();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setABoolean(Boolean value);

      abstract Builder setADouble(Double value);

      abstract Builder setAFloat(Float value);

      abstract Builder setAnInteger(Integer value);

      abstract Builder setALong(Long value);

      abstract Builder setAString(String value);

      abstract NullableAllPrimitiveDataTypes build();
    }
  }

  /**
   * Contains time-related types. The purpose of this class is to test schema-aware PTransforms with
   * time-related {@link Schema.FieldType} containing {@link Row}s.
   */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  abstract static class TimeContaining {

    abstract Instant getInstant();

    abstract List<Instant> getInstantList();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setInstant(Instant value);

      abstract Builder setInstantList(List<Instant> value);

      abstract TimeContaining build();
    }
  }
}
