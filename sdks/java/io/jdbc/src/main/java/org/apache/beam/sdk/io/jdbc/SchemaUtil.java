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
package org.apache.beam.sdk.io.jdbc;

import static java.sql.JDBCType.BINARY;
import static java.sql.JDBCType.CHAR;
import static java.sql.JDBCType.LONGNVARCHAR;
import static java.sql.JDBCType.LONGVARBINARY;
import static java.sql.JDBCType.LONGVARCHAR;
import static java.sql.JDBCType.NCHAR;
import static java.sql.JDBCType.NVARCHAR;
import static java.sql.JDBCType.VARBINARY;
import static java.sql.JDBCType.VARCHAR;
import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;
import java.sql.Array;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.EnumMap;
import java.util.List;
import java.util.Objects;
import java.util.TimeZone;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.FixedPrecisionNumeric;
import org.apache.beam.sdk.schemas.logicaltypes.FixedString;
import org.apache.beam.sdk.schemas.logicaltypes.VariableBytes;
import org.apache.beam.sdk.schemas.logicaltypes.VariableString;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.chrono.ISOChronology;

/** Provides utility functions for working with Beam {@link Schema} types. */
public class SchemaUtil {
  /**
   * Interface implemented by functions that extract values of different types from a JDBC
   * ResultSet.
   */
  @FunctionalInterface
  interface ResultSetFieldExtractor extends Serializable {
    @Nullable
    Object extract(ResultSet rs, Integer index) throws SQLException;
  }
  // ResultSetExtractors for primitive schema types (excluding arrays, structs and logical types).
  private static final EnumMap<Schema.TypeName, ResultSetFieldExtractor>
      RESULTSET_FIELD_EXTRACTORS =
          new EnumMap<>(
              ImmutableMap.<Schema.TypeName, ResultSetFieldExtractor>builder()
                  .put(Schema.TypeName.BOOLEAN, ResultSet::getBoolean)
                  .put(Schema.TypeName.BYTE, ResultSet::getByte)
                  .put(Schema.TypeName.BYTES, ResultSet::getBytes)
                  .put(Schema.TypeName.DATETIME, ResultSet::getTimestamp)
                  .put(Schema.TypeName.DECIMAL, ResultSet::getBigDecimal)
                  .put(Schema.TypeName.DOUBLE, ResultSet::getDouble)
                  .put(Schema.TypeName.FLOAT, ResultSet::getFloat)
                  .put(Schema.TypeName.INT16, ResultSet::getShort)
                  .put(Schema.TypeName.INT32, ResultSet::getInt)
                  .put(Schema.TypeName.INT64, ResultSet::getLong)
                  .put(Schema.TypeName.STRING, ResultSet::getString)
                  .build());

  private static final ResultSetFieldExtractor DATE_EXTRACTOR = createDateExtractor();
  private static final ResultSetFieldExtractor TIME_EXTRACTOR = createTimeExtractor();
  private static final ResultSetFieldExtractor TIMESTAMP_EXTRACTOR = createTimestampExtractor();
  private static final ResultSetFieldExtractor OBJECT_EXTRACTOR = createObjectExtractor();

  /**
   * Interface implemented by functions that create Beam {@link
   * org.apache.beam.sdk.schemas.Schema.Field} corresponding to JDBC field metadata.
   */
  @FunctionalInterface
  interface BeamFieldConverter extends Serializable {
    Schema.Field create(int index, ResultSetMetaData md) throws SQLException;
  }

  private static BeamFieldConverter jdbcTypeToBeamFieldConverter(
      JDBCType jdbcType, String className) {
    switch (jdbcType) {
      case ARRAY:
        return beamArrayField();
      case BIGINT:
        return beamFieldOfType(Schema.FieldType.INT64);
      case BINARY:
        return beamLogicalField(BINARY.getName(), LogicalTypes::fixedOrVariableBytes);
      case BIT:
        return beamFieldOfType(LogicalTypes.JDBC_BIT_TYPE);
      case BOOLEAN:
        return beamFieldOfType(Schema.FieldType.BOOLEAN);
      case CHAR:
        return beamLogicalField(CHAR.getName(), FixedString::of);
      case DATE:
        return beamFieldOfType(LogicalTypes.JDBC_DATE_TYPE);
      case DECIMAL:
        return beamFieldOfType(Schema.FieldType.DECIMAL);
      case DOUBLE:
        return beamFieldOfType(Schema.FieldType.DOUBLE);
      case FLOAT:
        return beamFieldOfType(LogicalTypes.JDBC_FLOAT_TYPE);
      case INTEGER:
        return beamFieldOfType(Schema.FieldType.INT32);
      case LONGNVARCHAR:
        return beamLogicalField(LONGNVARCHAR.getName(), VariableString::of);
      case LONGVARBINARY:
        return beamLogicalField(LONGVARBINARY.getName(), VariableBytes::of);
      case LONGVARCHAR:
        return beamLogicalField(LONGVARCHAR.getName(), VariableString::of);
      case NCHAR:
        return beamLogicalField(NCHAR.getName(), FixedString::of);
      case NUMERIC:
        return beamLogicalNumericField();
      case NVARCHAR:
        return beamLogicalField(NVARCHAR.getName(), VariableString::of);
      case REAL:
        return beamFieldOfType(Schema.FieldType.FLOAT);
      case SMALLINT:
        return beamFieldOfType(Schema.FieldType.INT16);
      case TIME:
        return beamFieldOfType(LogicalTypes.JDBC_TIME_TYPE);
      case TIMESTAMP:
        return beamFieldOfType(Schema.FieldType.DATETIME);
      case TIMESTAMP_WITH_TIMEZONE:
        return beamFieldOfType(LogicalTypes.JDBC_TIMESTAMP_WITH_TIMEZONE_TYPE);
      case TINYINT:
        return beamFieldOfType(Schema.FieldType.BYTE);
      case VARBINARY:
        return beamLogicalField(VARBINARY.getName(), VariableBytes::of);
      case VARCHAR:
        return beamLogicalField(VARCHAR.getName(), VariableString::of);
      case BLOB:
        return beamFieldOfType(FieldType.BYTES);
      case CLOB:
        return beamFieldOfType(FieldType.STRING);
      case OTHER:
      case JAVA_OBJECT:
        if (UUID.class.getName().equals(className)) {
          return beamFieldOfType(LogicalTypes.JDBC_UUID_TYPE);
        }
        return beamFieldOfType(LogicalTypes.OTHER_AS_STRING_TYPE);
      default:
        throw new UnsupportedOperationException(
            "Converting " + jdbcType + " to Beam schema type is not supported");
    }
  }

  /** Infers the Beam {@link Schema} from {@link ResultSetMetaData}. */
  public static Schema toBeamSchema(ResultSetMetaData md) throws SQLException {
    Schema.Builder schemaBuilder = Schema.builder();

    for (int i = 1; i <= md.getColumnCount(); i++) {
      JDBCType jdbcType = JDBCType.valueOf(md.getColumnType(i));
      String className = md.getColumnClassName(i);
      BeamFieldConverter fieldConverter = jdbcTypeToBeamFieldConverter(jdbcType, className);
      schemaBuilder.addField(fieldConverter.create(i, md));
    }

    return schemaBuilder.build();
  }

  /** Converts a primitive JDBC field to corresponding Beam schema field. */
  private static BeamFieldConverter beamFieldOfType(Schema.FieldType fieldType) {
    return (index, md) -> {
      String label = md.getColumnLabel(index);
      return Schema.Field.of(label, fieldType)
          .withNullable(md.isNullable(index) == ResultSetMetaData.columnNullable);
    };
  }

  /** Converts logical types with arguments such as VARCHAR(25). */
  private static <InputT, BaseT> BeamFieldConverter beamLogicalField(
      String identifier,
      BiFunction<String, Integer, Schema.LogicalType<InputT, BaseT>> constructor) {
    return (index, md) -> {
      int size = md.getPrecision(index);
      Schema.FieldType fieldType =
          Schema.FieldType.logicalType(constructor.apply(identifier, size));
      return beamFieldOfType(fieldType).create(index, md);
    };
  }

  /**
   * Converts numeric fields with specified precision and scale to {@link FixedPrecisionNumeric}. If
   * a precision of numeric field is not specified, then converts such field to {@link
   * FieldType#DECIMAL}.
   */
  private static BeamFieldConverter beamLogicalNumericField() {
    return (index, md) -> {
      int precision = md.getPrecision(index);
      if (precision == Integer.MAX_VALUE || precision == -1) {
        // If a precision is not specified, the column stores values as given (e.g. in Oracle DB)
        return Schema.Field.of(md.getColumnLabel(index), FieldType.DECIMAL)
            .withNullable(md.isNullable(index) == ResultSetMetaData.columnNullable);
      }
      int scale = md.getScale(index);
      Schema.FieldType fieldType =
          Schema.FieldType.logicalType(FixedPrecisionNumeric.of(precision, scale));
      return beamFieldOfType(fieldType).create(index, md);
    };
  }

  /** Converts array fields. */
  private static BeamFieldConverter beamArrayField() {
    return (index, md) -> {
      JDBCType elementJdbcType = JDBCType.valueOf(md.getColumnTypeName(index));
      String elementClassName = md.getColumnClassName(index);
      BeamFieldConverter elementFieldConverter =
          jdbcTypeToBeamFieldConverter(elementJdbcType, elementClassName);

      String label = md.getColumnLabel(index);
      Schema.FieldType elementBeamType = elementFieldConverter.create(index, md).getType();
      return Schema.Field.of(label, Schema.FieldType.array(elementBeamType))
          .withNullable(md.isNullable(index) == ResultSetMetaData.columnNullable);
    };
  }

  /** Creates a {@link ResultSetFieldExtractor} for the given type. */
  private static ResultSetFieldExtractor createFieldExtractor(Schema.FieldType fieldType) {
    Schema.TypeName typeName = fieldType.getTypeName();
    switch (typeName) {
      case ARRAY:
      case ITERABLE:
        Schema.FieldType elementType = checkArgumentNotNull(fieldType.getCollectionElementType());
        ResultSetFieldExtractor elementExtractor = createFieldExtractor(elementType);
        return createArrayExtractor(elementExtractor);
      case DATETIME:
        return TIMESTAMP_EXTRACTOR;
      case LOGICAL_TYPE:
        return createLogicalTypeExtractor(checkArgumentNotNull(fieldType.getLogicalType()));
      default:
        if (!RESULTSET_FIELD_EXTRACTORS.containsKey(typeName)) {
          throw new UnsupportedOperationException(
              "BeamRowMapper does not have support for fields of type " + fieldType);
        }
        return RESULTSET_FIELD_EXTRACTORS.get(typeName);
    }
  }

  /** Creates a {@link ResultSetFieldExtractor} for array types. */
  private static ResultSetFieldExtractor createArrayExtractor(
      ResultSetFieldExtractor elementExtractor) {
    return (rs, index) -> {
      Array arrayVal = rs.getArray(index);
      if (arrayVal == null) {
        return null;
      }

      List<@Nullable Object> arrayElements = new ArrayList<>();
      ResultSet arrayRs = checkArgumentNotNull(arrayVal.getResultSet());
      while (arrayRs.next()) {
        arrayElements.add(elementExtractor.extract(arrayRs, 1));
      }
      return arrayElements;
    };
  }

  /** Creates a {@link ResultSetFieldExtractor} for logical types. */
  private static <InputT, BaseT> ResultSetFieldExtractor createLogicalTypeExtractor(
      final Schema.LogicalType<InputT, BaseT> fieldType) {
    String logicalTypeName = fieldType.getIdentifier();

    if (Objects.equals(fieldType, LogicalTypes.JDBC_UUID_TYPE.getLogicalType())) {
      return OBJECT_EXTRACTOR;
    } else if (logicalTypeName.equals("DATE")) {
      return DATE_EXTRACTOR;
    } else if (logicalTypeName.equals("TIME")) {
      return TIME_EXTRACTOR;
    } else if (logicalTypeName.equals("TIMESTAMP_EXTRACTOR")) {
      return TIMESTAMP_EXTRACTOR;
    } else {
      ResultSetFieldExtractor extractor = createFieldExtractor(fieldType.getBaseType());
      return (rs, index) -> {
        BaseT v = (BaseT) extractor.extract(rs, index);
        if (v == null) {
          return null;
        }
        return fieldType.toInputType(v);
      };
    }
  }

  /** Convert SQL date type to Beam DateTime. */
  private static ResultSetFieldExtractor createDateExtractor() {
    return (rs, i) -> {
      // TODO(https://github.com/apache/beam/issues/19215) import when joda LocalDate is removed.
      java.time.LocalDate date = rs.getObject(i, java.time.LocalDate.class);
      if (date == null) {
        return null;
      }
      ZonedDateTime zdt = date.atStartOfDay(ZoneOffset.UTC);
      return new DateTime(zdt.toInstant().toEpochMilli(), ISOChronology.getInstanceUTC());
    };
  }

  /** Convert SQL time type to Beam DateTime. */
  private static ResultSetFieldExtractor createTimeExtractor() {
    return (rs, i) -> {
      Time time = rs.getTime(i, Calendar.getInstance(TimeZone.getTimeZone(ZoneOffset.UTC)));
      if (time == null) {
        return null;
      }
      return new DateTime(time.getTime(), ISOChronology.getInstanceUTC())
          .withDate(new LocalDate(0L));
    };
  }

  /** Convert SQL timestamp type to Beam DateTime. */
  private static ResultSetFieldExtractor createTimestampExtractor() {
    return (rs, i) -> {
      Timestamp ts = rs.getTimestamp(i, Calendar.getInstance(TimeZone.getTimeZone(ZoneOffset.UTC)));
      if (ts == null) {
        return null;
      }
      return new DateTime(ts.toInstant().toEpochMilli(), ISOChronology.getInstanceUTC());
    };
  }

  /** Convert SQL OTHER type to Beam Object. */
  private static ResultSetFieldExtractor createObjectExtractor() {
    return ResultSet::getObject;
  }

  /**
   * A {@link org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper} implementation that converts JDBC
   * results into Beam {@link Row} objects.
   */
  public static final class BeamRowMapper implements JdbcIO.RowMapper<Row> {
    private final Schema schema;
    private final List<ResultSetFieldExtractor> fieldExtractors;

    public static BeamRowMapper of(Schema schema) {
      List<ResultSetFieldExtractor> fieldExtractors =
          IntStream.range(0, schema.getFieldCount())
              .mapToObj(i -> createFieldExtractor(schema.getField(i).getType()))
              .collect(Collectors.toList());

      return new BeamRowMapper(schema, fieldExtractors);
    }

    private BeamRowMapper(Schema schema, List<ResultSetFieldExtractor> fieldExtractors) {
      this.schema = schema;
      this.fieldExtractors = fieldExtractors;
    }

    @Override
    public Row mapRow(ResultSet rs) throws Exception {
      Row.Builder rowBuilder = Row.withSchema(schema);
      for (int i = 0; i < schema.getFieldCount(); i++) {
        Object value = fieldExtractors.get(i).extract(rs, i + 1);
        if (rs.wasNull()) {
          rowBuilder.addValue(null);
        } else {
          rowBuilder.addValue(value);
        }
      }
      return rowBuilder.build();
    }
  }

  /**
   * compares two fields. Does not compare nullability of field types.
   *
   * @param a field 1
   * @param b field 2
   * @return TRUE if fields are equal. Otherwise FALSE
   */
  public static boolean compareSchemaField(Schema.Field a, Schema.Field b) {
    if (!a.getName().equalsIgnoreCase(b.getName())) {
      return false;
    }

    return compareSchemaFieldType(a.getType(), b.getType());
  }

  /**
   * checks nullability for fields.
   *
   * @param fields list of fields
   * @return TRUE if any field is not nullable
   */
  static boolean checkNullabilityForFields(List<Schema.Field> fields) {
    return fields.stream().anyMatch(field -> !field.getType().getNullable());
  }

  /**
   * Compares two FieldType. Modified from FieldType.equals to ignore nullability.
   *
   * @param a FieldType 1
   * @param b FieldType 2
   * @return TRUE if FieldType are equal. Otherwise FALSE
   */
  static boolean compareSchemaFieldType(Schema.FieldType a, Schema.FieldType b) {
    if (a.getTypeName().equals(b.getTypeName())) {
      if (!a.getTypeName().isLogicalType()) {
        return true;
      } else {
        Schema.LogicalType<?, ?> aLogicalType = checkArgumentNotNull(a.getLogicalType());
        Schema.LogicalType<?, ?> bLogicalType = checkArgumentNotNull(b.getLogicalType());
        return compareSchemaFieldType(aLogicalType.getBaseType(), bLogicalType.getBaseType());
      }
    } else if (a.getTypeName().isLogicalType()) {
      Schema.LogicalType<?, ?> aLogicalType = checkArgumentNotNull(a.getLogicalType());
      return aLogicalType.getBaseType().getTypeName().equals(b.getTypeName());
    } else if (b.getTypeName().isLogicalType()) {
      Schema.LogicalType<?, ?> bLogicalType = checkArgumentNotNull(b.getLogicalType());
      return bLogicalType.getBaseType().getTypeName().equals(a.getTypeName());
    }
    return false;
  }

  static class FieldWithIndex implements Serializable {
    private final Schema.Field field;
    private final Integer index;

    private FieldWithIndex(Schema.Field field, Integer index) {
      this.field = field;
      this.index = index;
    }

    static FieldWithIndex of(Schema.Field field, Integer index) {
      checkArgument(field != null);
      checkArgument(index != null);
      return new FieldWithIndex(field, index);
    }

    public Schema.Field getField() {
      return field;
    }

    public Integer getIndex() {
      return index;
    }
  }
}
