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
import static java.sql.JDBCType.NUMERIC;
import static java.sql.JDBCType.NVARCHAR;
import static java.sql.JDBCType.VARBINARY;
import static java.sql.JDBCType.VARCHAR;
import static java.sql.JDBCType.valueOf;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;
import java.sql.Array;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.EnumMap;
import java.util.List;
import java.util.TimeZone;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.chrono.ISOChronology;

/** Provides utility functions for working with Beam {@link Schema} types. */
@Experimental(Kind.SCHEMAS)
class SchemaUtil {
  /**
   * Interface implemented by functions that extract values of different types from a JDBC
   * ResultSet.
   */
  @FunctionalInterface
  interface ResultSetFieldExtractor extends Serializable {
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

  /**
   * Interface implemented by functions that create Beam {@link
   * org.apache.beam.sdk.schemas.Schema.Field} corresponding to JDBC field metadata.
   */
  @FunctionalInterface
  interface BeamFieldConverter extends Serializable {
    Schema.Field create(int index, ResultSetMetaData md) throws SQLException;
  }

  private static BeamFieldConverter jdbcTypeToBeamFieldConverter(JDBCType jdbcType) {
    switch (jdbcType) {
      case ARRAY:
        return beamArrayField();
      case BIGINT:
        return beamFieldOfType(Schema.FieldType.INT64);
      case BINARY:
        return beamLogicalField(BINARY.getName(), LogicalTypes.FixedLengthBytes::of);
      case BIT:
        return beamFieldOfType(LogicalTypes.JDBC_BIT_TYPE);
      case BOOLEAN:
        return beamFieldOfType(Schema.FieldType.BOOLEAN);
      case CHAR:
        return beamLogicalField(CHAR.getName(), LogicalTypes.FixedLengthString::of);
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
        return beamLogicalField(LONGNVARCHAR.getName(), LogicalTypes.VariableLengthString::of);
      case LONGVARBINARY:
        return beamLogicalField(LONGVARBINARY.getName(), LogicalTypes.VariableLengthBytes::of);
      case LONGVARCHAR:
        return beamLogicalField(LONGVARCHAR.getName(), LogicalTypes.VariableLengthString::of);
      case NCHAR:
        return beamLogicalField(NCHAR.getName(), LogicalTypes.FixedLengthString::of);
      case NUMERIC:
        return beamLogicalNumericField(NUMERIC.getName());
      case NVARCHAR:
        return beamLogicalField(NVARCHAR.getName(), LogicalTypes.VariableLengthString::of);
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
        return beamLogicalField(VARBINARY.getName(), LogicalTypes.VariableLengthBytes::of);
      case VARCHAR:
        return beamLogicalField(VARCHAR.getName(), LogicalTypes.VariableLengthString::of);
      default:
        throw new UnsupportedOperationException(
            "Converting " + jdbcType + " to Beam schema type is not supported");
    }
  }

  /** Infers the Beam {@link Schema} from {@link ResultSetMetaData}. */
  static Schema toBeamSchema(ResultSetMetaData md) throws SQLException {
    Schema.Builder schemaBuilder = Schema.builder();

    for (int i = 1; i <= md.getColumnCount(); i++) {
      JDBCType jdbcType = valueOf(md.getColumnType(i));
      BeamFieldConverter fieldConverter = jdbcTypeToBeamFieldConverter(jdbcType);
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

  /** Converts numeric fields with specified precision and scale. */
  private static BeamFieldConverter beamLogicalNumericField(String identifier) {
    return (index, md) -> {
      int precision = md.getPrecision(index);
      int scale = md.getScale(index);
      Schema.FieldType fieldType =
          Schema.FieldType.logicalType(
              LogicalTypes.FixedPrecisionNumeric.of(identifier, precision, scale));
      return beamFieldOfType(fieldType).create(index, md);
    };
  }

  /** Converts array fields. */
  private static BeamFieldConverter beamArrayField() {
    return (index, md) -> {
      JDBCType elementJdbcType = valueOf(md.getColumnTypeName(index));
      BeamFieldConverter elementFieldConverter = jdbcTypeToBeamFieldConverter(elementJdbcType);

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
        Schema.FieldType elementType = fieldType.getCollectionElementType();
        ResultSetFieldExtractor elementExtractor = createFieldExtractor(elementType);
        return createArrayExtractor(elementExtractor);
      case DATETIME:
        return TIMESTAMP_EXTRACTOR;
      case LOGICAL_TYPE:
        return createLogicalTypeExtractor(fieldType.getLogicalType());
      default:
        if (!RESULTSET_FIELD_EXTRACTORS.containsKey(typeName)) {
          throw new UnsupportedOperationException(
              "BeamRowMapper does not have support for fields of type " + fieldType.toString());
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

      List<Object> arrayElements = new ArrayList<>();
      ResultSet arrayRs = arrayVal.getResultSet();
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
    JDBCType underlyingType = JDBCType.valueOf(logicalTypeName);
    switch (underlyingType) {
      case DATE:
        return DATE_EXTRACTOR;
      case TIME:
        return TIME_EXTRACTOR;
      case TIMESTAMP_WITH_TIMEZONE:
        return TIMESTAMP_EXTRACTOR;
      default:
        ResultSetFieldExtractor extractor = createFieldExtractor(fieldType.getBaseType());
        return (rs, index) -> fieldType.toInputType((BaseT) extractor.extract(rs, index));
    }
  }

  /** Convert SQL date type to Beam DateTime. */
  private static ResultSetFieldExtractor createDateExtractor() {
    return (rs, i) -> {
      Date date = rs.getDate(i, Calendar.getInstance(TimeZone.getTimeZone(ZoneOffset.UTC)));
      if (date == null) {
        return null;
      }
      ZonedDateTime zdt = ZonedDateTime.of(date.toLocalDate(), LocalTime.MIDNIGHT, ZoneOffset.UTC);
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

  /**
   * A {@link org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper} implementation that converts JDBC
   * results into Beam {@link Row} objects.
   */
  static final class BeamRowMapper implements JdbcIO.RowMapper<Row> {
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
        rowBuilder.addValue(fieldExtractors.get(i).extract(rs, i + 1));
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
   * compares two FieldType. Does not compare nullability.
   *
   * @param a FieldType 1
   * @param b FieldType 2
   * @return TRUE if FieldType are equal. Otherwise FALSE
   */
  static boolean compareSchemaFieldType(Schema.FieldType a, Schema.FieldType b) {
    if (a.getTypeName().equals(b.getTypeName())) {
      return !a.getTypeName().equals(Schema.TypeName.LOGICAL_TYPE)
          || compareSchemaFieldType(
              a.getLogicalType().getBaseType(), b.getLogicalType().getBaseType());
    } else if (a.getTypeName().isLogicalType()) {
      return a.getLogicalType().getBaseType().getTypeName().equals(b.getTypeName());
    } else if (b.getTypeName().isLogicalType()) {
      return b.getLogicalType().getBaseType().getTypeName().equals(a.getTypeName());
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
