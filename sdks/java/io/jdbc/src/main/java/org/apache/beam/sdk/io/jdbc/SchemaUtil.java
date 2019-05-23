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

import java.io.Serializable;
import java.sql.Array;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.schemas.LogicalTypes;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v20_0.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;

/** Provides utility functions for working with Beam {@link Schema} types. */
class SchemaUtil {
  private static final String SQL_DATE = "SqlDateType";
  private static final String SQL_TIME = "SqlTimeType";
  private static final String SQL_TIMESTAMP_WITH_LOCAL_TZ = "SqlTimestampWithLocalTzType";

  @VisibleForTesting
  static final Schema.FieldType SQL_DATE_LOGICAL_TYPE =
      Schema.FieldType.logicalType(
          new LogicalTypes.PassThroughLogicalType<Instant>(
              SQL_DATE, "", Schema.FieldType.DATETIME) {});

  @VisibleForTesting
  static final Schema.FieldType SQL_TIME_LOGICAL_TYPE =
      Schema.FieldType.logicalType(
          new LogicalTypes.PassThroughLogicalType<Instant>(
              SQL_TIME, "", Schema.FieldType.DATETIME) {});

  @VisibleForTesting
  static final Schema.FieldType SQL_TIMESTAMP_WITH_LOCAL_TZ_LOGICAL_TYPE =
      Schema.FieldType.logicalType(
          new LogicalTypes.PassThroughLogicalType<Instant>(
              SQL_TIMESTAMP_WITH_LOCAL_TZ, "", Schema.FieldType.DATETIME) {});

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

  /*
   * Mapping of primitive JDBC types to Beam schema types.
   * TODO: Add support for BLOB and CLOB types
   */
  private static final EnumMap<JDBCType, Schema.FieldType> JDBC_TO_BEAM_MAPPING =
      new EnumMap<>(
          ImmutableMap.<JDBCType, Schema.FieldType>builder()
              .put(JDBCType.BIGINT, Schema.FieldType.INT64)
              .put(JDBCType.BINARY, Schema.FieldType.BYTES)
              .put(JDBCType.BIT, Schema.FieldType.BOOLEAN)
              .put(JDBCType.BOOLEAN, Schema.FieldType.BOOLEAN)
              .put(JDBCType.CHAR, Schema.FieldType.STRING)
              .put(JDBCType.DECIMAL, Schema.FieldType.DECIMAL)
              .put(JDBCType.DOUBLE, Schema.FieldType.DOUBLE)
              .put(JDBCType.FLOAT, Schema.FieldType.FLOAT)
              .put(JDBCType.INTEGER, Schema.FieldType.INT32)
              .put(JDBCType.LONGVARBINARY, Schema.FieldType.BYTES)
              .put(JDBCType.LONGVARCHAR, Schema.FieldType.STRING)
              .put(JDBCType.NUMERIC, Schema.FieldType.DECIMAL)
              .put(JDBCType.REAL, Schema.FieldType.FLOAT)
              .put(JDBCType.SMALLINT, Schema.FieldType.INT16)
              .put(JDBCType.TIMESTAMP, Schema.FieldType.DATETIME)
              .put(JDBCType.TINYINT, Schema.FieldType.BYTE)
              .put(JDBCType.VARBINARY, Schema.FieldType.BYTES)
              .put(JDBCType.VARCHAR, Schema.FieldType.STRING)
              .build());

  /**
   * Interface implemented by functions that create Beam {@link
   * org.apache.beam.sdk.schemas.Schema.Field} corresponding to JDBC field metadata.
   */
  @FunctionalInterface
  interface BeamFieldConverter extends Serializable {
    Schema.Field create(int index, ResultSetMetaData md) throws SQLException;
  }

  /**
   * Infers the Beam {@link Schema} from {@link ResultSetMetaData}.
   *
   * <p>Only a subset of JDBC types are supported.
   */
  static Schema toBeamSchema(ResultSetMetaData md) throws SQLException {
    Schema.Builder schemaBuilder = Schema.builder();
    for (int i = 1; i <= md.getColumnCount(); i++) {
      JDBCType jdbcType = JDBCType.valueOf(md.getColumnType(i));
      Schema.Field field;
      switch (jdbcType) {
        case ARRAY:
          field = beamArrayField().create(i, md);
          break;
        case DATE:
          field = beamFieldOfType(SQL_DATE_LOGICAL_TYPE).create(i, md);
          break;
        case TIME:
          field = beamFieldOfType(SQL_TIME_LOGICAL_TYPE).create(i, md);
          break;
        case TIMESTAMP_WITH_TIMEZONE:
          field = beamFieldOfType(SQL_TIMESTAMP_WITH_LOCAL_TZ_LOGICAL_TYPE).create(i, md);
          break;
        default:
          if (!JDBC_TO_BEAM_MAPPING.containsKey(jdbcType)) {
            throw new UnsupportedOperationException(
                "Representing fields of type [" + jdbcType + "] in Beam schemas is not supported");
          }
          field = beamFieldOfType(JDBC_TO_BEAM_MAPPING.get(jdbcType)).create(i, md);
          break;
      }

      schemaBuilder.addField(field);
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

  /** Converts a JDBC array field to corresponding Beam schema array field. */
  private static BeamFieldConverter beamArrayField() {
    return (index, md) -> {
      JDBCType elementJdbcType = JDBCType.valueOf(md.getColumnTypeName(index));
      if (!JDBC_TO_BEAM_MAPPING.containsKey(elementJdbcType)) {
        throw new UnsupportedOperationException(
            "Arrays of type " + elementJdbcType.toString() + " are not supported");
      }

      String label = md.getColumnLabel(index);
      Schema.FieldType elementBeamType = JDBC_TO_BEAM_MAPPING.get(elementJdbcType);
      return Schema.Field.of(label, Schema.FieldType.array(elementBeamType))
          .withNullable(md.isNullable(index) == ResultSetMetaData.columnNullable);
    };
  }

  /** Creates a {@link ResultSetFieldExtractor} for the given type. */
  private static ResultSetFieldExtractor createFieldExtractor(Schema.FieldType fieldType) {
    Schema.TypeName typeName = fieldType.getTypeName();
    switch (typeName) {
      case ARRAY:
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
  private static ResultSetFieldExtractor createLogicalTypeExtractor(Schema.LogicalType fieldType) {
    String logicalTypeName = fieldType.getIdentifier();
    switch (logicalTypeName) {
      case SQL_DATE:
        return DATE_EXTRACTOR;
      case SQL_TIME:
        return TIME_EXTRACTOR;
      case SQL_TIMESTAMP_WITH_LOCAL_TZ:
        return TIMESTAMP_EXTRACTOR;
      default:
        throw new UnsupportedOperationException("Unknown logical type " + logicalTypeName);
    }
  }

  /** Convert SQL date type to Beam DateTime. */
  private static ResultSetFieldExtractor createDateExtractor() {
    return (rs, i) -> {
      Date date = rs.getDate(i);
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
      Time time = rs.getTime(i);
      if (time == null) {
        return null;
      }
      ZonedDateTime zdt =
          ZonedDateTime.of(LocalDate.ofEpochDay(0), time.toLocalTime(), ZoneOffset.systemDefault());
      return new DateTime(zdt.toInstant().toEpochMilli(), ISOChronology.getInstanceUTC());
    };
  }

  /** Convert SQL timestamp type to Beam DateTime. */
  private static ResultSetFieldExtractor createTimestampExtractor() {
    return (rs, i) -> {
      Timestamp ts = rs.getTimestamp(i);
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
}
