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
package org.apache.beam.sdk.io.singlestore;

import static java.sql.Types.BIGINT;
import static java.sql.Types.BINARY;
import static java.sql.Types.BIT;
import static java.sql.Types.CHAR;
import static java.sql.Types.DATE;
import static java.sql.Types.DECIMAL;
import static java.sql.Types.DOUBLE;
import static java.sql.Types.INTEGER;
import static java.sql.Types.LONGVARBINARY;
import static java.sql.Types.LONGVARCHAR;
import static java.sql.Types.NULL;
import static java.sql.Types.REAL;
import static java.sql.Types.SMALLINT;
import static java.sql.Types.TIME;
import static java.sql.Types.TIMESTAMP;
import static java.sql.Types.TINYINT;
import static java.sql.Types.VARBINARY;
import static java.sql.Types.VARCHAR;
import static org.apache.beam.sdk.schemas.Schema.FieldType.BOOLEAN;
import static org.apache.beam.sdk.schemas.Schema.FieldType.BYTE;
import static org.apache.beam.sdk.schemas.Schema.FieldType.FLOAT;
import static org.apache.beam.sdk.schemas.Schema.FieldType.INT16;
import static org.apache.beam.sdk.schemas.Schema.FieldType.INT32;
import static org.apache.beam.sdk.schemas.Schema.FieldType.INT64;
import static org.apache.beam.sdk.schemas.Schema.FieldType.STRING;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.logicaltypes.VariableBytes;
import org.apache.beam.sdk.schemas.logicaltypes.VariableString;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.chrono.ISOChronology;

/** RowMapper that maps {@link ResultSet} row to the {@link Row}. */
class SingleStoreDefaultRowMapper
    implements SingleStoreIO.RowMapperWithInit<Row>, SingleStoreIO.RowMapperWithCoder<Row> {
  @Nullable Schema schema = null;
  List<ResultSetFieldConverter> converters = new ArrayList<>();

  @Override
  public void init(ResultSetMetaData metaData) throws SQLException {
    for (int i = 0; i < metaData.getColumnCount(); i++) {
      converters.add(ResultSetFieldConverter.of(metaData.getColumnType(i + 1)));
    }

    Schema.Builder schemaBuilder = new Schema.Builder();
    for (int i = 0; i < metaData.getColumnCount(); i++) {
      schemaBuilder.addField(converters.get(i).getSchemaField(metaData, i + 1));
    }
    this.schema = schemaBuilder.build();
  }

  @Override
  public Row mapRow(ResultSet resultSet) throws Exception {
    Schema schema = checkStateNotNull(this.schema, "mapRow is called before init");

    Row.Builder rowBuilder = Row.withSchema(schema);

    int fieldCount = schema.getFieldCount();
    for (int i = 0; i < fieldCount; i++) {
      Object value = converters.get(i).getValue(resultSet, i + 1);

      if (resultSet.wasNull() || value == null) {
        rowBuilder.addValue(null);
      } else {
        rowBuilder.addValue(value);
      }
    }

    return rowBuilder.build();
  }

  @Override
  public SchemaCoder<Row> getCoder() throws Exception {
    if (schema == null) {
      throw new UnsupportedOperationException("getCoder is called before init");
    }

    return RowCoder.of(this.schema);
  }

  abstract static class ResultSetFieldConverter implements Serializable {
    abstract @Nullable Object getValue(ResultSet rs, Integer index) throws SQLException;

    Schema.Field getSchemaField(ResultSetMetaData md, Integer index) throws SQLException {
      String label = md.getColumnLabel(index);
      return Schema.Field.of(label, getSchemaFieldType(md, index))
          .withNullable(md.isNullable(index) == java.sql.ResultSetMetaData.columnNullable);
    }

    abstract Schema.FieldType getSchemaFieldType(ResultSetMetaData md, Integer index)
        throws SQLException;

    /**
     * Interface implemented by functions that extract values of different types from a JDBC
     * ResultSet.
     */
    @FunctionalInterface
    interface ResultSetFieldExtractor extends Serializable {
      @Nullable
      Object extract(ResultSet rs, Integer index) throws SQLException;
    }

    static ResultSetFieldConverter of(int columnType) {
      switch (columnType) {
        case BIT:
          return new DirectResultSetFieldConverter(BOOLEAN, ResultSet::getBoolean);
        case TINYINT:
          return new DirectResultSetFieldConverter(BYTE, ResultSet::getByte);
        case SMALLINT:
          return new DirectResultSetFieldConverter(INT16, ResultSet::getShort);
        case INTEGER:
          return new DirectResultSetFieldConverter(INT32, ResultSet::getInt);
        case BIGINT:
          return new DirectResultSetFieldConverter(INT64, ResultSet::getLong);
        case REAL:
          return new DirectResultSetFieldConverter(FLOAT, ResultSet::getFloat);
        case DOUBLE:
          return new DirectResultSetFieldConverter(Schema.FieldType.DOUBLE, ResultSet::getDouble);
        case DECIMAL:
          return new DirectResultSetFieldConverter(
              Schema.FieldType.DECIMAL, ResultSet::getBigDecimal);
        case TIMESTAMP:
          return new TimestampResultSetFieldConverter();
        case DATE:
          return new DateResultSetFieldConverter();
        case TIME:
          return new TimeResultSetFieldConverter();
        case LONGVARBINARY:
        case VARBINARY:
        case BINARY:
          return new BinaryResultSetFieldConverter();
        case LONGVARCHAR:
        case VARCHAR:
        case CHAR:
          return new CharResultSetFieldConverter();
        case NULL:
          return new DirectResultSetFieldConverter(STRING, ResultSet::getString);
        default:
          throw new UnsupportedOperationException(
              "Converting " + columnType + " to Beam schema type is not supported");
      }
    }
  }

  static class DirectResultSetFieldConverter extends ResultSetFieldConverter {
    Schema.FieldType fieldType;
    ResultSetFieldExtractor extractor;

    public DirectResultSetFieldConverter(
        Schema.FieldType fieldType, ResultSetFieldExtractor extractor) {
      this.fieldType = fieldType;
      this.extractor = extractor;
    }

    @Override
    @Nullable
    Object getValue(ResultSet rs, Integer index) throws SQLException {
      return extractor.extract(rs, index);
    }

    @Override
    Schema.FieldType getSchemaFieldType(ResultSetMetaData md, Integer index) {
      return fieldType;
    }
  }

  static class CharResultSetFieldConverter extends ResultSetFieldConverter {
    @Override
    @Nullable
    Object getValue(ResultSet rs, Integer index) throws SQLException {
      return rs.getString(index);
    }

    @Override
    Schema.FieldType getSchemaFieldType(ResultSetMetaData md, Integer index) throws SQLException {
      int size = md.getPrecision(index);
      return Schema.FieldType.logicalType(VariableString.of(size));
    }
  }

  static class BinaryResultSetFieldConverter extends ResultSetFieldConverter {
    @Override
    @Nullable
    Object getValue(ResultSet rs, Integer index) throws SQLException {
      return rs.getBytes(index);
    }

    @Override
    Schema.FieldType getSchemaFieldType(ResultSetMetaData md, Integer index) throws SQLException {
      int size = md.getPrecision(index);
      return Schema.FieldType.logicalType(VariableBytes.of(size));
    }
  }

  static class TimestampResultSetFieldConverter extends ResultSetFieldConverter {
    @Override
    @Nullable
    Object getValue(ResultSet rs, Integer index) throws SQLException {
      Timestamp ts =
          rs.getTimestamp(index, Calendar.getInstance(TimeZone.getTimeZone(ZoneOffset.UTC)));
      if (ts == null) {
        return null;
      }
      return new DateTime(ts.toInstant().toEpochMilli(), ISOChronology.getInstanceUTC());
    }

    @Override
    Schema.FieldType getSchemaFieldType(ResultSetMetaData md, Integer index) {
      return Schema.FieldType.DATETIME;
    }
  }

  static class TimeResultSetFieldConverter extends ResultSetFieldConverter {
    @Override
    @Nullable
    Object getValue(ResultSet rs, Integer index) throws SQLException {
      Time time = rs.getTime(index, Calendar.getInstance(TimeZone.getTimeZone(ZoneOffset.UTC)));
      if (time == null) {
        return null;
      }
      return new DateTime(time.getTime(), ISOChronology.getInstanceUTC())
          .withDate(new LocalDate(0L));
    }

    @Override
    Schema.FieldType getSchemaFieldType(ResultSetMetaData md, Integer index) {
      return Schema.FieldType.DATETIME;
    }
  }

  static class DateResultSetFieldConverter extends ResultSetFieldConverter {
    @Override
    @Nullable
    Object getValue(ResultSet rs, Integer index) throws SQLException {
      // TODO(https://github.com/apache/beam/issues/19215) import when joda LocalDate is removed.
      java.time.LocalDate date = rs.getObject(index, java.time.LocalDate.class);
      if (date == null) {
        return null;
      }
      ZonedDateTime zdt = date.atStartOfDay(ZoneOffset.UTC);
      return new DateTime(zdt.toInstant().toEpochMilli(), ISOChronology.getInstanceUTC());
    }

    @Override
    Schema.FieldType getSchemaFieldType(ResultSetMetaData md, Integer index) {
      return Schema.FieldType.DATETIME;
    }
  }
}
