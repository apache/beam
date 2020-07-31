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
package org.apache.beam.sdk.extensions.sql.impl.schema;

import static org.apache.beam.sdk.values.Row.toRow;

import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.avatica.util.ByteString;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.util.NlsString;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.joda.time.DateTime;

/**
 * Utility methods for working with {@code BeamTable}.
 *
 * <p>TODO: Does not yet support nested types.
 */
public final class BeamTableUtils {

  /**
   * Decode zero or more CSV records from the given string, according to the specified {@link
   * CSVFormat}, and converts them to {@link Row Rows} with the specified {@link Schema}.
   *
   * <p>A single "line" read from e.g. {@link TextIO} can have zero or more records, depending on
   * whether the line was split on the same characters that delimite CSV records, and whether the
   * {@link CSVFormat} ignores blank lines.
   */
  public static Iterable<Row> csvLines2BeamRows(CSVFormat csvFormat, String line, Schema schema) {
    // Empty lines can result in empty strings after Beam splits the file,
    // which are not empty records to CSVParser unless they have a record terminator.
    if (!line.endsWith(csvFormat.getRecordSeparator())) {
      line += csvFormat.getRecordSeparator();
    }
    try (CSVParser parser = CSVParser.parse(line, csvFormat)) {
      List<Row> rows = new ArrayList<>();
      for (CSVRecord rawRecord : parser.getRecords()) {
        if (rawRecord.size() != schema.getFieldCount()) {
          throw new IllegalArgumentException(
              String.format(
                  "Expect %d fields, but actually %d", schema.getFieldCount(), rawRecord.size()));
        }
        rows.add(
            IntStream.range(0, schema.getFieldCount())
                .mapToObj(idx -> autoCastField(schema.getField(idx), rawRecord.get(idx)))
                .collect(toRow(schema)));
      }
      return rows;
    } catch (IOException e) {
      throw new IllegalArgumentException(
          String.format("Could not parse CSV records from %s with format %s", line, csvFormat), e);
    }
  }

  public static String beamRow2CsvLine(Row row, CSVFormat csvFormat) {
    StringWriter writer = new StringWriter();
    try (CSVPrinter printer = csvFormat.print(writer)) {
      for (int i = 0; i < row.getFieldCount(); i++) {
        printer.print(row.getBaseValue(i, Object.class).toString());
      }
      printer.println();
    } catch (IOException e) {
      throw new IllegalArgumentException("encodeRecord failed!", e);
    }
    return writer.toString();
  }

  /**
   * Attempt to cast an object to a specified Schema.Field.Type.
   *
   * @throws IllegalArgumentException if the value cannot be cast to that type.
   * @return The casted object in Schema.Field.Type.
   */
  public static Object autoCastField(Schema.Field field, Object rawObj) {
    if (rawObj == null) {
      if (!field.getType().getNullable()) {
        throw new IllegalArgumentException(String.format("Field %s not nullable", field.getName()));
      }
      return null;
    }

    FieldType type = field.getType();
    if (CalciteUtils.isStringType(type)) {
      if (rawObj instanceof NlsString) {
        return ((NlsString) rawObj).getValue();
      } else {
        return rawObj;
      }
    } else if (CalciteUtils.DATE.typesEqual(type) || CalciteUtils.NULLABLE_DATE.typesEqual(type)) {
      if (rawObj instanceof GregorianCalendar) { // used by the SQL CLI
        GregorianCalendar calendar = (GregorianCalendar) rawObj;
        return Instant.ofEpochMilli(calendar.getTimeInMillis())
            .atZone(calendar.getTimeZone().toZoneId())
            .toLocalDate();
      } else {
        return LocalDate.ofEpochDay((Integer) rawObj);
      }
    } else if (CalciteUtils.TIME.typesEqual(type) || CalciteUtils.NULLABLE_TIME.typesEqual(type)) {
      if (rawObj instanceof GregorianCalendar) { // used by the SQL CLI
        GregorianCalendar calendar = (GregorianCalendar) rawObj;
        return Instant.ofEpochMilli(calendar.getTimeInMillis())
            .atZone(calendar.getTimeZone().toZoneId())
            .toLocalTime();
      } else {
        return LocalTime.ofNanoOfDay((Long) rawObj);
      }
    } else if (CalciteUtils.isDateTimeType(type)) {
      // Internal representation of Date in Calcite is convertible to Joda's Datetime.
      return new DateTime(rawObj);
    } else if (type.getTypeName().isNumericType()
        && ((rawObj instanceof String)
            || (rawObj instanceof BigDecimal && type.getTypeName() != TypeName.DECIMAL))) {
      String raw = rawObj.toString();
      switch (type.getTypeName()) {
        case BYTE:
          return Byte.valueOf(raw);
        case INT16:
          return Short.valueOf(raw);
        case INT32:
          if (raw.equals("")) {
            return null;
          }
          return Integer.valueOf(raw);
        case INT64:
          if (raw.equals("")) {
            return null;
          }
          return Long.valueOf(raw);
        case FLOAT:
          if (raw.equals("")) {
            return null;
          }
          return Float.valueOf(raw);
        case DOUBLE:
          if (raw.equals("")) {
            return null;
          }
          return Double.valueOf(raw);
          //          BigDecimal bdvalue = new BigDecimal(raw);
          //          bdvalue = bdvalue.setScale(2);
          //          return bdvalue.doubleValue();
        case DECIMAL:
          // Decimal case is needed to handle very long decimal number and ensure precision, which
          // can't be simply achieved by double.
          if (raw.equals("")) {
            return null;
          }
          return new BigDecimal(raw);
        default:
          throw new UnsupportedOperationException(
              String.format("Column type %s is not supported yet!", type));
      }
    } else if (type.getTypeName().isPrimitiveType()) {
      if (TypeName.BYTES.equals(type.getTypeName()) && rawObj instanceof ByteString) {
        return ((ByteString) rawObj).getBytes();
      }
    }
    return rawObj;
  }
}
