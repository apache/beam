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

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.BeamRecordSqlType;
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils;
import org.apache.beam.sdk.values.BeamRecord;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

/**
 * Utility methods for working with {@code BeamTable}.
 */
public final class BeamTableUtils {
  public static BeamRecord csvLine2BeamRecord(
      CSVFormat csvFormat,
      String line,
      BeamRecordSqlType beamRecordSqlType) {
    List<Object> fieldsValue = new ArrayList<>(beamRecordSqlType.getFieldCount());
    try (StringReader reader = new StringReader(line)) {
      CSVParser parser = csvFormat.parse(reader);
      CSVRecord rawRecord = parser.getRecords().get(0);

      if (rawRecord.size() != beamRecordSqlType.getFieldCount()) {
        throw new IllegalArgumentException(String.format(
            "Expect %d fields, but actually %d",
            beamRecordSqlType.getFieldCount(), rawRecord.size()
        ));
      } else {
        for (int idx = 0; idx < beamRecordSqlType.getFieldCount(); idx++) {
          String raw = rawRecord.get(idx);
          fieldsValue.add(autoCastField(beamRecordSqlType.getFieldTypeByIndex(idx), raw));
        }
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("decodeRecord failed!", e);
    }
    return new BeamRecord(beamRecordSqlType, fieldsValue);
  }

  public static String beamRecord2CsvLine(BeamRecord row, CSVFormat csvFormat) {
    StringWriter writer = new StringWriter();
    try (CSVPrinter printer = csvFormat.print(writer)) {
      for (int i = 0; i < row.getFieldCount(); i++) {
        printer.print(row.getFieldValue(i).toString());
      }
      printer.println();
    } catch (IOException e) {
      throw new IllegalArgumentException("encodeRecord failed!", e);
    }
    return writer.toString();
  }

  public static Object autoCastField(int fieldType, Object rawObj) {
    if (rawObj == null) {
      return null;
    }

    SqlTypeName columnType = CalciteUtils.toCalciteType(fieldType);
    // auto-casting for numberics
    if ((rawObj instanceof String && SqlTypeName.NUMERIC_TYPES.contains(columnType))
        || (rawObj instanceof BigDecimal && columnType != SqlTypeName.DECIMAL)) {
      String raw = rawObj.toString();
      switch (columnType) {
        case TINYINT:
          return Byte.valueOf(raw);
        case SMALLINT:
          return Short.valueOf(raw);
        case INTEGER:
          return Integer.valueOf(raw);
        case BIGINT:
          return Long.valueOf(raw);
        case FLOAT:
          return Float.valueOf(raw);
        case DOUBLE:
          return Double.valueOf(raw);
        default:
          throw new UnsupportedOperationException(
              String.format("Column type %s is not supported yet!", columnType));
      }
    } else if (SqlTypeName.CHAR_TYPES.contains(columnType)) {
      // convert NlsString to String
      if (rawObj instanceof NlsString) {
        return ((NlsString) rawObj).getValue();
      } else {
        return rawObj;
      }
    } else {
      return rawObj;
    }
  }
}
