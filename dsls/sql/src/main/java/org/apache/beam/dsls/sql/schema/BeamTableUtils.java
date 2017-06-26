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

package org.apache.beam.dsls.sql.schema;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.math.BigDecimal;
import org.apache.beam.dsls.sql.utils.CalciteUtils;
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
  public static BeamSqlRow csvLine2BeamSqlRow(
      CSVFormat csvFormat,
      String line,
      BeamSqlRecordType beamSqlRecordType) {
    BeamSqlRow row = new BeamSqlRow(beamSqlRecordType);
    try (StringReader reader = new StringReader(line)) {
      CSVParser parser = csvFormat.parse(reader);
      CSVRecord rawRecord = parser.getRecords().get(0);

      if (rawRecord.size() != beamSqlRecordType.size()) {
        throw new IllegalArgumentException(String.format(
            "Expect %d fields, but actually %d",
            beamSqlRecordType.size(), rawRecord.size()
        ));
      } else {
        for (int idx = 0; idx < beamSqlRecordType.size(); idx++) {
          String raw = rawRecord.get(idx);
          addFieldWithAutoTypeCasting(row, idx, raw);
        }
      }
    } catch (IOException e) {
      throw new IllegalArgumentException("decodeRecord failed!", e);
    }
    return row;
  }

  public static String beamSqlRow2CsvLine(BeamSqlRow row, CSVFormat csvFormat) {
    StringWriter writer = new StringWriter();
    try (CSVPrinter printer = csvFormat.print(writer)) {
      for (int i = 0; i < row.size(); i++) {
        printer.print(row.getFieldValue(i).toString());
      }
      printer.println();
    } catch (IOException e) {
      throw new IllegalArgumentException("encodeRecord failed!", e);
    }
    return writer.toString();
  }

  public static void addFieldWithAutoTypeCasting(BeamSqlRow row, int idx, Object rawObj) {
    if (rawObj == null) {
      row.addField(idx, null);
      return;
    }

    SqlTypeName columnType = CalciteUtils.getFieldType(row.getDataType(), idx);
    // auto-casting for numberics
    if ((rawObj instanceof String && SqlTypeName.NUMERIC_TYPES.contains(columnType))
        || (rawObj instanceof BigDecimal && columnType != SqlTypeName.DECIMAL)) {
      String raw = rawObj.toString();
      switch (columnType) {
        case TINYINT:
          row.addField(idx, Byte.valueOf(raw));
          break;
        case SMALLINT:
          row.addField(idx, Short.valueOf(raw));
          break;
        case INTEGER:
          row.addField(idx, Integer.valueOf(raw));
          break;
        case BIGINT:
          row.addField(idx, Long.valueOf(raw));
          break;
        case FLOAT:
          row.addField(idx, Float.valueOf(raw));
          break;
        case DOUBLE:
          row.addField(idx, Double.valueOf(raw));
          break;
        default:
          throw new UnsupportedOperationException(
              String.format("Column type %s is not supported yet!", columnType));
      }
    } else if (SqlTypeName.CHAR_TYPES.contains(columnType)) {
      // convert NlsString to String
      if (rawObj instanceof NlsString) {
        row.addField(idx, ((NlsString) rawObj).getValue());
      } else {
        row.addField(idx, rawObj);
      }
    } else {
      // keep the origin
      row.addField(idx, rawObj);
    }
  }
}
