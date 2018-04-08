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
import java.io.StringReader;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.util.stream.IntStream;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.util.NlsString;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

/**
 * Utility methods for working with {@code BeamTable}.
 *
 * <p>TODO: Does not yet support nested types.
 */
public final class BeamTableUtils {
  public static Row csvLine2BeamRow(
      CSVFormat csvFormat,
      String line,
      Schema schema) {

    try (StringReader reader = new StringReader(line)) {
      CSVParser parser = csvFormat.parse(reader);
      CSVRecord rawRecord = parser.getRecords().get(0);

      if (rawRecord.size() != schema.getFieldCount()) {
        throw new IllegalArgumentException(String.format(
            "Expect %d fields, but actually %d",
            schema.getFieldCount(), rawRecord.size()
        ));
      }

      return
          IntStream
              .range(0, schema.getFieldCount())
              .mapToObj(idx -> autoCastField(schema.getField(idx), rawRecord.get(idx)))
              .collect(toRow(schema));

    } catch (IOException e) {
      throw new IllegalArgumentException("decodeRecord failed!", e);
    }
  }

  public static String beamRow2CsvLine(Row row, CSVFormat csvFormat) {
    StringWriter writer = new StringWriter();
    try (CSVPrinter printer = csvFormat.print(writer)) {
      for (int i = 0; i < row.getFieldCount(); i++) {
        printer.print(row.getValue(i).toString());
      }
      printer.println();
    } catch (IOException e) {
      throw new IllegalArgumentException("encodeRecord failed!", e);
    }
    return writer.toString();
  }

  public static Object autoCastField(Schema.Field field, Object rawObj) {
    if (rawObj == null) {
      if (!field.getNullable()) {
        throw new IllegalArgumentException(String.format("Field %s not nullable", field.getName()));
      }
      return null;
    }

    TypeName type = field.getType().getTypeName();
    if (type.isStringType()) {
      if (rawObj instanceof NlsString) {
        return ((NlsString) rawObj).getValue();
      } else {
        return rawObj;
      }
    } else if (type.isNumericType()) {
      if ((rawObj instanceof String)
          || (rawObj instanceof BigDecimal && type != TypeName.DECIMAL)) {
        String raw = rawObj.toString();
        switch (type) {
          case BYTE:
            return Byte.valueOf(raw);
          case INT16:
            return Short.valueOf(raw);
          case INT32:
            return Integer.valueOf(raw);
          case INT64:
            return Long.valueOf(raw);
          case FLOAT:
            return Float.valueOf(raw);
          case DOUBLE:
            return Double.valueOf(raw);
          default:
            throw new UnsupportedOperationException(
                String.format("Column type %s is not supported yet!", type));
        }
      }
    }
    return rawObj;
  }
}
