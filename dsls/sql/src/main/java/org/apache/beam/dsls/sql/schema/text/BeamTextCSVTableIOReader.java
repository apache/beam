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

package org.apache.beam.dsls.sql.schema.text;

import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;

import org.apache.beam.dsls.sql.exception.BeamSqlUnsupportedException;
import org.apache.beam.dsls.sql.schema.BeamSQLRecordType;
import org.apache.beam.dsls.sql.schema.BeamSQLRow;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IOReader for {@code BeamTextCSVTable}.
 */
public class BeamTextCSVTableIOReader
    extends PTransform<PBegin, PCollection<BeamSQLRow>>
    implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(BeamTextCSVTableIOReader.class);
  private String filePattern;
  protected BeamSQLRecordType beamSqlRecordType;
  protected CSVFormat csvFormat;

  public BeamTextCSVTableIOReader(BeamSQLRecordType beamSqlRecordType, String filePattern,
      CSVFormat csvFormat) {
    this.filePattern = filePattern;
    this.beamSqlRecordType = beamSqlRecordType;
    this.csvFormat = csvFormat;
  }

  @Override
  public PCollection<BeamSQLRow> expand(PBegin input) {
    return input.apply("decodeRecord", TextIO.Read.from(filePattern))
        .apply(ParDo.of(new DoFn<String, BeamSQLRow>() {
          @ProcessElement
          public void processElement(ProcessContext ctx) {
            String str = ctx.element();

            try (StringReader reader = new StringReader(str)) {
              CSVRecord rawRecord = null;
              try {
                CSVParser parser = csvFormat.parse(reader);
                rawRecord = parser.getRecords().get(0);
              } catch (IOException e) {
                throw new IllegalArgumentException("Invalid text filePattern: " + filePattern, e);
              }

              BeamSQLRow row = new BeamSQLRow(beamSqlRecordType);
              if (rawRecord.size() != beamSqlRecordType.size()) {
                throw new IllegalArgumentException(String.format(
                    "Invalid filePattern: {}, expect %d fields, but actually %d", str,
                    filePattern, beamSqlRecordType.size(), rawRecord.size()
                ));
              } else {
                for (int idx = 0; idx < beamSqlRecordType.size(); idx++) {
                  String raw = rawRecord.get(idx);
                  addFieldWithAutoTypeCasting(row, idx, raw);
                }
                ctx.output(row);
              }
            }
          }
        }));
  }

  public void addFieldWithAutoTypeCasting(BeamSQLRow row, int idx, String raw) {
    SqlTypeName columnType = row.getDataType().getFieldsType().get(idx);
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
      case VARCHAR:
        row.addField(idx, raw);
        break;
      default:
        throw new BeamSqlUnsupportedException(String.format(
            "Column type %s is not supported yet!", columnType));
    }
  }
}
