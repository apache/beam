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
import java.io.StringWriter;

import org.apache.beam.dsls.sql.schema.BeamSQLRecordType;
import org.apache.beam.dsls.sql.schema.BeamSQLRow;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * IOWriter for {@code BeamTextCSVTable}.
 */
public class BeamTextCSVTableIOWriter extends PTransform<PCollection<BeamSQLRow>, PDone>
    implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(BeamTextCSVTableIOWriter.class);

  private String filePattern;
  protected BeamSQLRecordType beamSqlRecordType;
  protected CSVFormat csvFormat;

  public BeamTextCSVTableIOWriter(BeamSQLRecordType beamSqlRecordType, String filePattern,
      CSVFormat csvFormat) {
    this.filePattern = filePattern;
    this.beamSqlRecordType = beamSqlRecordType;
    this.csvFormat = csvFormat;
  }

  @Override public PDone expand(PCollection<BeamSQLRow> input) {
    return input.apply("encodeRecord", ParDo.of(new DoFn<BeamSQLRow, String>() {

      @ProcessElement public void processElement(ProcessContext ctx) {
        BeamSQLRow row = ctx.element();
        StringWriter writer = new StringWriter();

        try (CSVPrinter printer = csvFormat.print(writer)) {
          for (int i = 0; i < row.size(); i++) {
            printer.print(row.getFieldValue(i).toString());
          }
          printer.println();
        } catch (IOException e) {
          throw new IllegalArgumentException("Invalid filePattern: " + filePattern, e);
        }

        ctx.output(writer.toString());
      }
    })).apply(TextIO.Write.to(filePattern));
  }
}
