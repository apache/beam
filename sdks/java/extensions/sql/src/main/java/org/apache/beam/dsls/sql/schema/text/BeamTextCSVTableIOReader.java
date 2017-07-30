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

import static org.apache.beam.dsls.sql.schema.BeamTableUtils.csvLine2BeamSqlRow;

import java.io.Serializable;
import org.apache.beam.dsls.sql.schema.BeamSqlRow;
import org.apache.beam.dsls.sql.schema.BeamSqlRowType;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.csv.CSVFormat;

/**
 * IOReader for {@code BeamTextCSVTable}.
 */
public class BeamTextCSVTableIOReader
    extends PTransform<PCollection<String>, PCollection<BeamSqlRow>>
    implements Serializable {
  private String filePattern;
  protected BeamSqlRowType beamSqlRowType;
  protected CSVFormat csvFormat;

  public BeamTextCSVTableIOReader(BeamSqlRowType beamSqlRowType, String filePattern,
      CSVFormat csvFormat) {
    this.filePattern = filePattern;
    this.beamSqlRowType = beamSqlRowType;
    this.csvFormat = csvFormat;
  }

  @Override
  public PCollection<BeamSqlRow> expand(PCollection<String> input) {
    return input.apply(ParDo.of(new DoFn<String, BeamSqlRow>() {
          @ProcessElement
          public void processElement(ProcessContext ctx) {
            String str = ctx.element();
            ctx.output(csvLine2BeamSqlRow(csvFormat, str, beamSqlRowType));
          }
        }));
  }
}
