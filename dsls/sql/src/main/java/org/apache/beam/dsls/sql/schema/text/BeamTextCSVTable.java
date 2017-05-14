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

import org.apache.beam.dsls.sql.schema.BeamSQLRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.commons.csv.CSVFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code BeamTextCSVTable} is a {@code BeamTextTable} which formatted in CSV.
 *
 * <p>
 * {@link CSVFormat} itself has many dialects, check its javadoc for more info.
 * </p>
 */
public class BeamTextCSVTable extends BeamTextTable {
  private static final Logger LOG = LoggerFactory
      .getLogger(BeamTextCSVTable.class);

  private CSVFormat csvFormat;

  /**
   * CSV table with {@link CSVFormat#DEFAULT DEFAULT} format.
   */
  public BeamTextCSVTable(RelProtoDataType protoDataType, String filePattern)  {
    this(protoDataType, filePattern, CSVFormat.DEFAULT);
  }

  public BeamTextCSVTable(RelProtoDataType protoDataType, String filePattern,
      CSVFormat csvFormat) {
    super(protoDataType, filePattern);
    this.csvFormat = csvFormat;
  }

  @Override
  public PCollection<BeamSQLRow> buildIOReader(Pipeline pipeline) {
    return PBegin.in(pipeline).apply("decodeRecord", TextIO.Read.from(filePattern))
        .apply("parseCSVLine",
            new BeamTextCSVTableIOReader(beamSqlRecordType, filePattern, csvFormat));
  }

  @Override
  public PTransform<? super PCollection<BeamSQLRow>, PDone> buildIOWriter() {
    return new BeamTextCSVTableIOWriter(beamSqlRecordType, filePattern, csvFormat);
  }
}
