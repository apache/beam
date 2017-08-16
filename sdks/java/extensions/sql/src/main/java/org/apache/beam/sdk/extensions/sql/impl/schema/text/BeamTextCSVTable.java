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

package org.apache.beam.sdk.extensions.sql.impl.schema.text;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.BeamRecordSqlType;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.BeamRecord;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
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
  public BeamTextCSVTable(BeamRecordSqlType beamSqlRowType, String filePattern)  {
    this(beamSqlRowType, filePattern, CSVFormat.DEFAULT);
  }

  public BeamTextCSVTable(BeamRecordSqlType beamSqlRowType, String filePattern,
      CSVFormat csvFormat) {
    super(beamSqlRowType, filePattern);
    this.csvFormat = csvFormat;
  }

  @Override
  public PCollection<BeamRecord> buildIOReader(Pipeline pipeline) {
    return PBegin.in(pipeline).apply("decodeRecord", TextIO.read().from(filePattern))
        .apply("parseCSVLine",
            new BeamTextCSVTableIOReader(beamSqlRowType, filePattern, csvFormat));
  }

  @Override
  public PTransform<? super PCollection<BeamRecord>, PDone> buildIOWriter() {
    return new BeamTextCSVTableIOWriter(beamSqlRowType, filePattern, csvFormat);
  }
}
