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

package org.apache.beam.sdk.extensions.sql.meta.provider.text;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.Schema;
import org.apache.commons.csv.CSVFormat;

/**
 * {@code BeamTextCSVTable} is a {@code BeamTextTable} which formatted in CSV.
 *
 * <p>
 * {@link CSVFormat} itself has many dialects, check its javadoc for more info.
 * </p>
 */
public class BeamTextCSVTable extends BeamTextTable {

  private String filePattern;
  private CSVFormat csvFormat;

  /**
   * CSV table with {@link CSVFormat#DEFAULT DEFAULT} format.
   */
  public BeamTextCSVTable(Schema beamSchema, String filePattern)  {
    this(beamSchema, filePattern, CSVFormat.DEFAULT);
  }

  public BeamTextCSVTable(Schema schema, String filePattern,
                          CSVFormat csvFormat) {
    super(schema, filePattern);
    this.filePattern = filePattern;
    this.csvFormat = csvFormat;
  }

  @Override
  public PCollection<Row> buildIOReader(Pipeline pipeline) {
    return PBegin.in(pipeline).apply("decodeRecord", TextIO.read().from(filePattern))
        .apply("parseCSVLine",
            new BeamTextCSVTableIOReader(schema, filePattern, csvFormat));
  }

  @Override
  public PTransform<? super PCollection<Row>, PDone> buildIOWriter() {
    return new BeamTextCSVTableIOWriter(schema, filePattern, csvFormat);
  }

  public CSVFormat getCsvFormat() {
    return csvFormat;
  }

  public String getFilePattern() {
    return filePattern;
  }
}
