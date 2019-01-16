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

import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.sql.impl.schema.BaseBeamTable;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.csv.CSVFormat;

/**
 * {@link TextTable} is a {@link org.apache.beam.sdk.extensions.sql.BeamSqlTable} that reads text
 * files and converts them according to the specified format.
 *
 * <p>Support formats are {@code "csv"} and {@code "lines"}.
 *
 * <p>{@link CSVFormat} itself has many dialects, check its javadoc for more info.
 */
@Internal
public class TextTable extends BaseBeamTable {

  private final PTransform<PCollection<String>, PCollection<Row>> readConverter;
  private final PTransform<PCollection<Row>, PCollection<String>> writeConverter;
  private final String filePattern;

  /** Text table with the specified read and write transforms. */
  public TextTable(
      Schema schema,
      String filePattern,
      PTransform<PCollection<String>, PCollection<Row>> readConverter,
      PTransform<PCollection<Row>, PCollection<String>> writeConverter) {
    super(schema);
    this.filePattern = filePattern;
    this.readConverter = readConverter;
    this.writeConverter = writeConverter;
  }

  public String getFilePattern() {
    return filePattern;
  }

  @Override
  public PCollection.IsBounded isBounded() {
    return PCollection.IsBounded.BOUNDED;
  }

  @Override
  public PCollection<Row> buildIOReader(PBegin begin) {
    return begin
        .apply("ReadTextFiles", TextIO.read().from(filePattern))
        .apply("StringToRow", readConverter);
  }

  @Override
  public PDone buildIOWriter(PCollection<Row> input) {
    return input
        .apply("RowToString", writeConverter)
        .apply("WriteTextFiles", TextIO.write().withDelimiter(new char[] {}).to(filePattern));
  }
}
