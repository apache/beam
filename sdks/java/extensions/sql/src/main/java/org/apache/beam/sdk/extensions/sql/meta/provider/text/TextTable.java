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

import java.io.IOException;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.sql.impl.BeamTableStatistics;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.SchemaBaseBeamTable;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.TextRowCountEstimator;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.csv.CSVFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link TextTable} is a {@link BeamSqlTable} that reads text files and converts them according to
 * the specified format.
 *
 * <p>Support formats are {@code "csv"} and {@code "lines"}.
 *
 * <p>{@link CSVFormat} itself has many dialects, check its javadoc for more info.
 */
@Internal
public class TextTable extends SchemaBaseBeamTable {

  private final PTransform<PCollection<String>, PCollection<Row>> readConverter;
  private final PTransform<PCollection<Row>, PCollection<String>> writeConverter;
  private static final TextRowCountEstimator.SamplingStrategy DEFAULT_SAMPLING_STRATEGY =
      new TextRowCountEstimator.LimitNumberOfTotalBytes(1024 * 1024L);
  private final String filePattern;
  private BeamTableStatistics rowCountStatistics = null;
  private static final Logger LOG = LoggerFactory.getLogger(TextTable.class);

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
  public BeamTableStatistics getTableStatistics(PipelineOptions options) {
    if (rowCountStatistics == null) {
      rowCountStatistics = getTextRowEstimate(options, getFilePattern());
    }

    return rowCountStatistics;
  }

  private static BeamTableStatistics getTextRowEstimate(
      PipelineOptions options, String filePattern) {
    TextRowCountEstimator textRowCountEstimator =
        TextRowCountEstimator.builder()
            .setFilePattern(filePattern)
            .setSamplingStrategy(DEFAULT_SAMPLING_STRATEGY)
            .build();
    try {
      Double rows = textRowCountEstimator.estimateRowCount(options);
      return BeamTableStatistics.createBoundedTableStatistics(rows);
    } catch (IOException | TextRowCountEstimator.NoEstimationException e) {
      LOG.warn("Could not get the row count for the text table " + filePattern, e);
    }
    return BeamTableStatistics.BOUNDED_UNKNOWN;
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
