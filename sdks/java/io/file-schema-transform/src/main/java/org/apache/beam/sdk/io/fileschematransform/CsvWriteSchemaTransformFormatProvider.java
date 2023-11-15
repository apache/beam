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
package org.apache.beam.sdk.io.fileschematransform;

import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviders.CSV;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviders.getCompression;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviders.getFilenameSuffix;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviders.getNumShards;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviders.getShardNameTemplate;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformProvider.RESULT_TAG;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.service.AutoService;
import java.util.Optional;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.io.csv.CsvIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;

/** A {@link FileWriteSchemaTransformFormatProvider} for CSV format. */
@AutoService(FileWriteSchemaTransformFormatProvider.class)
public class CsvWriteSchemaTransformFormatProvider
    implements FileWriteSchemaTransformFormatProvider {

  final String suffix = String.format(".%s", CSV);

  @Override
  public String identifier() {
    return CSV;
  }

  @Override
  public PTransform<PCollection<Row>, PCollectionTuple> buildTransform(
      FileWriteSchemaTransformConfiguration configuration, Schema schema) {

    return new PTransform<PCollection<Row>, PCollectionTuple>() {
      @Override
      public PCollectionTuple expand(PCollection<Row> input) {
        FileWriteSchemaTransformConfiguration.CsvConfiguration csvConfiguration =
            getCSVConfiguration(configuration);
        CSVFormat csvFormat =
            CSVFormat.Predefined.valueOf(csvConfiguration.getPredefinedCsvFormat()).getFormat();
        CsvIO.Write<Row> write =
            CsvIO.writeRows(configuration.getFilenamePrefix(), csvFormat).withSuffix(suffix);

        if (configuration.getCompression() != null) {
          write = write.withCompression(getCompression(configuration));
        }

        if (configuration.getNumShards() != null) {
          int numShards = getNumShards(configuration);
          // Python SDK external transforms do not support null values requiring additional check.
          if (numShards > 0) {
            write = write.withNumShards(numShards);
          }
        }

        if (!Strings.isNullOrEmpty(configuration.getShardNameTemplate())) {
          write = write.withShardTemplate(getShardNameTemplate(configuration));
        }

        if (!Strings.isNullOrEmpty(configuration.getFilenameSuffix())) {
          write = write.withSuffix(getFilenameSuffix(configuration));
        }

        WriteFilesResult<String> result = input.apply("Row to CSV", write);
        PCollection<String> output =
            result
                .getPerDestinationOutputFilenames()
                .apply("perDestinationOutputFilenames", Values.create());
        return PCollectionTuple.of(RESULT_TAG, output);
      }
    };
  }

  private FileWriteSchemaTransformConfiguration.CsvConfiguration getCSVConfiguration(
      FileWriteSchemaTransformConfiguration configuration) {
    // resolves Checker Framework incompatible argument for requireNonNull parameter
    Optional<FileWriteSchemaTransformConfiguration.CsvConfiguration> safeCsvConfiguration =
        Optional.ofNullable(configuration.getCsvConfiguration());
    checkState(safeCsvConfiguration.isPresent());
    return safeCsvConfiguration.get();
  }
}
