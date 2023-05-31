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

import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformFormatProviders.applyCommonFileIOWriteFeatures;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.auto.service.AutoService;
import java.util.Optional;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroGenericCoder;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformConfiguration.ParquetConfiguration;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/** A {@link FileWriteSchemaTransformFormatProvider} for Parquet format. */
@AutoService(FileWriteSchemaTransformFormatProvider.class)
public class ParquetWriteSchemaTransformFormatProvider
    implements FileWriteSchemaTransformFormatProvider {

  private static final String SUFFIX =
      String.format(".%s", FileWriteSchemaTransformFormatProviders.PARQUET);

  @Override
  public String identifier() {
    return FileWriteSchemaTransformFormatProviders.PARQUET;
  }

  /**
   * Builds a {@link PTransform} that transforms a {@link Row} {@link PCollection} into result
   * {@link PCollection} file names written using {@link ParquetIO.Sink} and {@link FileIO.Write}.
   */
  @Override
  public PTransform<PCollection<Row>, PCollection<String>> buildTransform(
      FileWriteSchemaTransformConfiguration configuration, Schema schema) {
    return new PTransform<PCollection<Row>, PCollection<String>>() {
      @Override
      public PCollection<String> expand(PCollection<Row> input) {
        org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(schema);
        AvroGenericCoder coder = AvroGenericCoder.of(avroSchema);

        FileIO.Write<Void, GenericRecord> write =
            FileIO.<GenericRecord>write()
                .to(configuration.getFilenamePrefix())
                .via(buildSink(parquetConfiguration(configuration), schema))
                .withSuffix(SUFFIX);

        write = applyCommonFileIOWriteFeatures(write, configuration);

        return input
            .apply(
                "Row To GenericRecord",
                FileWriteSchemaTransformFormatProviders.mapRowsToGenericRecords(schema))
            .setCoder(coder)
            .apply("Write Parquet", write)
            .getPerDestinationOutputFilenames()
            .apply("perDestinationOutputFilenames", Values.create());
      }
    };
  }

  private ParquetIO.Sink buildSink(ParquetConfiguration configuration, Schema schema) {

    ParquetIO.Sink sink =
        ParquetIO.sink(AvroUtils.toAvroSchema(schema))
            .withCompressionCodec(
                CompressionCodecName.valueOf(configuration.getCompressionCodecName()));

    if (configuration.getRowGroupSize() != null) {
      int rowGroupSize = getRowGroupSize(configuration);
      // Python SDK external transforms do not support null values requiring additional check.
      if (rowGroupSize > 0) {
        sink = sink.withRowGroupSize(rowGroupSize);
      }
    }

    return sink;
  }

  private static ParquetConfiguration parquetConfiguration(
      FileWriteSchemaTransformConfiguration configuration) {
    // resolves Checker Framework incompatible argument for requireNonNull parameter
    Optional<ParquetConfiguration> parquetConfiguration =
        Optional.ofNullable(configuration.getParquetConfiguration());
    checkState(parquetConfiguration.isPresent());
    return parquetConfiguration.get();
  }

  private static Integer getRowGroupSize(ParquetConfiguration configuration) {
    // resolves Checker Framework [unboxing.of.nullable] unboxing a possibly-null reference
    Optional<Integer> rowGroupSize = Optional.ofNullable(configuration.getRowGroupSize());
    checkState(rowGroupSize.isPresent());
    return rowGroupSize.get();
  }
}
