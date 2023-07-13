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

import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformProvider.ERROR_SCHEMA;
import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformProvider.ERROR_TAG;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.Providers;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link FileWriteSchemaTransformFormatProviders} contains {@link
 * FileWriteSchemaTransformFormatProvider} implementations.
 *
 * <p>The design goals of this class are to enable clean {@link
 * FileWriteSchemaTransformConfiguration#getFormat()} lookups mapping to the appropriate {@link
 * org.apache.beam.sdk.io.FileIO.Write} that encodes the file data into the configured format.
 */
@Internal
public final class FileWriteSchemaTransformFormatProviders {
  static final String AVRO = "avro";
  static final String CSV = "csv";
  static final String JSON = "json";
  static final String PARQUET = "parquet";
  static final String XML = "xml";
  private static final Logger LOG =
      LoggerFactory.getLogger(FileWriteSchemaTransformFormatProviders.class);

  /** Load all {@link FileWriteSchemaTransformFormatProvider} implementations. */
  public static Map<String, FileWriteSchemaTransformFormatProvider> loadProviders() {
    return Providers.loadProviders(FileWriteSchemaTransformFormatProvider.class);
  }

  /** Builds a {@link MapElements}i transform to map {@link Row}s to {@link GenericRecord}s. */
  static MapElements<Row, GenericRecord> mapRowsToGenericRecords(Schema beamSchema) {
    return MapElements.into(TypeDescriptor.of(GenericRecord.class))
        .via(AvroUtils.getRowToGenericRecordFunction(AvroUtils.toAvroSchema(beamSchema)));
  }

  // Applies generic mapping from Beam row to other data types through the provided mapFn.
  // Implemenets error handling with metrics and DLQ support.
  // Arguments:
  //    name: the metric name to use.
  //    mapFn: the mapping function for mapping from Beam row to other data types.
  //    outputTag: TupleTag for output. Used to direct output to correct output source, or in the
  //        case of error, a DLQ.
  static class BeamRowMapperWithDlq<OutputT extends Object> extends DoFn<Row, OutputT> {
    private SerializableFunction<Row, OutputT> mapFn;
    private Counter errorCounter;
    private TupleTag<OutputT> outputTag;
    private long errorsInBundle = 0L;

    public BeamRowMapperWithDlq(
        String name, SerializableFunction<Row, OutputT> mapFn, TupleTag<OutputT> outputTag) {
      errorCounter = Metrics.counter(FileWriteSchemaTransformFormatProvider.class, name);
      this.mapFn = mapFn;
      this.outputTag = outputTag;
    }

    @ProcessElement
    public void process(@DoFn.Element Row row, MultiOutputReceiver receiver) {
      try {
        receiver.get(outputTag).output(mapFn.apply(row));
      } catch (Exception e) {
        errorsInBundle += 1;
        LOG.warn("Error while parsing input element", e);
        receiver
            .get(ERROR_TAG)
            .output(
                Row.withSchema(ERROR_SCHEMA)
                    .addValues(e.toString(), row.toString().getBytes(StandardCharsets.UTF_8))
                    .build());
      }
    }

    @FinishBundle
    public void finish() {
      errorCounter.inc(errorsInBundle);
      errorsInBundle = 0L;
    }
  }

  /**
   * Applies common parameters from {@link FileWriteSchemaTransformConfiguration} to {@link
   * FileIO.Write}.
   */
  static <T> FileIO.Write<Void, T> applyCommonFileIOWriteFeatures(
      FileIO.Write<Void, T> write, FileWriteSchemaTransformConfiguration configuration) {

    if (!Strings.isNullOrEmpty(configuration.getFilenameSuffix())) {
      write = write.withSuffix(getFilenameSuffix(configuration));
    }

    if (configuration.getNumShards() != null) {
      int numShards = getNumShards(configuration);
      // Python SDK external transforms do not support null values requiring additional check.
      if (numShards > 0) {
        write = write.withNumShards(numShards);
      }
    }

    if (!Strings.isNullOrEmpty(configuration.getCompression())) {
      write = write.withCompression(getCompression(configuration));
    }

    return write;
  }

  /**
   * Applies common parameters from {@link FileWriteSchemaTransformConfiguration} to {@link
   * TextIO.Write}.
   */
  static TextIO.Write applyCommonTextIOWriteFeatures(
      TextIO.Write write, FileWriteSchemaTransformConfiguration configuration) {
    write = write.to(configuration.getFilenamePrefix());

    if (!Strings.isNullOrEmpty(configuration.getFilenameSuffix())) {
      write = write.withSuffix(getFilenameSuffix(configuration));
    }

    if (!Strings.isNullOrEmpty(configuration.getCompression())) {
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
      write = write.withShardNameTemplate(getShardNameTemplate(configuration));
    }

    return write;
  }

  static Compression getCompression(FileWriteSchemaTransformConfiguration configuration) {
    // resolves Checker Framework incompatible argument for valueOf parameter
    Optional<String> compression = Optional.ofNullable(configuration.getCompression());
    checkState(compression.isPresent());
    return Compression.valueOf(compression.get());
  }

  static String getFilenameSuffix(FileWriteSchemaTransformConfiguration configuration) {
    // resolves Checker Framework incompatible argument for parameter suffix of withSuffix
    Optional<String> suffix = Optional.ofNullable(configuration.getFilenameSuffix());
    checkState(suffix.isPresent());
    return suffix.get();
  }

  static Integer getNumShards(FileWriteSchemaTransformConfiguration configuration) {
    // resolves Checker Framework unboxing a possibly-null reference
    Optional<Integer> numShards = Optional.ofNullable(configuration.getNumShards());
    checkState(numShards.isPresent());
    return numShards.get();
  }

  static String getShardNameTemplate(FileWriteSchemaTransformConfiguration configuration) {
    // resolves Checker Framework incompatible null argument for parameter shardTemplate
    Optional<String> shardNameTemplate = Optional.ofNullable(configuration.getShardNameTemplate());
    checkState(shardNameTemplate.isPresent());
    return shardNameTemplate.get();
  }
}
