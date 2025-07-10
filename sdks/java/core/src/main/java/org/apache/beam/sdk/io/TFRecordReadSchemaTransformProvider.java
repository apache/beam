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
package org.apache.beam.sdk.io;

import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;

import com.google.auto.service.AutoService;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.providers.ErrorHandling;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService(SchemaTransformProvider.class)
public class TFRecordReadSchemaTransformProvider
    extends TypedSchemaTransformProvider<TFRecordReadSchemaTransformConfiguration> {
  private static final String IDENTIFIER = "beam:schematransform:org.apache.beam:tfrecord_read:v1";
  private static final String OUTPUT = "output";
  private static final String ERROR = "errors";
  public static final TupleTag<Row> OUTPUT_TAG = new TupleTag<Row>() {};
  public static final TupleTag<Row> ERROR_TAG = new TupleTag<Row>() {};
  private static final Logger LOG =
      LoggerFactory.getLogger(TFRecordReadSchemaTransformProvider.class);

  /** Returns the expected {@link SchemaTransform} of the configuration. */
  @Override
  protected SchemaTransform from(TFRecordReadSchemaTransformConfiguration configuration) {
    return new TFRecordReadSchemaTransform(configuration);
  }

  /** Implementation of the {@link TypedSchemaTransformProvider} identifier method. */
  @Override
  public String identifier() {
    return IDENTIFIER;
  }

  /** Implementation of the {@link TypedSchemaTransformProvider} outputCollectionNames method. */
  @Override
  public List<String> outputCollectionNames() {
    return Arrays.asList(OUTPUT, ERROR);
  }

  /**
   * An implementation of {@link SchemaTransform} for TFRecord read jobs configured using {@link
   * TFRecordReadSchemaTransformConfiguration}.
   */
  static class TFRecordReadSchemaTransform extends SchemaTransform {
    private final TFRecordReadSchemaTransformConfiguration configuration;

    TFRecordReadSchemaTransform(TFRecordReadSchemaTransformConfiguration configuration) {
      this.configuration = configuration;
    }

    public Row getConfigurationRow() {
      try {
        // To stay consistent with our SchemaTransform configuration naming conventions,
        // we sort lexicographically
        return SchemaRegistry.createDefault()
            .getToRowFunction(TFRecordReadSchemaTransformConfiguration.class)
            .apply(configuration)
            .sorted()
            .toSnakeCase();
      } catch (NoSuchSchemaException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      // Validate configuration parameters
      configuration.validate();

      TFRecordIO.Read readTransform =
          TFRecordIO.read().withCompression(Compression.valueOf(configuration.getCompression()));

      String filePattern = configuration.getFilePattern();
      if (filePattern != null) {
        readTransform = readTransform.from(filePattern);
      }
      if (!configuration.getValidate()) {
        readTransform = readTransform.withoutValidation();
      }

      // Read TFRecord files into a PCollection of byte arrays.
      PCollection<byte[]> tfRecordValues = input.getPipeline().apply(readTransform);

      // Define the schema for the row
      final Schema schema = Schema.of(Schema.Field.of("record", Schema.FieldType.BYTES));
      Schema errorSchema = ErrorHandling.errorSchemaBytes();
      boolean handleErrors = ErrorHandling.hasOutput(configuration.getErrorHandling());

      SerializableFunction<byte[], Row> bytesToRowFn = getBytesToRowFn(schema);

      // Apply bytes to row fn
      PCollectionTuple outputTuple =
          tfRecordValues.apply(
              ParDo.of(
                      new ErrorFn(
                          "TFRecord-read-error-counter", bytesToRowFn, errorSchema, handleErrors))
                  .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));

      PCollectionRowTuple outputRows =
          PCollectionRowTuple.of("output", outputTuple.get(OUTPUT_TAG).setRowSchema(schema));

      // Error handling
      PCollection<Row> errorOutput = outputTuple.get(ERROR_TAG).setRowSchema(errorSchema);
      if (handleErrors) {
        outputRows =
            outputRows.and(
                checkArgumentNotNull(configuration.getErrorHandling()).getOutput(), errorOutput);
      }
      return outputRows;
    }
  }

  public static SerializableFunction<byte[], Row> getBytesToRowFn(Schema schema) {
    return new SimpleFunction<byte[], Row>() {
      @Override
      public Row apply(byte[] input) {
        return Row.withSchema(schema).addValues(input).build();
      }
    };
  }

  public static class ErrorFn extends DoFn<byte[], Row> {
    private final SerializableFunction<byte[], Row> valueMapper;
    private final Counter errorCounter;
    private Long errorsInBundle = 0L;
    private final boolean handleErrors;
    private final Schema errorSchema;

    public ErrorFn(
        String name,
        SerializableFunction<byte[], Row> valueMapper,
        Schema errorSchema,
        boolean handleErrors) {
      this.errorCounter = Metrics.counter(TFRecordReadSchemaTransformProvider.class, name);
      this.valueMapper = valueMapper;
      this.handleErrors = handleErrors;
      this.errorSchema = errorSchema;
    }

    @ProcessElement
    public void process(@DoFn.Element byte[] msg, MultiOutputReceiver receiver) {
      Row mappedRow = null;
      try {
        mappedRow = valueMapper.apply(msg);
      } catch (Exception e) {
        if (!handleErrors) {
          throw new RuntimeException(e);
        }
        errorsInBundle += 1;
        LOG.warn("Error while parsing the element", e);
        receiver.get(ERROR_TAG).output(ErrorHandling.errorRecord(errorSchema, msg, e));
      }
      if (mappedRow != null) {
        receiver.get(OUTPUT_TAG).output(mappedRow);
      }
    }

    @FinishBundle
    public void finish(FinishBundleContext c) {
      errorCounter.inc(errorsInBundle);
      errorsInBundle = 0L;
    }
  }
}
