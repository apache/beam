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

import com.google.auto.service.AutoService;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.ByteArrayCoder;
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
public class TFRecordWriteSchemaTransformProvider
    extends TypedSchemaTransformProvider<TFRecordWriteSchemaTransformConfiguration> {
  private static final String IDENTIFIER = "beam:schematransform:org.apache.beam:tfrecord_write:v1";
  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String ERROR = "errors";
  public static final TupleTag<byte[]> OUTPUT_TAG = new TupleTag<byte[]>() {};
  public static final TupleTag<Row> ERROR_TAG = new TupleTag<Row>() {};
  private static final Logger LOG =
      LoggerFactory.getLogger(TFRecordWriteSchemaTransformProvider.class);

  /** Returns the expected {@link SchemaTransform} of the configuration. */
  @Override
  protected SchemaTransform from(TFRecordWriteSchemaTransformConfiguration configuration) {
    return new TFRecordWriteSchemaTransform(configuration);
  }

  /** Implementation of the {@link TypedSchemaTransformProvider} identifier method. */
  @Override
  public String identifier() {
    return IDENTIFIER;
  }

  /** Implementation of the {@link TypedSchemaTransformProvider} inputCollectionNames method. */
  @Override
  public List<String> inputCollectionNames() {
    return Arrays.asList(INPUT);
  }

  /** Implementation of the {@link TypedSchemaTransformProvider} outputCollectionNames method. */
  @Override
  public List<String> outputCollectionNames() {
    return Arrays.asList(OUTPUT, ERROR);
  }

  /**
   * An implementation of {@link SchemaTransform} for TFRecord Write jobs configured using {@link
   * TFRecordWriteSchemaTransformConfiguration}.
   */
  static class TFRecordWriteSchemaTransform extends SchemaTransform {
    private final TFRecordWriteSchemaTransformConfiguration configuration;

    TFRecordWriteSchemaTransform(TFRecordWriteSchemaTransformConfiguration configuration) {
      this.configuration = configuration;
    }

    public Row getConfigurationRow() {
      try {
        // To stay consistent with our SchemaTransform configuration naming conventions,
        // we sort lexicographically
        return SchemaRegistry.createDefault()
            .getToRowFunction(TFRecordWriteSchemaTransformConfiguration.class)
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

      // Create basic transform
      TFRecordIO.Write writeTransform =
          TFRecordIO.write().withCompression(Compression.valueOf(configuration.getCompression()));

      // Add more parameters if not null
      writeTransform = writeTransform.to(configuration.getOutputPrefix());
      String shardTemplate = configuration.getShardTemplate();
      if (shardTemplate != null) {
        writeTransform = writeTransform.withShardNameTemplate(shardTemplate);
      }
      String filenameSuffix = configuration.getFilenameSuffix();
      if (filenameSuffix != null) {
        writeTransform = writeTransform.withSuffix(filenameSuffix);
      }
      if (configuration.getNumShards() > 0) {
        writeTransform = writeTransform.withNumShards(configuration.getNumShards());
      } else {
        writeTransform = writeTransform.withoutSharding();
      }
      if (Boolean.TRUE.equals(configuration.getNoSpilling())) {
        writeTransform = writeTransform.withNoSpilling();
      }
      if (configuration.getMaxNumWritersPerBundle() != null) {
        writeTransform =
            writeTransform.withMaxNumWritersPerBundle(configuration.getMaxNumWritersPerBundle());
      }

      // Obtain input schema and verify only one field and its bytes
      Schema inputSchema = input.get(INPUT).getSchema();
      int numFields = inputSchema.getFields().size();
      if (numFields != 1) {
        throw new IllegalArgumentException("Expecting exactly one field, found " + numFields);
      } else if (!inputSchema.getField(0).getType().equals(Schema.FieldType.BYTES)) {
        throw new IllegalArgumentException(
            "The input schema must have exactly one field of type byte.");
      }

      final String schemaField;
      if (inputSchema.getField(0).getName() != null) {
        schemaField = inputSchema.getField(0).getName();
      } else {
        schemaField = "record";
      }

      PCollection<Row> inputRows = input.get(INPUT);

      // Convert Beam Rows to byte arrays
      SerializableFunction<Row, byte[]> rowToBytesFn = getRowToBytesFn(schemaField);

      Schema errorSchema = ErrorHandling.errorSchema(inputSchema);
      boolean handleErrors = ErrorHandling.hasOutput(configuration.getErrorHandling());

      // Apply row to bytes fn
      PCollectionTuple byteArrays =
          inputRows.apply(
              ParDo.of(
                      new ErrorFn(
                          "TFRecord-write-error-counter", rowToBytesFn, errorSchema, handleErrors))
                  .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));

      // Apply the write transform to byte arrays to write tfrecords to file.
      byteArrays.get(OUTPUT_TAG).setCoder(ByteArrayCoder.of()).apply(writeTransform);

      // Error handling
      String output = "";
      ErrorHandling errorHandler = configuration.getErrorHandling();
      if (errorHandler != null) {
        String outputHandler = errorHandler.getOutput();
        if (outputHandler != null) {
          output = outputHandler;
        } else {
          output = "";
        }
      }
      PCollection<Row> errorOutput =
          byteArrays.get(ERROR_TAG).setRowSchema(ErrorHandling.errorSchema(errorSchema));
      return PCollectionRowTuple.of(handleErrors ? output : "errors", errorOutput);
    }
  }

  public static SerializableFunction<Row, byte[]> getRowToBytesFn(String rowFieldName) {
    return new SimpleFunction<Row, byte[]>() {
      @Override
      public byte[] apply(Row input) {
        byte[] rawBytes = input.getBytes(rowFieldName);
        if (rawBytes == null) {
          throw new NullPointerException();
        }
        return rawBytes;
      }
    };
  }

  public static class ErrorFn extends DoFn<Row, byte[]> {
    private final SerializableFunction<Row, byte[]> toBytesFn;
    private final Counter errorCounter;
    private Long errorsInBundle = 0L;
    private final boolean handleErrors;
    private final Schema errorSchema;

    public ErrorFn(
        String name,
        SerializableFunction<Row, byte[]> toBytesFn,
        Schema errorSchema,
        boolean handleErrors) {
      this.toBytesFn = toBytesFn;
      this.errorCounter = Metrics.counter(TFRecordReadSchemaTransformProvider.class, name);
      this.handleErrors = handleErrors;
      this.errorSchema = errorSchema;
    }

    @ProcessElement
    public void process(@DoFn.Element Row row, MultiOutputReceiver receiver) {
      byte[] output = null;
      try {
        output = toBytesFn.apply(row);
      } catch (Exception e) {
        if (!handleErrors) {
          throw new RuntimeException(e);
        }
        errorsInBundle += 1;
        LOG.warn("Error while processing the element", e);
        receiver.get(ERROR_TAG).output(ErrorHandling.errorRecord(errorSchema, row, e));
      }
      if (output != null) {
        receiver.get(OUTPUT_TAG).output(output);
      }
    }

    @FinishBundle
    public void finish() {
      errorCounter.inc(errorsInBundle);
      errorsInBundle = 0L;
    }
  }
}
