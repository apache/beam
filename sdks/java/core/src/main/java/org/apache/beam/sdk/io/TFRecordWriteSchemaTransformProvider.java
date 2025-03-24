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
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.providers.ErrorHandling;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.Element;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService(SchemaTransformProvider.class)
public class TFRecordWriteSchemaTransformProvider
    extends TypedSchemaTransformProvider<TFRecordWriteSchemaTransformConfiguration> {
  private static final String IDENTIFIER = "beam:schematransform:org.apache.beam:tfrecord_write:v1";
  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String ERROR = "errors";
  public static final TupleTag<KV<byte[], byte[]>> OUTPUT_TAG =
      new TupleTag<KV<byte[], byte[]>>() {};
  public static final TupleTag<Row> ERROR_TAG = new TupleTag<Row>() {};
  private static final Logger LOG =
      LoggerFactory.getLogger(TFRecordWriteSchemaTransformProvider.class);

  /** Returns the expected class of the configuration. */
  @Override
  protected Class<TFRecordWriteSchemaTransformConfiguration> configurationClass() {
    return TFRecordWriteSchemaTransformConfiguration.class;
  }

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
    return Arrays.asList(INPUT, ERROR);
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

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      // Validate configuration parameters
      configuration.validate();

      // Create basic transform
      TFRecordIO.Write writeTransform =
          TFRecordIO.write().withCompression(configuration.getCompression());

      // Add more parameters if not null
      String outputPrefix = configuration.getOutputPrefix();
      if (outputPrefix != null) {
        writeTransform = writeTransform.to(outputPrefix);
      }
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
      if (configuration.getNoSpilling()) {
        writeTransform = writeTransform.withNoSpilling();
      }

      // Obtain input schema and verify only one field and its bytes
      Schema inputSchema = input.get("input").getSchema();
      int numFields = inputSchema.getFields().size();
      if (numFields != 1) {
        throw new IllegalArgumentException("Expecting exactly one field, found " + numFields);
      } else if (!inputSchema.getField(0).getType().equals(Schema.FieldType.BYTES)) {
        throw new IllegalArgumentException(
            "The input schema must have exactly one field of type byte.");
      }

      // Obtaian schema field name
      final String schemaField;
      if (inputSchema.getField(0).getName() != null) {
        schemaField = inputSchema.getField(0).getName();
      } else {
        schemaField = "record";
      }

      PCollection<Row> inputRows = input.get(INPUT);

      // Convert Beam Rows to byte arrays
      SerializableFunction<Row, byte[]> rowToBytesFn = getRowToBytesFn(schemaField);

      PCollection<byte[]> byteArrays = inputRows.apply(ParDo.of(new RowToBytesDoFn(rowToBytesFn)));
     
      // Apply the write transform to byte arrays to write tfrecords to file.
      byteArrays.apply(writeTransform);

      return PCollectionRowTuple.empty(input.getPipeline());

    }
  }

  public static class LogRowDoFn extends DoFn<Row, Void> {
    @ProcessElement
    public void processElement(@Element Row row) {
      LOG.info("Row: {}", row.toString());
    }
  }

  public static class RowToBytesDoFn extends DoFn<Row, byte[]> {
    private final SerializableFunction<Row, byte[]> rowToBytesFn;

    public RowToBytesDoFn(SerializableFunction<Row, byte[]> rowToBytesFn) {
      this.rowToBytesFn = rowToBytesFn;
    }

    @ProcessElement
    public void processElement(@Element Row row, OutputReceiver<byte[]> out) {
      byte[] bytes = rowToBytesFn.apply(row);
      // System.out.printf("row: %s", bytes);
      // LOG.info("Byte array: {}", Arrays.toString(bytes));

      out.output(bytes);
    }
  }

  public static SerializableFunction<Row, byte[]> getRowToBytesFn(String rowFieldName) {
    return new SimpleFunction<Row, byte[]>() {
      @Override
      public byte[] apply(Row input) {
        byte[] rawBytes = input.getBytes(rowFieldName);
        // LOG.info("Byte array: {}", Arrays.toString(rawBytes));
        if (rawBytes == null) {
          throw new NullPointerException();
        }
        return rawBytes;
      }
    };
  }

  public static class ErrorCounterFn extends DoFn<Row, KV<byte[], byte[]>> {
    private final SerializableFunction<Row, byte[]> toBytesFn;
    private final Counter errorCounter;
    private Long errorsInBundle = 0L;
    private final boolean handleErrors;
    private final Schema errorSchema;

    public ErrorCounterFn(
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
      KV<byte[], byte[]> output = null;
      try {
        output = KV.of(new byte[1], toBytesFn.apply(row));
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
