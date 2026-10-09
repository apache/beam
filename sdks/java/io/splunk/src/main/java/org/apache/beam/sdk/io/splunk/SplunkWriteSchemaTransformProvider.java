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
package org.apache.beam.sdk.io.splunk;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.service.AutoService;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import java.net.HttpURLConnection;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.providers.ErrorHandling;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptors;

/**
 * An implementation of {@link TypedSchemaTransformProvider} for writing to Splunk's Http Event
 * Collector (HEC) using {@link SplunkIO}.
 *
 * <p>Each input row is converted to a {@link SplunkEvent}. The row must contain a string {@code
 * event} field, and may contain {@code time} (INT64), {@code host}, {@code source}, {@code
 * sourcetype} and {@code index} (strings) and {@code fields} (a row or a JSON object string). Any
 * other fields are ignored.
 */
@AutoService(SchemaTransformProvider.class)
public class SplunkWriteSchemaTransformProvider
    extends TypedSchemaTransformProvider<SplunkWriteSchemaTransformConfiguration> {
  private static final String IDENTIFIER = "beam:schematransform:org.apache.beam:splunk_write:v1";
  static final String INPUT = "input";
  static final String OUTPUT = "output";
  static final String ERROR = "errors";
  public static final TupleTag<Row> ERROR_TAG = new TupleTag<Row>() {};
  public static final TupleTag<SplunkEvent> EVENT_TAG = new TupleTag<SplunkEvent>() {};

  @Override
  protected Class<SplunkWriteSchemaTransformConfiguration> configurationClass() {
    return SplunkWriteSchemaTransformConfiguration.class;
  }

  /** Returns the expected {@link SchemaTransform} of the configuration. */
  @Override
  protected SchemaTransform from(SplunkWriteSchemaTransformConfiguration configuration) {
    return new SplunkWriteSchemaTransform(configuration);
  }

  /** Implementation of the {@link TypedSchemaTransformProvider} identifier method. */
  @Override
  public String identifier() {
    return IDENTIFIER;
  }

  /** Implementation of the {@link TypedSchemaTransformProvider} input collection names method. */
  @Override
  public List<String> inputCollectionNames() {
    return Collections.singletonList(INPUT);
  }

  /** Implementation of the {@link TypedSchemaTransformProvider} output collection names method. */
  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList(ERROR);
  }

  /**
   * An implementation of {@link SchemaTransform} for Splunk Write jobs configured using {@link
   * SplunkWriteSchemaTransformConfiguration}.
   */
  static class SplunkWriteSchemaTransform extends SchemaTransform {
    private final SplunkWriteSchemaTransformConfiguration configuration;

    SplunkWriteSchemaTransform(SplunkWriteSchemaTransformConfiguration configuration) {
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      configuration.validate();

      PCollection<Row> inputRows = input.get(INPUT);
      boolean handleErrors = ErrorHandling.hasOutput(configuration.getErrorHandling());

      Schema errorSchema =
          Schema.builder()
              .addNullableRowField("failed_row", inputRows.getSchema())
              .addNullableField("payload", Schema.FieldType.STRING)
              .addNullableField("statusCode", Schema.FieldType.INT32)
              .addNullableField("statusMessage", Schema.FieldType.STRING)
              .build();

      PCollectionTuple convertResult =
          inputRows.apply(
              "Convert to SplunkEvent",
              ParDo.of(new RowToEventFn(handleErrors, ERROR_TAG, errorSchema))
                  .withOutputTags(EVENT_TAG, TupleTagList.of(ERROR_TAG)));

      PCollection<SplunkEvent> splunkEvents =
          convertResult.get(EVENT_TAG).setCoder(SplunkEventCoder.of());
      PCollection<Row> conversionErrors =
          convertResult.get(ERROR_TAG).setCoder(RowCoder.of(errorSchema));

      SplunkIO.Write write = SplunkIO.write(configuration.getUrl(), configuration.getToken());
      Integer batchCount = configuration.getBatchCount();
      if (batchCount != null) {
        write = write.withBatchCount(batchCount);
      }
      Integer parallelism = configuration.getParallelism();
      if (parallelism != null) {
        write = write.withParallelism(parallelism);
      }
      Boolean disableCertificateValidation = configuration.getDisableCertificateValidation();
      if (disableCertificateValidation != null) {
        write = write.withDisableCertificateValidation(disableCertificateValidation);
      }
      String rootCaCertificatePath = configuration.getRootCaCertificatePath();
      if (rootCaCertificatePath != null) {
        write = write.withRootCaCertificatePath(rootCaCertificatePath);
      }
      Boolean enableBatchLogs = configuration.getEnableBatchLogs();
      if (enableBatchLogs != null) {
        write = write.withEnableBatchLogs(enableBatchLogs);
      }
      Boolean enableGzipHttpCompression = configuration.getEnableGzipHttpCompression();
      if (enableGzipHttpCompression != null) {
        write = write.withEnableGzipHttpCompression(enableGzipHttpCompression);
      }

      PCollection<SplunkWriteError> writeErrors = splunkEvents.apply("Write To Splunk", write);

      ErrorHandling errorHandling = configuration.getErrorHandling();
      if (handleErrors && errorHandling != null) {
        PCollection<Row> writeErrorRows =
            writeErrors
                .apply(
                    "Convert Write Errors to Rows",
                    MapElements.into(TypeDescriptors.rows())
                        .via(
                            error ->
                                Row.withSchema(errorSchema)
                                    .addValue(null)
                                    .addValue(error.payload())
                                    .addValue(error.statusCode())
                                    .addValue(error.statusMessage())
                                    .build()))
                .setCoder(RowCoder.of(errorSchema));

        PCollection<Row> allErrors =
            PCollectionList.of(conversionErrors)
                .and(writeErrorRows)
                .apply("Flatten Errors", Flatten.pCollections())
                .setCoder(RowCoder.of(errorSchema));

        return PCollectionRowTuple.of(errorHandling.getOutput(), allErrors);
      } else {
        writeErrors.apply("Fail on Write Error", ParDo.of(new FailOnWriteErrorFn()));
        PCollection<Row> emptyErrors =
            input
                .getPipeline()
                .apply("Empty Errors Placeholder", Create.empty(RowCoder.of(errorSchema)));
        return PCollectionRowTuple.of(ERROR, emptyErrors);
      }
    }
  }

  static SplunkEvent rowToEvent(Row row) {
    SplunkEvent.Builder builder = SplunkEvent.newBuilder();
    Schema schema = row.getSchema();

    Long time = schema.hasField(SplunkEvent.TIME) ? row.getInt64(SplunkEvent.TIME) : null;
    if (time != null) {
      builder.withTime(time);
    }
    String host = schema.hasField(SplunkEvent.HOST) ? row.getString(SplunkEvent.HOST) : null;
    if (host != null) {
      builder.withHost(host);
    }
    String source = schema.hasField(SplunkEvent.SOURCE) ? row.getString(SplunkEvent.SOURCE) : null;
    if (source != null) {
      builder.withSource(source);
    }
    String sourceType =
        schema.hasField(SplunkEvent.SOURCE_TYPE) ? row.getString(SplunkEvent.SOURCE_TYPE) : null;
    if (sourceType != null) {
      builder.withSourceType(sourceType);
    }
    String index = schema.hasField(SplunkEvent.INDEX) ? row.getString(SplunkEvent.INDEX) : null;
    if (index != null) {
      builder.withIndex(index);
    }
    Object fields = schema.hasField(SplunkEvent.FIELDS) ? row.getValue(SplunkEvent.FIELDS) : null;
    if (fields instanceof Row) {
      builder.withFields(toJsonElement(fields).getAsJsonObject());
    } else if (fields instanceof String) {
      builder.withFields(JsonParser.parseString((String) fields).getAsJsonObject());
    } else if (fields != null) {
      throw new IllegalArgumentException(
          "The fields field must be a row or a JSON object string, but got: "
              + schema.getField(SplunkEvent.FIELDS).getType());
    }
    String event = schema.hasField(SplunkEvent.EVENT) ? row.getString(SplunkEvent.EVENT) : null;
    builder.withEvent(checkNotNull(event, "Event is required."));

    return builder.create();
  }

  private static JsonElement toJsonElement(Object value) {
    if (value instanceof Row) {
      Row row = (Row) value;
      JsonObject json = new JsonObject();
      for (Schema.Field field : row.getSchema().getFields()) {
        Object fieldValue = row.getValue(field.getName());
        if (fieldValue != null) {
          json.add(field.getName(), toJsonElement(fieldValue));
        }
      }
      return json;
    } else if (value instanceof Iterable) {
      JsonArray json = new JsonArray();
      for (Object element : (Iterable<?>) value) {
        if (element != null) {
          json.add(toJsonElement(element));
        }
      }
      return json;
    } else if (value instanceof Number) {
      return new JsonPrimitive((Number) value);
    } else if (value instanceof Boolean) {
      return new JsonPrimitive((Boolean) value);
    }
    return new JsonPrimitive(value.toString());
  }

  static class RowToEventFn extends DoFn<Row, SplunkEvent> {
    private final boolean handleErrors;
    private final TupleTag<Row> errorOutputTag;
    private final Schema errorSchema;

    RowToEventFn(boolean handleErrors, TupleTag<Row> errorOutputTag, Schema errorSchema) {
      this.handleErrors = handleErrors;
      this.errorOutputTag = errorOutputTag;
      this.errorSchema = errorSchema;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      try {
        c.output(rowToEvent(c.element()));
      } catch (Exception e) {
        if (handleErrors) {
          String rowString = c.element().toString();
          String payload = rowString.length() <= 1024 ? rowString : rowString.substring(0, 1024);
          c.output(
              errorOutputTag,
              Row.withSchema(errorSchema)
                  .addValue(c.element())
                  .addValue(payload)
                  .addValue(HttpURLConnection.HTTP_BAD_REQUEST)
                  .addValue(e.getMessage())
                  .build());
        } else {
          throw new RuntimeException(e);
        }
      }
    }
  }

  /**
   * A {@link DoFn} that throws a {@link RuntimeException} when a write error is encountered,
   * causing the pipeline to fail. This is the default error handling behavior when no error output
   * is configured.
   */
  static class FailOnWriteErrorFn extends DoFn<SplunkWriteError, Void> {
    @ProcessElement
    public void processElement(@Element SplunkWriteError error) {
      String message = error.statusMessage();
      if (error.statusCode() != null) {
        throw new RuntimeException(
            String.format(
                "Splunk write failed with status code %d: %s", error.statusCode(), message));
      } else {
        throw new RuntimeException("Splunk write failed: " + message);
      }
    }
  }
}
