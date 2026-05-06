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
package org.apache.beam.sdk.io.mongodb;

import com.google.auto.service.AutoService;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.providers.ErrorHandling;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.bson.Document;
import org.checkerframework.checker.nullness.qual.Nullable;

/** An implementation of {@link TypedSchemaTransformProvider} for writing to MongoDB. */
@AutoService(SchemaTransformProvider.class)
public class MongoDbWriteSchemaTransformProvider
    extends TypedSchemaTransformProvider<MongoDbWriteSchemaTransformConfiguration> {

  private static final String INPUT_TAG = "input";
  public static final TupleTag<Document> OUTPUT_TAG = new TupleTag<Document>() {};
  public static final TupleTag<Row> ERROR_TAG = new TupleTag<Row>() {};

  private static final org.apache.beam.sdk.metrics.Counter errorCounter =
      org.apache.beam.sdk.metrics.Metrics.counter(
          MongoDbWriteSchemaTransformProvider.class, "MongoDB-write-error-counter");

  @Override
  protected SchemaTransform from(MongoDbWriteSchemaTransformConfiguration configuration) {
    return new MongoDbWriteSchemaTransform(configuration);
  }

  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:mongodb_write:v1";
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.singletonList(INPUT_TAG);
  }

  /** The {@link SchemaTransform} that performs the write operation. */
  private static class MongoDbWriteSchemaTransform extends SchemaTransform {
    private final MongoDbWriteSchemaTransformConfiguration configuration;

    MongoDbWriteSchemaTransform(MongoDbWriteSchemaTransformConfiguration configuration) {
      configuration.validate();
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      // Retrieve the input PCollection of Rows and its schema.
      PCollection<Row> rows = input.get(INPUT_TAG);
      org.apache.beam.sdk.schemas.Schema inputSchema = rows.getSchema();

      // Determine if error handling is enabled and set up the error schema.
      boolean handleErrors = ErrorHandling.hasOutput(configuration.getErrorHandling());
      org.apache.beam.sdk.schemas.Schema errorSchema = ErrorHandling.errorSchema(inputSchema);

      // Convert Beam Rows to BSON Documents, emitting errors to a separate tag if enabled.
      PCollectionTuple outputTuple =
          rows.apply(
              "ConvertToDocument",
              ParDo.of(new RowToBsonDocumentFn(handleErrors, errorSchema))
                  .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));

      PCollection<Document> documents = outputTuple.get(OUTPUT_TAG);

      // Configure the MongoDB write operation.
      MongoDbIO.Write write =
          MongoDbIO.write()
              .withUri(configuration.getUri())
              .withDatabase(configuration.getDatabase())
              .withCollection(configuration.getCollection());

      Long batchSize = configuration.getBatchSize();
      if (batchSize != null) {
        write = write.withBatchSize(batchSize);
      }

      // Apply the MongoDB write transform.
      documents.apply("WriteToMongo", write);

      // Extract and format the error collection.
      PCollection<Row> errorOutput = outputTuple.get(ERROR_TAG).setRowSchema(errorSchema);

      // Return the error collection as specified by the configuration.
      ErrorHandling errorHandling = configuration.getErrorHandling();
      return PCollectionRowTuple.of(
          (handleErrors && errorHandling != null) ? errorHandling.getOutput() : "errors",
          errorOutput);
    }
  }

  /** Converts a Beam {@link Row} to a BSON {@link Document}. */
  static class RowToBsonDocumentFn extends DoFn<Row, Document> {
    private final boolean handleErrors;
    private final org.apache.beam.sdk.schemas.Schema errorSchema;

    RowToBsonDocumentFn(boolean handleErrors, org.apache.beam.sdk.schemas.Schema errorSchema) {
      this.handleErrors = handleErrors;
      this.errorSchema = errorSchema;
    }

    @ProcessElement
    public void processElement(@Element Row row, MultiOutputReceiver receiver) {
      try {
        Object converted = convertToBsonValue(row);
        if (converted instanceof Document) {
          receiver.get(OUTPUT_TAG).output((Document) converted);
        } else {
          throw new IllegalStateException(
              "Expected Document but got "
                  + (converted != null ? converted.getClass().getName() : "null"));
        }
      } catch (Exception e) {
        if (!handleErrors) {
          throw new RuntimeException(e);
        }
        errorCounter.inc();
        receiver.get(ERROR_TAG).output(ErrorHandling.errorRecord(errorSchema, row, e));
      }
    }
  }

  private static @Nullable Object convertToBsonValue(@Nullable Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Row) {
      Row row = (Row) value;
      Document doc = new Document();
      for (Field field : row.getSchema().getFields()) {
        Object fieldValue = row.getValue(field.getName());
        Object convertedValue = convertToBsonValue(fieldValue);
        if (convertedValue != null) {
          doc.append(field.getName(), convertedValue);
        }
      }
      return doc;
    } else if (value instanceof List) {
      List<?> list = (List<?>) value;
      List<Object> bsonList = new ArrayList<>(list.size());
      for (Object item : list) {
        Object convertedItem = convertToBsonValue(item);
        if (convertedItem != null) {
          bsonList.add(convertedItem);
        }
      }
      return bsonList;
    } else if (value instanceof Iterable) {
      List<Object> bsonList = new ArrayList<>();
      for (Object item : (Iterable<?>) value) {
        Object convertedItem = convertToBsonValue(item);
        if (convertedItem != null) {
          bsonList.add(convertedItem);
        }
      }
      return bsonList;
    } else if (value instanceof Map) {
      Map<?, ?> map = (Map<?, ?>) value;
      Document doc = new Document();
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        Object convertedValue = convertToBsonValue(entry.getValue());
        if (convertedValue != null) {
          doc.append(String.valueOf(entry.getKey()), convertedValue);
        }
      }
      return doc;
    }
    return value;
  }
}
