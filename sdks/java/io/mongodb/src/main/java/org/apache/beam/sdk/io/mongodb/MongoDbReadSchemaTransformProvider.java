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
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.providers.ErrorHandling;
import org.apache.beam.sdk.schemas.utils.JsonUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.bson.Document;

/** An implementation of {@link TypedSchemaTransformProvider} for reading from MongoDB. */
@AutoService(SchemaTransformProvider.class)
public class MongoDbReadSchemaTransformProvider
    extends TypedSchemaTransformProvider<MongoDbReadSchemaTransformConfiguration> {

  private static final String OUTPUT_TAG_NAME = "output";
  public static final TupleTag<Row> OUTPUT_TAG = new TupleTag<Row>() {};
  public static final TupleTag<Row> ERROR_TAG = new TupleTag<Row>() {};

  private static final org.apache.beam.sdk.metrics.Counter errorCounter =
      org.apache.beam.sdk.metrics.Metrics.counter(
          MongoDbReadSchemaTransformProvider.class, "MongoDB-read-error-counter");

  @Override
  protected SchemaTransform from(MongoDbReadSchemaTransformConfiguration configuration) {
    return new MongoDbReadSchemaTransform(configuration);
  }

  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:mongodb_read:v1";
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.emptyList();
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList(OUTPUT_TAG_NAME);
  }

  /** The {@link SchemaTransform} that performs the read operation. */
  private static class MongoDbReadSchemaTransform extends SchemaTransform {
    private final MongoDbReadSchemaTransformConfiguration configuration;

    MongoDbReadSchemaTransform(MongoDbReadSchemaTransformConfiguration configuration) {
      configuration.validate();
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      Schema schema = JsonUtils.beamSchemaFromJsonSchema(configuration.getSchema());

      MongoDbIO.Read read =
          MongoDbIO.read()
              .withUri(configuration.getUri())
              .withDatabase(configuration.getDatabase())
              .withCollection(configuration.getCollection());

      final String filterStr = configuration.getFilter();
      if (filterStr != null) {
        read = read.withQueryFn(FindQuery.create().withFilters(Document.parse(filterStr)));
      }

      PCollection<Document> mongoDocs = input.getPipeline().apply("ReadFromMongoDb", read);

      boolean handleErrors = ErrorHandling.hasOutput(configuration.getErrorHandling());
      Schema errorSchema = ErrorHandling.errorSchemaBytes();

      PCollectionTuple outputTuple =
          mongoDocs.apply(
              "ConvertToBeamRows",
              ParDo.of(new DocumentToRowFn(schema, handleErrors, errorSchema))
                  .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));

      PCollection<Row> beamRows = outputTuple.get(OUTPUT_TAG).setRowSchema(schema);
      PCollection<Row> errorOutput = outputTuple.get(ERROR_TAG).setRowSchema(errorSchema);

      PCollectionRowTuple output = PCollectionRowTuple.of(OUTPUT_TAG_NAME, beamRows);
      ErrorHandling errorHandling = configuration.getErrorHandling();
      if (handleErrors && errorHandling != null) {
        output = output.and(errorHandling.getOutput(), errorOutput);
      }
      return output;
    }
  }

  /** Converts a MongoDB BSON {@link Document} to a Beam {@link Row}. */
  static class DocumentToRowFn extends DoFn<Document, Row> {
    private final Schema schema;
    private final boolean handleErrors;
    private final Schema errorSchema;

    DocumentToRowFn(Schema schema, boolean handleErrors, Schema errorSchema) {
      this.schema = schema;
      this.handleErrors = handleErrors;
      this.errorSchema = errorSchema;
    }

    @ProcessElement
    public void processElement(@Element Document doc, MultiOutputReceiver receiver) {
      try {
        receiver.get(OUTPUT_TAG).output(MongoDbUtils.toRow(doc, schema));
      } catch (Exception e) {
        if (!handleErrors) {
          throw new RuntimeException(
              "Failed to convert BSON Document to Beam Row: " + doc.toJson(), e);
        }
        errorCounter.inc();
        byte[] docBytes;
        try {
          docBytes = doc.toJson().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        } catch (Exception jsonEx) {
          docBytes = doc.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }
        receiver.get(ERROR_TAG).output(ErrorHandling.errorRecord(errorSchema, docBytes, e));
      }
    }
  }
}
