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
package org.apache.beam.sdk.io.gcp.firestore;

import com.google.auto.service.AutoService;
import com.google.firestore.v1.Document;
import com.google.firestore.v1.ListDocumentsRequest;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.providers.ErrorHandling;
import org.apache.beam.sdk.schemas.utils.JsonUtils;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;

/** A {@link SchemaTransformProvider} for reading from Google Cloud Firestore. */
@AutoService(SchemaTransformProvider.class)
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class FirestoreReadSchemaTransformProvider
    extends TypedSchemaTransformProvider<FirestoreReadSchemaTransformConfiguration> {

  private static final String OUTPUT_TAG_NAME = "output";
  public static final TupleTag<Row> OUTPUT_TAG = new TupleTag<Row>() {};
  public static final TupleTag<Row> ERROR_TAG = new TupleTag<Row>() {};

  private static final org.apache.beam.sdk.metrics.Counter errorCounter =
      org.apache.beam.sdk.metrics.Metrics.counter(
          FirestoreReadSchemaTransformProvider.class, "Firestore-read-error-counter");

  @Override
  protected SchemaTransform from(FirestoreReadSchemaTransformConfiguration configuration) {
    return new FirestoreReadSchemaTransform(configuration);
  }

  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:firestore_read:v1";
  }

  @Override
  public String description() {
    return "Reads documents from a Google Cloud Firestore collection and outputs Beam Rows.";
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.emptyList();
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.singletonList(OUTPUT_TAG_NAME);
  }

  private static class FirestoreReadSchemaTransform extends SchemaTransform {
    private final FirestoreReadSchemaTransformConfiguration configuration;

    FirestoreReadSchemaTransform(FirestoreReadSchemaTransformConfiguration configuration) {
      configuration.validate();
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      if (!input.getAll().isEmpty()) {
        throw new IllegalStateException(
            "Firestore read transform does not expect input PCollections.");
      }

      Schema schema = JsonUtils.beamSchemaFromJsonSchema(configuration.getSchema());
      String projectId = resolveProjectId(input.getPipeline());
      String databaseId = resolveDatabaseId(input.getPipeline());
      String parent = FirestoreUtils.documentsRoot(projectId, databaseId);

      PCollection<ListDocumentsRequest> requests =
          input
              .getPipeline()
              .apply("CreateCollectionId", Create.of(configuration.getCollectionId()))
              .apply(
                  "BuildListDocumentsRequest",
                  ParDo.of(
                      new DoFn<String, ListDocumentsRequest>() {
                        @ProcessElement
                        public void processElement(
                            @Element String collectionId,
                            OutputReceiver<ListDocumentsRequest> out) {
                          out.output(
                              ListDocumentsRequest.newBuilder()
                                  .setParent(parent)
                                  .setCollectionId(collectionId)
                                  .build());
                        }
                      }));

      FirestoreV1.ListDocuments.Builder readBuilder =
          FirestoreIO.v1()
              .read()
              .listDocuments()
              .withProjectId(projectId)
              .withDatabaseId(databaseId);

      PCollection<Document> documents = requests.apply("ReadFromFirestore", readBuilder.build());

      boolean handleErrors = ErrorHandling.hasOutput(configuration.getErrorHandling());
      Schema errorSchema = ErrorHandling.errorSchemaBytes();
      String documentIdField = schema.hasField("document_id") ? "document_id" : null;

      PCollectionTuple outputTuple =
          documents.apply(
              "ConvertToBeamRows",
              ParDo.of(new DocumentToRowFn(schema, documentIdField, handleErrors, errorSchema))
                  .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));

      PCollection<Row> rows = outputTuple.get(OUTPUT_TAG).setRowSchema(schema);
      PCollectionRowTuple output = PCollectionRowTuple.of(OUTPUT_TAG_NAME, rows);
      if (handleErrors && configuration.getErrorHandling() != null) {
        output =
            output.and(
                configuration.getErrorHandling().getOutput(),
                outputTuple.get(ERROR_TAG).setRowSchema(errorSchema));
      }
      return output;
    }

    private String resolveProjectId(org.apache.beam.sdk.Pipeline pipeline) {
      if (!Strings.isNullOrEmpty(configuration.getProjectId())) {
        return configuration.getProjectId();
      }
      FirestoreOptions firestoreOptions = pipeline.getOptions().as(FirestoreOptions.class);
      if (!Strings.isNullOrEmpty(firestoreOptions.getFirestoreProject())) {
        return firestoreOptions.getFirestoreProject();
      }
      String project = pipeline.getOptions().as(GcpOptions.class).getProject();
      if (Strings.isNullOrEmpty(project)) {
        throw new IllegalArgumentException(
            "Firestore project id must be set on the transform or pipeline options.");
      }
      return project;
    }

    private String resolveDatabaseId(org.apache.beam.sdk.Pipeline pipeline) {
      if (!Strings.isNullOrEmpty(configuration.getDatabaseId())) {
        return configuration.getDatabaseId();
      }
      return pipeline.getOptions().as(FirestoreOptions.class).getFirestoreDb();
    }
  }

  static class DocumentToRowFn extends DoFn<Document, Row> {
    private final Schema schema;
    private final @org.checkerframework.checker.nullness.qual.Nullable String documentIdField;
    private final boolean handleErrors;
    private final Schema errorSchema;

    DocumentToRowFn(
        Schema schema,
        @org.checkerframework.checker.nullness.qual.Nullable String documentIdField,
        boolean handleErrors,
        Schema errorSchema) {
      this.schema = schema;
      this.documentIdField = documentIdField;
      this.handleErrors = handleErrors;
      this.errorSchema = errorSchema;
    }

    @ProcessElement
    public void processElement(@Element Document document, MultiOutputReceiver receiver) {
      try {
        receiver
            .get(OUTPUT_TAG)
            .output(FirestoreUtils.documentToRow(document, schema, documentIdField));
      } catch (Exception e) {
        if (!handleErrors) {
          throw new RuntimeException(
              "Failed to convert Firestore document to Beam Row: " + document.getName(), e);
        }
        errorCounter.inc();
        receiver
            .get(ERROR_TAG)
            .output(
                ErrorHandling.errorRecord(
                    errorSchema, document.getName().getBytes(StandardCharsets.UTF_8), e));
      }
    }
  }
}
