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
import com.google.firestore.v1.Write;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.schemas.Schema;
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
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;

/** A {@link SchemaTransformProvider} for writing to Google Cloud Firestore. */
@AutoService(SchemaTransformProvider.class)
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class FirestoreWriteSchemaTransformProvider
    extends TypedSchemaTransformProvider<FirestoreWriteSchemaTransformConfiguration> {

  private static final String INPUT_TAG = "input";
  private static final String DEFAULT_DOCUMENT_ID_FIELD = "document_id";
  public static final TupleTag<Write> OUTPUT_TAG = new TupleTag<Write>() {};
  public static final TupleTag<Row> ERROR_TAG = new TupleTag<Row>() {};

  private static final org.apache.beam.sdk.metrics.Counter errorCounter =
      org.apache.beam.sdk.metrics.Metrics.counter(
          FirestoreWriteSchemaTransformProvider.class, "Firestore-write-error-counter");

  @Override
  protected SchemaTransform from(FirestoreWriteSchemaTransformConfiguration configuration) {
    return new FirestoreWriteSchemaTransform(configuration);
  }

  @Override
  public String identifier() {
    return "beam:schematransform:org.apache.beam:firestore_write:v1";
  }

  @Override
  public String description() {
    return "Writes Beam Rows to a Google Cloud Firestore collection.";
  }

  @Override
  public List<String> inputCollectionNames() {
    return Collections.singletonList(INPUT_TAG);
  }

  @Override
  public List<String> outputCollectionNames() {
    return Collections.emptyList();
  }

  private static class FirestoreWriteSchemaTransform extends SchemaTransform {
    private final FirestoreWriteSchemaTransformConfiguration configuration;

    FirestoreWriteSchemaTransform(FirestoreWriteSchemaTransformConfiguration configuration) {
      configuration.validate();
      this.configuration = configuration;
    }

    @Override
    public PCollectionRowTuple expand(PCollectionRowTuple input) {
      PCollection<Row> rows = input.get(INPUT_TAG);
      Schema inputSchema = rows.getSchema();
      String projectId = resolveProjectId(input.getPipeline());
      String databaseId = resolveDatabaseId(input.getPipeline());
      String documentIdField =
          Strings.isNullOrEmpty(configuration.getDocumentIdField())
              ? DEFAULT_DOCUMENT_ID_FIELD
              : configuration.getDocumentIdField();
      if (!inputSchema.hasField(documentIdField)) {
        throw new IllegalArgumentException(
            "Input schema must contain document id field: " + documentIdField);
      }

      boolean handleErrors = ErrorHandling.hasOutput(configuration.getErrorHandling());
      Schema errorSchema = ErrorHandling.errorSchema(inputSchema);

      PCollectionTuple outputTuple =
          rows.apply(
              "ConvertToFirestoreWrite",
              ParDo.of(
                      new RowToWriteFn(
                          inputSchema,
                          projectId,
                          databaseId,
                          configuration.getCollectionId(),
                          documentIdField,
                          handleErrors,
                          errorSchema))
                  .withOutputTags(OUTPUT_TAG, TupleTagList.of(ERROR_TAG)));

      FirestoreV1.Write write =
          FirestoreIO.v1().write().withProjectId(projectId).withDatabaseId(databaseId);

      outputTuple.get(OUTPUT_TAG).apply("WriteToFirestore", write.batchWrite().build());

      PCollection<Row> errorOutput = outputTuple.get(ERROR_TAG).setRowSchema(errorSchema);
      ErrorHandling errorHandling = configuration.getErrorHandling();
      return PCollectionRowTuple.of(
          (handleErrors && errorHandling != null) ? errorHandling.getOutput() : "errors",
          errorOutput);
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

  static class RowToWriteFn extends DoFn<Row, Write> {
    private final Schema schema;
    private final String projectId;
    private final String databaseId;
    private final String collectionId;
    private final String documentIdField;
    private final boolean handleErrors;
    private final Schema errorSchema;

    RowToWriteFn(
        Schema schema,
        String projectId,
        String databaseId,
        String collectionId,
        String documentIdField,
        boolean handleErrors,
        Schema errorSchema) {
      this.schema = schema;
      this.projectId = projectId;
      this.databaseId = databaseId;
      this.collectionId = collectionId;
      this.documentIdField = documentIdField;
      this.handleErrors = handleErrors;
      this.errorSchema = errorSchema;
    }

    @ProcessElement
    public void processElement(@Element Row row, MultiOutputReceiver receiver) {
      try {
        Document document =
            FirestoreUtils.rowToDocument(
                row, schema, projectId, databaseId, collectionId, documentIdField);
        receiver.get(OUTPUT_TAG).output(Write.newBuilder().setUpdate(document).build());
      } catch (Exception e) {
        if (!handleErrors) {
          throw new RuntimeException(e);
        }
        errorCounter.inc();
        receiver.get(ERROR_TAG).output(ErrorHandling.errorRecord(errorSchema, row, e));
      }
    }
  }
}
