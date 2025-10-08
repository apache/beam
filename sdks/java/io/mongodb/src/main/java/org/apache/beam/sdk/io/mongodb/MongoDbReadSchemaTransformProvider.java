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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.service.AutoService;
import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.SchemaTransform;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.TypedSchemaTransformProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.bson.Document;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * An implementation of {@link TypedSchemaTransformProvider} for reading from MongoDB.
 *
 * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
 * provide no backwards compatibility guarantees, and it should not be implemented outside the Beam
 * repository.
 */
@AutoService(SchemaTransformProvider.class)
public class MongoDbReadSchemaTransformProvider
    extends TypedSchemaTransformProvider<
        MongoDbReadSchemaTransformProvider.MongoDbReadSchemaTransformConfiguration> {

  private static final String OUTPUT_TAG = "output";

  @Override
  protected Class<MongoDbReadSchemaTransformConfiguration> configurationClass() {
    return MongoDbReadSchemaTransformConfiguration.class;
  }

  @Override
  protected SchemaTransform from(MongoDbReadSchemaTransformConfiguration configuration) {
    return new MongoDbReadSchemaTransform(configuration);
  }

  @Override
  public String identifier() {
    // Return a unique URN for the transform.
    return "beam:schematransform:org.apache.beam:mongodb_read:v1";
  }

  @Override
  public List<String> inputCollectionNames() {
    // A read transform does not have an input PCollection.
    return Collections.emptyList();
  }

  @Override
  public List<String> outputCollectionNames() {
    // The primary output is a PCollection of Rows.
    // Error handling could be added later with a second "errors" output tag.
    return Collections.singletonList(OUTPUT_TAG);
  }

  /** Configuration class for the MongoDB Read transform. */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class MongoDbReadSchemaTransformConfiguration implements Serializable {

    @SchemaFieldDescription("The connection URI for the MongoDB server.")
    public abstract String getUri();

    @SchemaFieldDescription("The MongoDB database to read from.")
    public abstract String getDatabase();

    @SchemaFieldDescription("The MongoDB collection to read from.")
    public abstract String getCollection();

    @SchemaFieldDescription(
        "An optional BSON filter to apply to the read. This should be a valid JSON string.")
    @Nullable
    public abstract String getFilter();

    public void validate() {
      checkArgument(getUri() != null && !getUri().isEmpty(), "MongoDB URI must be specified.");
      checkArgument(
          getDatabase() != null && !getDatabase().isEmpty(), "MongoDB database must be specified.");
      checkArgument(
          getCollection() != null && !getCollection().isEmpty(),
          "MongoDB collection must be specified.");
    }

    public static Builder builder() {
      return new AutoValue_MongoDbReadSchemaTransformProvider_MongoDbReadSchemaTransformConfiguration
          .Builder();
    }

    /** Builder for the {@link MongoDbReadSchemaTransformConfiguration}. */
    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setUri(String uri);

      public abstract Builder setDatabase(String database);

      public abstract Builder setCollection(String collection);

      public abstract Builder setFilter(String filter);

      public abstract MongoDbReadSchemaTransformConfiguration build();
    }
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
      // A read transform does not have an input, so we start with the pipeline.
      PCollection<Document> mongoDocs =
          input
              .getPipeline()
              .apply(
                  "ReadFromMongoDb",
                  MongoDbIO.read()
                      .withUri(configuration.getUri())
                      .withDatabase(configuration.getDatabase())
                      .withCollection(configuration.getCollection()));
      // TODO: Add support for .withFilter() if it exists in your MongoDbIO,
      // using configuration.getFilter().

      // Convert the BSON Document objects into Beam Row objects.
      PCollection<Row> beamRows =
          mongoDocs.apply("ConvertToBeamRows", ParDo.of(new MongoDocumentToRowFn()));

      return PCollectionRowTuple.of(OUTPUT_TAG, beamRows);
    }
  }

  /**
   * A {@link DoFn} to convert a MongoDB {@link Document} to a Beam {@link Row}.
   *
   * <p>This is a critical step to ensure data is in a schema-aware format.
   */
  private static class MongoDocumentToRowFn extends DoFn<Document, Row> {
    // TODO: Define the Beam Schema that corresponds to your MongoDB documents.
    // This could be made dynamic based on an inferred schema or a user-provided schema.
    // For this skeleton, we assume a static schema.
    // public static final Schema OUTPUT_SCHEMA = Schema.builder()...build();

    @ProcessElement
    public void processElement(@Element Document doc, OutputReceiver<Row> out) {
      // Here you will convert the BSON document to a Beam Row.
      // This requires you to know the target schema.

      // Example pseudo-code:
      // Row.Builder rowBuilder = Row.withSchema(OUTPUT_SCHEMA);
      // for (Map.Entry<String, Object> entry : doc.entrySet()) {
      //   rowBuilder.addValue(entry.getValue());
      // }
      // out.output(rowBuilder.build());

      // For a robust implementation, you would handle data type conversions
      // between BSON types and Beam schema types.
      throw new UnsupportedOperationException(
          "MongoDocumentToRowFn must be implemented to convert MongoDB Documents to Beam Rows.");
    }
  }
}
