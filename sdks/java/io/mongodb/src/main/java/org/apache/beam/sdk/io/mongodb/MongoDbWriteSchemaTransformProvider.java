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
import org.apache.beam.sdk.schemas.Schema.Field;
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

/**
 * An implementation of {@link TypedSchemaTransformProvider} for writing to MongoDB.
 *
 * <p><b>Internal only:</b> This class is actively being worked on, and it will likely change. We
 * provide no backwards compatibility guarantees, and it should not be implemented outside the Beam
 * repository.
 */
@AutoService(SchemaTransformProvider.class)
public class MongoDbWriteSchemaTransformProvider
    extends TypedSchemaTransformProvider<
        MongoDbWriteSchemaTransformProvider.MongoDbWriteSchemaTransformConfiguration> {

  private static final String INPUT_TAG = "input";

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

  /** Configuration class for the MongoDB Write transform. */
  @DefaultSchema(AutoValueSchema.class)
  @AutoValue
  public abstract static class MongoDbWriteSchemaTransformConfiguration implements Serializable {

    @SchemaFieldDescription("The connection URI for the MongoDB server.")
    public abstract String getUri();

    @SchemaFieldDescription("The MongoDB database to write to.")
    public abstract String getDatabase();

    @SchemaFieldDescription("The MongoDB collection to write to.")
    public abstract String getCollection();

    //    @SchemaFieldDescription("The number of documents to include in each batch write.")
    //    @Nullable
    //    public abstract Long getBatchSize();
    //
    //    @SchemaFieldDescription("Whether the writes should be performed in an ordered manner.")
    //    @Nullable
    //    public abstract Boolean getOrdered();

    public void validate() {
      checkArgument(getUri() != null && !getUri().isEmpty(), "MongoDB URI must be specified.");
      checkArgument(
          getDatabase() != null && !getDatabase().isEmpty(), "MongoDB database must be specified.");
      checkArgument(
          getCollection() != null && !getCollection().isEmpty(),
          "MongoDB collection must be specified.");
    }

    public static Builder builder() {
      return new AutoValue_MongoDbWriteSchemaTransformProvider_MongoDbWriteSchemaTransformConfiguration
          .Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setUri(String uri);

      public abstract Builder setDatabase(String database);

      public abstract Builder setCollection(String collection);

      public abstract Builder setBatchSize(Long batchSize);

      public abstract Builder setOrdered(Boolean ordered);

      public abstract MongoDbWriteSchemaTransformConfiguration build();
    }
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
      PCollection<Row> rows = input.get(INPUT_TAG);

      PCollection<Document> documents =
          rows.apply("ConvertToDocument", ParDo.of(new RowToBsonDocumentFn()));

      MongoDbIO.Write write =
          MongoDbIO.write()
              .withUri(configuration.getUri())
              .withDatabase(configuration.getDatabase())
              .withCollection(configuration.getCollection());

      //      if (configuration.getBatchSize() != null) {
      //        write = write.withBatchSize(configuration.getBatchSize());
      //      }
      //      if (configuration.getOrdered() != null) {
      //        write = write.withOrdered(configuration.getOrdered());
      //      }

      documents.apply("WriteToMongo", write);

      return PCollectionRowTuple.empty(input.getPipeline());
    }
  }

  /** A {@link DoFn} to convert a Beam {@link Row} to a MongoDB {@link Document}. */
  private static class RowToMongoDocumentFn extends DoFn<Row, Document> {
    @ProcessElement
    public void processElement(@Element Row row, OutputReceiver<Document> out) {
      Document doc = new Document();
      for (int i = 0; i < row.getSchema().getFieldCount(); i++) {
        String fieldName = row.getSchema().getField(i).getName();
        Object value = row.getValue(i);

        if (value != null) {
          doc.append(fieldName, value);
        }
      }
      out.output(doc);
    }
  }
  /** Converts a Beam {@link Row} to a BSON {@link Document}. */
  static class RowToBsonDocumentFn extends DoFn<Row, Document> {
    @ProcessElement
    public void processElement(@Element Row row, OutputReceiver<Document> out) {
      Document doc = new Document();
      for (Field field : row.getSchema().getFields()) {
        doc.append(field.getName(), row.getValue(field.getName()));
      }
      out.output(doc);
    }
  }
}
