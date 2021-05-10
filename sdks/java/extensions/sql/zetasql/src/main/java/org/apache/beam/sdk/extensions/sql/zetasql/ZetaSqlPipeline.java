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
package org.apache.beam.sdk.extensions.sql.zetasql;

import com.google.api.services.bigquery.model.TableReference;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.datacatalog.v1.ColumnSchema;
import com.google.cloud.datacatalog.v1.DataCatalogClient;
import com.google.cloud.datacatalog.v1.Entry;
import com.google.cloud.datacatalog.v1.LookupEntryRequest;
import com.google.cloud.datacatalog.v1.PhysicalSchema;
import com.google.cloud.datacatalog.v1.PhysicalSchema.AvroSchema;
import com.google.cloud.datacatalog.v1.Schema;
import com.google.cloud.datacatalog.v1.UpdateEntryRequest;
import com.google.cloud.pubsub.v1.Publisher;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.protobuf.ByteString;
import com.google.protobuf.FieldMask;
import com.google.pubsub.v1.Encoding;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlPipelineOptions;
import org.apache.beam.sdk.extensions.sql.meta.provider.datacatalog.DataCatalogPipelineOptions;
import org.apache.beam.sdk.extensions.sql.meta.provider.datacatalog.DataCatalogTableProvider;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

// Add dependency to Data Catalog and Dataflow Runner
// compile project(":sdks:java:extensions:sql:datacatalog")
// compile project(":runners:google-cloud-dataflow-java")
//
// To run as a gradle task
// task execute(type: JavaExec) {
//     main = "org.apache.beam.sdk.extensions.sql.zetasql.ZetaSqlPipeline"
//     classpath = sourceSets.main.runtimeClasspath
// }
public class ZetaSqlPipeline {

  public static void main(String[] args) throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(DataflowRunner.class);
    options.as(DataflowPipelineOptions.class).setProject("google.com:clouddfe");
    options.as(DataflowPipelineOptions.class).setTempLocation("gs://robinyq/tmp");
    options.as(DataflowPipelineOptions.class).setRegion("us-central1");
    options
        .as(BeamSqlPipelineOptions.class)
        .setPlannerName("org.apache.beam.sdk.extensions.sql.zetasql.ZetaSQLQueryPlanner");
    GoogleCredentials credentials = GoogleCredentials.fromStream(
        new FileInputStream(
            "/usr/local/google/home/robinyq/Downloads/Dataflow cloud-workflows-9b5863035032.json"))
        .createScoped(
            Arrays.asList(
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/devstorage.full_control",
                "https://www.googleapis.com/auth/userinfo.email",
                "https://www.googleapis.com/auth/datastore",
                "https://www.googleapis.com/auth/pubsub"));
    options
        .as(GcpOptions.class)
        .setGcpCredential(credentials);
    options
        .as(DataflowPipelineOptions.class)
        .setServiceAccount("robinyq@clouddfe.google.com.iam.gserviceaccount.com");

    /*
    DataCatalogClient dataCatalog = DataCatalogTableProvider.createDataCatalogClient(options.as(DataCatalogPipelineOptions.class));
    com.google.cloud.datacatalog.v1.Schema physicalSchema =
        Schema.newBuilder().setPhysicalSchema(
            PhysicalSchema.newBuilder().setAvro(
                AvroSchema.newBuilder().setText("{\n"
                    + "  \"type\": \"record\",\n"
                    + "  \"name\": \"Avro\",\n"
                    + "  \"fields\": [\n"
                    + "    {\n"
                    + "      \"name\": \"StringField\",\n"
                    + "      \"type\": \"string\"\n"
                    + "    },\n"
                    + "    {\n"
                    + "      \"name\": \"IntField\",\n"
                    + "      \"type\": \"int\"\n"
                    + "    }\n"
                    + "  ]\n"
                    + "}").build()
            ).build()
        ).build();
    String resource = String.format("pubsub.topic.`%s`.`%s`", options.as(GcpOptions.class).getProject(), "robinyq-avro-bin");
    Entry entry =
        dataCatalog.lookupEntry(LookupEntryRequest.newBuilder().setSqlResource(resource).build());
    System.out.println(entry.getSchema());
    dataCatalog.updateEntry(
        UpdateEntryRequest.newBuilder()
            .setEntry(entry.toBuilder().setSchema(physicalSchema).build())
            .build());
    */

    Pipeline p = Pipeline.create(options);
    String query = String.format("SELECT Avro.StringField AS s FROM pubsub.topic.`%s`.`%s`", options.as(GcpOptions.class).getProject(), "robinyq-avro-bin");

    try (final DataCatalogTableProvider tableProvider =
        DataCatalogTableProvider.create(options.as(DataCatalogPipelineOptions.class))) {
      SqlTransform sqlTransform =
          SqlTransform.query(query).withDefaultTableProvider("datacatalog", tableProvider);
      PCollection<Row> queryResult = p.apply("Run SQL Query", sqlTransform);
      queryResult.apply(
          "Write to BigQuery",
          BigQueryIO.<Row>write()
              .withSchema(BigQueryUtils.toTableSchema(queryResult.getSchema()))
              .withFormatFunction(BigQueryUtils.toTableRow())
              .withWriteDisposition(WriteDisposition.WRITE_APPEND)
              .to(
                  new TableReference()
                      .setProjectId("google.com:clouddfe")
                      .setDatasetId("robinyq")
                      .setTableId("new-dest")));
    }
    p.run();
  }
}