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
package org.apache.beam.sdk.extensions.sql;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.beam.sdk.schemas.Schema.FieldType.INT64;
import static org.apache.beam.sdk.schemas.Schema.FieldType.STRING;
import static org.junit.Assert.assertEquals;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpStatusCodes;
import com.google.api.services.bigquery.model.TableRow;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.io.gcp.pubsub.TestPubsub;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.io.iceberg.IcebergUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests stream writing to Iceberg with Beam SQL. */
@RunWith(JUnit4.class)
public class PubsubToIcebergIT implements Serializable {
  private static final Schema SOURCE_SCHEMA =
      Schema.builder().addNullableField("id", INT64).addNullableField("name", STRING).build();

  @Rule public transient TestPipeline pipeline = TestPipeline.create();
  @Rule public transient TestPubsub pubsub = TestPubsub.create();
  private static final BigqueryClient BQ_CLIENT = new BigqueryClient("PubsubToIcebergIT");
  private static final String BQMS_CATALOG =
      "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog";
  static final String DATASET = "sql_pubsub_to_iceberg_it_" + System.nanoTime();
  static String warehouse;
  private static Catalog icebergCatalog;
  protected static final GcpOptions OPTIONS =
      TestPipeline.testingPipelineOptions().as(GcpOptions.class);
  @Rule public TestName testName = new TestName();

  @BeforeClass
  public static void createDataset() throws IOException, InterruptedException {
    warehouse =
        String.format(
            "%s%s/%s",
            TestPipeline.testingPipelineOptions().getTempLocation(),
            PubsubToIcebergIT.class.getSimpleName(),
            UUID.randomUUID());
    BQ_CLIENT.createNewDataset(OPTIONS.getProject(), DATASET);
    createCatalogDdl =
        "CREATE CATALOG my_catalog \n"
            + "TYPE iceberg \n"
            + "PROPERTIES (\n"
            + format("  'catalog-impl' = '%s', \n", BQMS_CATALOG)
            + "  'io-impl' = 'org.apache.iceberg.gcp.gcs.GCSFileIO', \n"
            + format("  'warehouse' = '%s', \n", warehouse)
            + format("  'gcp_project' = '%s', \n", OPTIONS.getProject())
            + "  'gcp_region' = 'us-central1')";
    setCatalogDdl = "USE CATALOG my_catalog";
    icebergCatalog =
        CatalogUtil.loadCatalog(
            BQMS_CATALOG,
            "my_catalog",
            ImmutableMap.<String, String>builder()
                .put("gcp_project", OPTIONS.getProject())
                .put("gcp_location", "us-central1")
                .put("warehouse", warehouse)
                .build(),
            null);
  }

  private String tableIdentifier;
  private static String createCatalogDdl;
  private static String setCatalogDdl;

  @Before
  public void setup() {
    tableIdentifier = DATASET + "." + testName.getMethodName();
  }

  @After
  public void cleanup() {
    icebergCatalog.dropTable(TableIdentifier.parse(tableIdentifier));
  }

  @AfterClass
  public static void deleteDataset() {
    BQ_CLIENT.deleteDataset(OPTIONS.getProject(), DATASET);
  }

  @Test
  public void testSimpleInsertWithPartitionedFields() throws Exception {
    String pubsubTableString =
        "CREATE EXTERNAL TABLE pubsub_topic (\n"
            + "event_timestamp TIMESTAMP, \n"
            + "attributes MAP<VARCHAR, VARCHAR>, \n"
            + "payload ROW< \n"
            + "             id BIGINT, \n"
            + "             name VARCHAR \n"
            + "           > \n"
            + ") \n"
            + "TYPE 'pubsub' \n"
            + "LOCATION '"
            + pubsub.topicPath()
            + "' \n"
            + "TBLPROPERTIES '{ \"timestampAttributeKey\" : \"ts\" }'";
    String icebergTableString =
        "CREATE EXTERNAL TABLE iceberg_table( \n"
            + "   id BIGINT, \n"
            + "   name VARCHAR \n "
            + ") \n"
            + "TYPE 'iceberg' \n"
            + "PARTITIONED BY('id', 'truncate(name, 3)') \n"
            + "LOCATION '"
            + tableIdentifier
            + "' \n"
            + "TBLPROPERTIES '{ \"triggering_frequency_seconds\" : 10 }'";
    String insertStatement =
        "INSERT INTO iceberg_table \n"
            + "SELECT \n"
            + "  pubsub_topic.payload.id, \n"
            + "  pubsub_topic.payload.name \n"
            + "FROM pubsub_topic";
    pipeline.apply(
        SqlTransform.query(insertStatement)
            .withDdlString(createCatalogDdl)
            .withDdlString(setCatalogDdl)
            .withDdlString(pubsubTableString)
            .withDdlString(icebergTableString));
    pipeline.run();

    // Block until a subscription for this topic exists
    pubsub.assertSubscriptionEventuallyCreated(
        pipeline.getOptions().as(GcpOptions.class).getProject(), Duration.standardMinutes(5));

    List<PubsubMessage> messages =
        ImmutableList.of(
            message(ts(1), 3, "foo"), message(ts(2), 5, "bar"), message(ts(3), 7, "baz"));
    pubsub.publish(messages);

    validateRowsWritten();

    // verify the table was created with the right partition spec
    PartitionSpec expectedSpec =
        PartitionSpec.builderFor(IcebergUtils.beamSchemaToIcebergSchema(SOURCE_SCHEMA))
            .identity("id")
            .truncate("name", 3)
            .build();
    Table table = icebergCatalog.loadTable(TableIdentifier.parse(tableIdentifier));
    assertEquals(expectedSpec, table.spec());
  }

  @Test
  public void testSimpleInsertFlat() throws Exception {
    String pubsubTableString =
        "CREATE EXTERNAL TABLE pubsub_topic (\n"
            + "event_timestamp TIMESTAMP, \n"
            + "id BIGINT, \n"
            + "name VARCHAR \n"
            + ") \n"
            + "TYPE 'pubsub' \n"
            + "LOCATION '"
            + pubsub.topicPath()
            + "' \n"
            + "TBLPROPERTIES '{ \"timestampAttributeKey\" : \"ts\" }'";
    String bqTableString =
        "CREATE EXTERNAL TABLE iceberg_table( \n"
            + "   id BIGINT, \n"
            + "   name VARCHAR \n "
            + ") \n"
            + "TYPE 'iceberg' \n"
            + "LOCATION '"
            + tableIdentifier
            + "' \n"
            + "TBLPROPERTIES '{ \"triggering_frequency_seconds\" : 10 }'";
    String insertStatement =
        "INSERT INTO iceberg_table \n"
            + "SELECT \n"
            + "  id, \n"
            + "  name \n"
            + "FROM pubsub_topic";

    pipeline.apply(
        SqlTransform.query(insertStatement)
            .withDdlString(createCatalogDdl)
            .withDdlString(setCatalogDdl)
            .withDdlString(pubsubTableString)
            .withDdlString(bqTableString));
    pipeline.run();

    // Block until a subscription for this topic exists
    pubsub.assertSubscriptionEventuallyCreated(
        pipeline.getOptions().as(GcpOptions.class).getProject(), Duration.standardMinutes(5));

    List<PubsubMessage> messages =
        ImmutableList.of(
            message(ts(1), 3, "foo"), message(ts(2), 5, "bar"), message(ts(3), 7, "baz"));
    pubsub.publish(messages);

    validateRowsWritten();
  }

  private void validateRowsWritten() throws IOException, InterruptedException {
    String query = String.format("SELECT * FROM `%s.%s`", OPTIONS.getProject(), tableIdentifier);
    List<Row> expectedRows =
        ImmutableList.of(
            row(SOURCE_SCHEMA, 3L, "foo"),
            row(SOURCE_SCHEMA, 5L, "bar"),
            row(SOURCE_SCHEMA, 7L, "baz"));

    BackOff backOff =
        FluentBackoff.DEFAULT
            .withInitialBackoff(Duration.standardSeconds(10))
            .withMaxBackoff(Duration.standardSeconds(20))
            .withMaxCumulativeBackoff(Duration.standardMinutes(5))
            .backoff();
    Sleeper sleeper = Sleeper.DEFAULT;
    do {
      List<TableRow> returnedRows = new ArrayList<>();
      try {
        returnedRows = BQ_CLIENT.queryUnflattened(query, OPTIONS.getProject(), true, true);
      } catch (GoogleJsonResponseException e) {
        if (e.getStatusCode() != HttpStatusCodes.STATUS_CODE_NOT_FOUND) {
          throw new RuntimeException(e);
        }
      }
      List<Row> beamRows =
          returnedRows.stream()
              .map(r -> BigQueryUtils.toBeamRow(SOURCE_SCHEMA, r))
              .collect(Collectors.toList());
      if (beamRows.containsAll(expectedRows)) {
        return;
      }
    } while (BackOffUtils.next(sleeper, backOff));

    throw new RuntimeException("Polled for 5 minutes and could not find all rows in table.");
  }

  private Row row(Schema schema, Object... values) {
    return Row.withSchema(schema).addValues(values).build();
  }

  private PubsubMessage message(Instant timestamp, int id, String name) {
    return message(timestamp, jsonString(id, name));
  }

  private PubsubMessage message(Instant timestamp, String jsonPayload) {
    return new PubsubMessage(
        jsonPayload.getBytes(UTF_8), ImmutableMap.of("ts", String.valueOf(timestamp.getMillis())));
  }

  private String jsonString(int id, String name) {
    return "{ \"id\" : " + id + ", \"name\" : \"" + name + "\" }";
  }

  private Instant ts(long millis) {
    return new Instant(millis);
  }
}
