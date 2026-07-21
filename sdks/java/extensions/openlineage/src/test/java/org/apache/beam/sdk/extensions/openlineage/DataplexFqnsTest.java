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
package org.apache.beam.sdk.extensions.openlineage;

import static org.junit.Assert.assertEquals;

import io.openlineage.client.utils.DatasetIdentifier;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link DataplexFqns}: every mapping must follow the OpenLineage dataset naming
 * conventions (https://openlineage.io/docs/spec/naming/).
 */
@RunWith(JUnit4.class)
public class DataplexFqnsTest {

  private static void assertMapsTo(String fqn, String expectedNamespace, String expectedName) {
    DatasetIdentifier identifier = DataplexFqns.toDatasetIdentifier(fqn);
    assertEquals(expectedNamespace, identifier.getNamespace());
    assertEquals(expectedName, identifier.getName());
  }

  @Test
  public void testPubsubTopic() {
    assertMapsTo("pubsub:topic:acme-prod.orders-events", "pubsub", "topic:acme-prod:orders-events");
  }

  @Test
  public void testPubsubSubscription() {
    assertMapsTo(
        "pubsub:subscription:acme-prod.orders-sub", "pubsub", "subscription:acme-prod:orders-sub");
  }

  @Test
  public void testKafkaWithEscapedBootstrapServers() {
    assertMapsTo(
        "kafka:`broker-1:9092,broker-2:9092`.payments", "kafka://broker-1:9092", "payments");
  }

  @Test
  public void testBigQuery() {
    assertMapsTo("bigquery:acme-prod.sales.orders", "bigquery", "acme-prod.sales.orders");
  }

  @Test
  public void testGcsWithEscapedObjectKey() {
    assertMapsTo(
        "gcs:acme-lakehouse.`warehouse/sales.db/orders`",
        "gs://acme-lakehouse",
        "warehouse/sales.db/orders");
  }

  @Test
  public void testS3() {
    assertMapsTo("s3:my-bucket.`data/file.parquet`", "s3://my-bucket", "data/file.parquet");
  }

  @Test
  public void testAzureBlobStorage() {
    assertMapsTo(
        "abs:myaccount.mycontainer.path/to/blob",
        "wasbs://mycontainer@myaccount.dfs.core.windows.net",
        "path/to/blob");
  }

  @Test
  public void testHdfs() {
    assertMapsTo("hdfs:`namenode:8020`./user/data", "hdfs://namenode:8020", "user/data");
  }

  @Test
  public void testSpanner() {
    assertMapsTo(
        "spanner:acme-prod.regional-us.instance1.salesdb.orders",
        "spanner://acme-prod:instance1",
        "salesdb.orders");
  }

  @Test
  public void testJdbcPostgresAlias() {
    assertMapsTo(
        "postgresql:`db-host:5432`.mydb.public.users",
        "postgres://db-host:5432",
        "mydb.public.users");
  }

  @Test
  public void testUnknownSystemFallsBackToGenericMapping() {
    // Systems without an OpenLineage naming-spec entry pass through conservatively.
    assertMapsTo("bigtable:acme-prod.instance1.events", "bigtable", "acme-prod.instance1.events");
  }

  @Test
  public void testTruncatedFqnMarkerIsStripped() {
    assertMapsTo("bigquery:acme-prod.sales.orders*", "bigquery", "acme-prod.sales.orders");
  }

  @Test
  public void testDoubledBacktickEscapesLiteralBacktick() {
    assertMapsTo("kafka:`h:9092`.`odd``topic`", "kafka://h:9092", "odd`topic");
  }
}
