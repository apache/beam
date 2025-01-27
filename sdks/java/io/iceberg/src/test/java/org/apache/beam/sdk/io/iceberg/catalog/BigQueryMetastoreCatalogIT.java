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
package org.apache.beam.sdk.io.iceberg.catalog;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import com.google.api.services.bigquery.model.TableRow;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.io.gcp.testing.BigqueryClient;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.util.RowFilter;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class BigQueryMetastoreCatalogIT extends IcebergCatalogBaseIT {
  private static final BigqueryClient BQ_CLIENT = new BigqueryClient("BigQueryMetastoreCatalogIT");
  static final String BQMS_CATALOG = "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog";
  static final String DATASET = "managed_iceberg_bqms_tests_" + System.nanoTime();;
  static final long SALT = System.nanoTime();

  @BeforeClass
  public static void createDataset() throws IOException, InterruptedException {
    BQ_CLIENT.createNewDataset(OPTIONS.getProject(), DATASET);
  }

  @AfterClass
  public static void deleteDataset() {
    BQ_CLIENT.deleteDataset(OPTIONS.getProject(), DATASET);
  }

  @Override
  public String tableId() {
    return DATASET + "." + testName.getMethodName() + "_" + SALT;
  }

  @Override
  public Catalog createCatalog() {
    return CatalogUtil.loadCatalog(
        BQMS_CATALOG,
        "bqms_" + catalogName,
        ImmutableMap.<String, String>builder()
            .put("gcp_project", OPTIONS.getProject())
            .put("gcp_location", "us-central1")
            .put("warehouse", warehouse)
            .build(),
        new Configuration());
  }

  @Override
  public void catalogCleanup() {
    for (TableIdentifier tableIdentifier : catalog.listTables(Namespace.of(DATASET))) {
      // only delete tables that were created in this test run
      if (tableIdentifier.name().contains(String.valueOf(SALT))) {
        catalog.dropTable(tableIdentifier);
      }
    }
  }

  @Override
  public Map<String, Object> managedIcebergConfig(String tableId) {
    return ImmutableMap.<String, Object>builder()
        .put("table", tableId)
        .put(
            "catalog_properties",
            ImmutableMap.<String, String>builder()
                .put("gcp_project", OPTIONS.getProject())
                .put("gcp_location", "us-central1")
                .put("warehouse", warehouse)
                .put("catalog-impl", BQMS_CATALOG)
                .put("io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO")
                .build())
        .build();
  }

  @Test
  public void testWriteToPartitionedAndValidateWithBQQuery()
      throws IOException, InterruptedException {
    // For an example row where bool=true, modulo_5=3, str=value_303,
    // this partition spec will create a partition like: /bool=true/modulo_5=3/str_trunc=value_3/
    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(ICEBERG_SCHEMA)
            .identity("bool")
            .hour("datetime")
            .truncate("str", "value_x".length())
            .build();
    catalog.createTable(TableIdentifier.parse(tableId()), ICEBERG_SCHEMA, partitionSpec);

    // Write with Beam
    Map<String, Object> config = managedIcebergConfig(tableId());
    PCollection<Row> input = pipeline.apply(Create.of(inputRows)).setRowSchema(BEAM_SCHEMA);
    input.apply(Managed.write(Managed.ICEBERG).withConfig(config));
    pipeline.run().waitUntilFinish();

    // Fetch records using a BigQuery query and validate
    BigqueryClient bqClient = new BigqueryClient(getClass().getSimpleName());
    String query = String.format("SELECT * FROM `%s.%s`", OPTIONS.getProject(), tableId());
    List<TableRow> rows = bqClient.queryUnflattened(query, OPTIONS.getProject(), true, true);
    List<Row> beamRows =
        rows.stream()
            .map(tr -> BigQueryUtils.toBeamRow(BEAM_SCHEMA, tr))
            .collect(Collectors.toList());

    assertThat(beamRows, containsInAnyOrder(inputRows.toArray()));

    String queryByPartition =
        String.format("SELECT bool, datetime FROM `%s.%s`", OPTIONS.getProject(), tableId());
    rows = bqClient.queryUnflattened(queryByPartition, OPTIONS.getProject(), true, true);
    RowFilter rowFilter = new RowFilter(BEAM_SCHEMA).keep(Arrays.asList("bool", "datetime"));
    beamRows =
        rows.stream()
            .map(tr -> BigQueryUtils.toBeamRow(rowFilter.outputSchema(), tr))
            .collect(Collectors.toList());
    assertThat(beamRows, containsInAnyOrder(inputRows.stream().map(rowFilter::filter).toArray()));
  }
}
