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

import static org.apache.beam.sdk.managed.Managed.ICEBERG;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertFalse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.iceberg.BigLakeTestCatalog;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.rest.RESTCatalog;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for {@link org.apache.iceberg.rest.RESTCatalog} using a multiple-bucket BigLake Metastore
 * catalog (see {@link BigLakeTestCatalog}).
 */
public class RESTCatalogBLMSIT extends IcebergCatalogBaseIT {
  private static Map<String, String> catalogProps;

  @BeforeClass
  public static void setup() {
    // The catalog decides where tables go (its default location); the base class only uses
    // `warehouse` to sweep leftover files, so point it at that location.
    warehouse = BigLakeTestCatalog.defaultLocation();
    catalogProps = BigLakeTestCatalog.catalogProperties();
  }

  @After
  public void after() {
    // making sure the cleanup path is directed at the correct warehouse
    warehouse = BigLakeTestCatalog.defaultLocation();
  }

  @Override
  public String type() {
    return "biglake";
  }

  @Override
  public String bigQueryTableSpec(String tableId) {
    // BigQuery surfaces Lakehouse runtime catalog (BigLake metastore REST) tables via 4-part
    // project.catalog.namespace.table identifiers. Requires the caller to hold biglake.* read
    // permissions (e.g. roles/biglake.viewer) in addition to the usual BigQuery roles.
    TableIdentifier identifier = TableIdentifier.parse(tableId);
    return BigLakeTestCatalog.bigQueryTableSpec(
        identifier.namespace().toString(), identifier.name());
  }

  @Override
  public Catalog createCatalog() {
    RESTCatalog restCatalog = new RESTCatalog();
    restCatalog.initialize(catalogName, catalogProps);
    return restCatalog;
  }

  @Override
  public Map<String, Object> managedIcebergConfig(String tableId) {
    return ImmutableMap.<String, Object>builder()
        .put("table", tableId)
        .put("catalog_properties", catalogProps)
        .build();
  }

  /**
   * A multiple-bucket catalog may place resources under any of its restricted locations, not just
   * its default one. BigLake pins a table under its namespace's location, so the namespace is
   * created in a second bucket; Beam only receives the table through the catalog and must write
   * wherever it was placed.
   */
  @Test
  public void testWriteReadTableInAdditionalLocation() throws IOException {
    String altNamespace = namespace() + "_alt";
    String altTableId = altNamespace + ".test_table";
    String namespaceLocation = BigLakeTestCatalog.additionalLocation() + "/" + altNamespace;
    assertFalse(
        "Test needs two distinct buckets",
        BigLakeTestCatalog.bucketOf(namespaceLocation)
            .equals(BigLakeTestCatalog.bucketOf(BigLakeTestCatalog.defaultLocation())));
    namespacesToCleanup.add(altNamespace);
    ((SupportsNamespaces) catalog)
        .createNamespace(
            Namespace.of(altNamespace), ImmutableMap.of("location", namespaceLocation));
    Table table = catalog.createTable(TableIdentifier.parse(altTableId), ICEBERG_SCHEMA);
    assertThat(table.location(), startsWith(namespaceLocation));

    pipeline
        .apply(Create.of(inputRows))
        .setRowSchema(BEAM_SCHEMA)
        .apply(Managed.write(ICEBERG).withConfig(managedIcebergConfig(altTableId)));
    pipeline.run().waitUntilFinish();

    table.refresh();
    List<Record> returnedRecords = readRecords(table);
    assertThat(
        returnedRecords, containsInAnyOrder(inputRows.stream().map(RECORD_FUNC::apply).toArray()));

    // Both the data files Beam wrote and the metadata the catalog committed live in the
    // additional location, not in the catalog's default bucket.
    List<String> dataFileLocations = new ArrayList<>();
    for (Snapshot snapshot : table.snapshots()) {
      for (DataFile dataFile : snapshot.addedDataFiles(table.io())) {
        dataFileLocations.add(dataFile.location());
      }
    }
    assertFalse("No data files were written", dataFileLocations.isEmpty());
    for (String location : dataFileLocations) {
      assertThat(location, startsWith(BigLakeTestCatalog.additionalLocation()));
    }
    String metadataLocation = ((BaseTable) table).operations().current().metadataFileLocation();
    assertThat(metadataLocation, startsWith(BigLakeTestCatalog.additionalLocation()));
  }
}
