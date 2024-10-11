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
package org.apache.beam.sdk.io.iceberg;

import static org.apache.iceberg.hadoop.HadoopOutputFile.fromPath;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.orc.ORC;
import org.apache.iceberg.parquet.Parquet;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Assert;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestDataWarehouse extends ExternalResource {
  private static final Logger LOG = LoggerFactory.getLogger(TestDataWarehouse.class);

  protected final TemporaryFolder temporaryFolder;
  protected final String database;

  protected final Configuration hadoopConf;

  protected String location;
  protected Catalog catalog;
  protected boolean someTableHasBeenCreated = false;

  public TestDataWarehouse(TemporaryFolder temporaryFolder, String database) {
    this.temporaryFolder = temporaryFolder;
    this.database = database;
    this.hadoopConf = new Configuration();
  }

  @Override
  protected void before() throws Throwable {
    File warehouseFile = temporaryFolder.newFolder();
    Assert.assertTrue(warehouseFile.delete());
    location = "file:" + warehouseFile.toString();
    catalog =
        CatalogUtil.loadCatalog(
            CatalogUtil.ICEBERG_CATALOG_HADOOP,
            "hadoop",
            ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, location),
            hadoopConf);
  }

  @Override
  protected void after() {
    if (!someTableHasBeenCreated) {
      return;
    }

    List<TableIdentifier> tables = catalog.listTables(Namespace.of(database));

    LOG.info("Cleaning up {} tables in test warehouse", tables.size());
    for (TableIdentifier t : tables) {
      try {
        LOG.info("Removing table {}", t);
        catalog.dropTable(t);
      } catch (Exception e) {
        LOG.error("Unable to remove table", e);
      }
    }
    try {
      ((HadoopCatalog) catalog).close();
    } catch (Exception e) {
      LOG.error("Unable to close catalog", e);
    }
  }

  public DataFile writeRecords(String filename, Schema schema, List<Record> records)
      throws IOException {
    Path path = new Path(location, filename);
    FileFormat format = FileFormat.fromFileName(filename);

    FileAppender<Record> appender;
    switch (format) {
      case PARQUET:
        appender =
            Parquet.write(fromPath(path, hadoopConf))
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .schema(schema)
                .overwrite()
                .build();
        break;
      case ORC:
        appender =
            ORC.write(fromPath(path, hadoopConf))
                .createWriterFunc(GenericOrcWriter::buildWriter)
                .schema(schema)
                .overwrite()
                .build();
        break;
      default:
        throw new IOException("Unable to create appender for " + format);
    }
    appender.addAll(records);
    appender.close();
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withInputFile(HadoopInputFile.fromPath(path, hadoopConf))
        .withMetrics(appender.metrics())
        .build();
  }

  public Table createTable(TableIdentifier tableId, Schema schema) {
    return createTable(tableId, schema, null);
  }

  public Table createTable(
      TableIdentifier tableId, Schema schema, @Nullable PartitionSpec partitionSpec) {
    return createTable(tableId, schema, partitionSpec, null);
  }

  public Table createTable(
      TableIdentifier tableId,
      Schema schema,
      @Nullable PartitionSpec partitionSpec,
      @Nullable Map<String, String> properties) {
    someTableHasBeenCreated = true;
    return catalog.createTable(tableId, schema, partitionSpec, properties);
  }

  public Table loadTable(TableIdentifier tableId) {
    return catalog.loadTable(tableId);
  }
}
