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

import java.util.Collections;
import java.util.UUID;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformTranslationTest;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

@RunWith(Enclosed.class)
public class IcebergSchemaTransformTranslationTest {
  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  public static class ReadTranslationTest extends SchemaTransformTranslationTest {
    @Rule
    public transient TestDataWarehouse warehouse =
        new TestDataWarehouse(TEMPORARY_FOLDER, "default");

    String tableIdentifier;

    @Before
    public void setup() {
      tableIdentifier = "default.table_" + Long.toString(UUID.randomUUID().hashCode(), 16);
      warehouse.createTable(TableIdentifier.parse(tableIdentifier), TestFixtures.SCHEMA);
    }

    static final IcebergReadSchemaTransformProvider READ_PROVIDER =
        new IcebergReadSchemaTransformProvider();

    @Override
    protected SchemaTransformProvider provider() {
      return READ_PROVIDER;
    }

    @Override
    protected Row configurationRow() {
      Row catalogConfigRow =
          Row.withSchema(IcebergSchemaTransformCatalogConfig.SCHEMA)
              .withFieldValue("catalog_name", "test_read")
              .withFieldValue("catalog_type", CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP)
              .withFieldValue("warehouse_location", warehouse.location)
              .build();
      return Row.withSchema(READ_PROVIDER.configurationSchema())
          .withFieldValue("table", tableIdentifier)
          .withFieldValue("catalog_config", catalogConfigRow)
          .build();
    }
  }

  public static class WriteTranslationTest extends SchemaTransformTranslationTest {
    static final IcebergWriteSchemaTransformProvider WRITE_PROVIDER =
        new IcebergWriteSchemaTransformProvider();

    @Override
    protected SchemaTransformProvider provider() {
      return WRITE_PROVIDER;
    }

    @Override
    protected Row configurationRow() {
      Row catalogConfigRow =
          Row.withSchema(IcebergSchemaTransformCatalogConfig.SCHEMA)
              .withFieldValue("catalog_name", "test_write")
              .withFieldValue("catalog_type", CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP)
              .withFieldValue("warehouse_location", "warehouse.location")
              .build();
      return Row.withSchema(WRITE_PROVIDER.configurationSchema())
          .withFieldValue("table", "test_identifier")
          .withFieldValue("catalog_config", catalogConfigRow)
          .build();
    }

    @Override
    protected PCollectionRowTuple input(Pipeline p) {
      Schema inputSchema = Schema.builder().addStringField("str").build();
      PCollection<Row> inputRows =
          p.apply(
                  Create.of(
                      Collections.singletonList(Row.withSchema(inputSchema).addValue("a").build())))
              .setRowSchema(inputSchema);
      return PCollectionRowTuple.of("input", inputRows);
    }
  }
}
