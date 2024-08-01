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
package org.apache.beam.sdk.io.iceberg.hive;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.beam.sdk.io.iceberg.IcebergUtils;
import org.apache.beam.sdk.io.iceberg.hive.testutils.HiveMetastoreExtension;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

public class IcebergHiveCatalogIT {
  private static final Schema DOUBLY_NESTED_ROW_SCHEMA =
      Schema.builder()
          .addStringField("doubly_nested_str")
          .addInt64Field("doubly_nested_float")
          .build();

  private static final Schema NESTED_ROW_SCHEMA =
      Schema.builder()
          .addStringField("nested_str")
          .addInt32Field("nested_int")
          .addFloatField("nested_float")
          .addRowField("nested_row", DOUBLY_NESTED_ROW_SCHEMA)
          .build();
  private static final Schema ROW_SCHEMA =
      Schema.builder()
          .addStringField("str")
          .addBooleanField("bool")
          .addNullableInt32Field("nullable_int")
          .addNullableInt64Field("nullable_long")
          .addArrayField("arr_long", Schema.FieldType.INT64)
          .addRowField("row", NESTED_ROW_SCHEMA)
          .addNullableRowField("nullable_row", NESTED_ROW_SCHEMA)
          .build();

  private static final SimpleFunction<Long, Row> ROW_FUNC =
      new SimpleFunction<Long, Row>() {
        @Override
        public Row apply(Long num) {
          String strNum = Long.toString(num);
          Row nestedRow =
              Row.withSchema(NESTED_ROW_SCHEMA)
                  .addValue("nested_str_value_" + strNum)
                  .addValue(Integer.valueOf(strNum))
                  .addValue(Float.valueOf(strNum + "." + strNum))
                  .addValue(
                      Row.withSchema(DOUBLY_NESTED_ROW_SCHEMA)
                          .addValue("doubly_nested_str_value_" + strNum)
                          .addValue(num)
                          .build())
                  .build();

          return Row.withSchema(ROW_SCHEMA)
              .addValue("str_value_" + strNum)
              .addValue(num % 2 == 0)
              .addValue(Integer.valueOf(strNum))
              .addValue(num)
              .addValue(LongStream.range(1, num % 10).boxed().collect(Collectors.toList()))
              .addValue(nestedRow)
              .addValue(num % 2 == 0 ? null : nestedRow)
              .build();
        }
      };
  private static HiveMetastoreExtension HIVE_METASTORE_EXTENSION;

  @Rule public TestPipeline writePipeline = TestPipeline.create();

  @Rule public TestPipeline readPipeline = TestPipeline.create();

  private static final String TEST_CATALOG = "test_catalog";
  private static final String TEST_TABLE = "test_table";
  private static HiveCatalog catalog;
  private static final String TEST_DB = "test_db";

  @BeforeClass
  public static void setUp() throws TException {
    String warehousePath = TestPipeline.testingPipelineOptions().getTempLocation();
    HIVE_METASTORE_EXTENSION = new HiveMetastoreExtension(warehousePath);
    catalog =
        (HiveCatalog)
            CatalogUtil.loadCatalog(
                HiveCatalog.class.getName(),
                TEST_CATALOG,
                ImmutableMap.of(
                    CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
                    String.valueOf(TimeUnit.SECONDS.toMillis(10))),
                HIVE_METASTORE_EXTENSION.hiveConf());

    String dbPath = HIVE_METASTORE_EXTENSION.metastore().getDatabasePath(TEST_DB);
    Database db = new Database(TEST_DB, "description", dbPath, Maps.newHashMap());
    HIVE_METASTORE_EXTENSION.metastoreClient().createDatabase(db);
  }

  @AfterClass
  public static void cleanup() throws Exception {
    HIVE_METASTORE_EXTENSION.cleanup();
  }

  @Test
  public void testWriteReadWithHiveCatalog() {
    TableIdentifier tableIdentifier =
        TableIdentifier.parse(String.format("%s.%s", TEST_DB, TEST_TABLE));
    catalog.createTable(tableIdentifier, IcebergUtils.beamSchemaToIcebergSchema(ROW_SCHEMA));

    String metastoreUri =
        HIVE_METASTORE_EXTENSION.hiveConf().getVar(HiveConf.ConfVars.METASTOREURIS);

    Map<String, String> confProperties =
        ImmutableMap.<String, String>builder()
            .put(HiveConf.ConfVars.METASTOREURIS.varname, metastoreUri)
            .build();

    Map<String, Object> transformConfig =
        ImmutableMap.<String, Object>builder()
            .put("table", tableIdentifier.toString())
            .put("config_properties", confProperties)
            .build();

    List<Row> inputRows =
        LongStream.range(1, 1000).mapToObj(ROW_FUNC::apply).collect(Collectors.toList());

    writePipeline
        .apply(Create.of(inputRows))
        .setRowSchema(ROW_SCHEMA)
        .apply(Managed.write(Managed.ICEBERG).withConfig(transformConfig));
    writePipeline.run().waitUntilFinish();

    PCollection<Row> outputRows =
        readPipeline
            .apply(Managed.read(Managed.ICEBERG).withConfig(transformConfig))
            .getSinglePCollection();
    PAssert.that(outputRows).containsInAnyOrder(inputRows);
    readPipeline.run().waitUntilFinish();
  }
}
