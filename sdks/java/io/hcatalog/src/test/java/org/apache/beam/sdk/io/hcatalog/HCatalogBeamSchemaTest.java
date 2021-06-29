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
package org.apache.beam.sdk.io.hcatalog;

import static org.apache.beam.sdk.io.hcatalog.test.HCatalogIOTestUtils.TEST_DATABASE;
import static org.apache.beam.sdk.io.hcatalog.test.HCatalogIOTestUtils.TEST_TABLE;
import static org.apache.beam.sdk.io.hcatalog.test.HCatalogIOTestUtils.insertTestData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.Serializable;
import org.apache.beam.sdk.io.hcatalog.test.EmbeddedMetastoreService;
import org.apache.beam.sdk.schemas.Schema;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link HCatalogBeamSchema}. */
@RunWith(JUnit4.class)
public class HCatalogBeamSchemaTest implements Serializable {

  private static final String TEST_TABLE_PARTITIONED = TEST_TABLE + "_partitioned";
  @ClassRule public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  private static EmbeddedMetastoreService service;

  @BeforeClass
  public static void setupEmbeddedMetastoreService() throws IOException {
    service = new EmbeddedMetastoreService(TMP_FOLDER.getRoot().getAbsolutePath());
  }

  @AfterClass
  public static void shutdownEmbeddedMetastoreService() throws Exception {
    if (service != null) {
      service.executeQuery("drop table " + TEST_TABLE);
      service.close();
    }
  }

  @Before
  public void setUp() throws Exception {
    prepareTestData();
  }

  @Test
  public void testHasDB() {
    HCatalogBeamSchema hcatSchema = HCatalogBeamSchema.create(service.getHiveConfAsMap());
    assertTrue(hcatSchema.hasDatabase(TEST_DATABASE));
  }

  @Test
  public void testDoesntHaveDB() {
    HCatalogBeamSchema hcatSchema = HCatalogBeamSchema.create(service.getHiveConfAsMap());
    assertFalse(hcatSchema.hasDatabase("non-existent-db"));
  }

  @Test
  public void testGetTableSchema() {
    HCatalogBeamSchema hcatSchema = HCatalogBeamSchema.create(service.getHiveConfAsMap());
    Schema schema = hcatSchema.getTableSchema(TEST_DATABASE, TEST_TABLE).get();

    Schema expectedSchema =
        Schema.builder()
            .addNullableField("mycol1", Schema.FieldType.STRING)
            .addNullableField("mycol2", Schema.FieldType.INT32)
            .build();

    assertEquals(expectedSchema, schema);
  }

  @Test
  public void testGetTableSchemaForPartitionedTable() {
    HCatalogBeamSchema hcatSchema = HCatalogBeamSchema.create(service.getHiveConfAsMap());
    Schema schema = hcatSchema.getTableSchema(TEST_DATABASE, TEST_TABLE_PARTITIONED).get();

    Schema expectedSchema =
        Schema.builder()
            .addNullableField("mycol1", Schema.FieldType.STRING)
            .addNullableField("mycol2", Schema.FieldType.INT32)
            .addNullableField("part1", Schema.FieldType.STRING)
            .addNullableField("part2", Schema.FieldType.INT32)
            .build();

    assertEquals(expectedSchema, schema);
  }

  @Test
  public void testDoesntHaveTable() {
    HCatalogBeamSchema hcatSchema = HCatalogBeamSchema.create(service.getHiveConfAsMap());
    assertFalse(hcatSchema.getTableSchema(TEST_DATABASE, "non-existent-table").isPresent());
  }

  private void prepareTestData() throws Exception {
    reCreateTestTable();
    insertTestData(service.getHiveConfAsMap());
  }

  private void reCreateTestTable() {
    service.executeQuery("drop table " + TEST_TABLE);
    service.executeQuery("drop table " + TEST_TABLE_PARTITIONED);
    service.executeQuery("create table " + TEST_TABLE + "(mycol1 string, mycol2 int)");
    service.executeQuery(
        "create table "
            + TEST_TABLE_PARTITIONED
            + "(mycol1 string, mycol2 int) "
            + "partitioned by (part1 string, part2 int)");
  }
}
