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
package org.apache.beam.sdk.extensions.sql.meta.provider.bigtable;

import static java.util.stream.Collectors.toList;
import static org.apache.beam.sdk.extensions.sql.meta.provider.bigtable.BigtableTableTestUtils.KEY1;
import static org.apache.beam.sdk.extensions.sql.meta.provider.bigtable.BigtableTableTestUtils.KEY2;
import static org.apache.beam.sdk.extensions.sql.meta.provider.bigtable.BigtableTableTestUtils.STRING_COLUMN;
import static org.apache.beam.sdk.extensions.sql.meta.provider.bigtable.BigtableTableTestUtils.TEST_SCHEMA;
import static org.apache.beam.sdk.extensions.sql.meta.provider.bigtable.BigtableTableTestUtils.createFullTableString;
import static org.apache.beam.sdk.extensions.sql.meta.provider.bigtable.BigtableTableTestUtils.createReadTable;
import static org.apache.beam.sdk.extensions.sql.meta.provider.bigtable.BigtableTableTestUtils.expectedFullRow;
import static org.apache.beam.sdk.extensions.sql.meta.provider.bigtable.BigtableTableTestUtils.expectedFullSchema;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.cloud.bigtable.emulator.v2.BigtableEmulatorRule;
import java.io.IOException;
import org.apache.beam.sdk.extensions.sql.BeamSqlCli;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.store.InMemoryMetaStore;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

public class BigtableTableWithRowsTest {

  @ClassRule
  public static final BigtableEmulatorRule BIGTABLE_EMULATOR = BigtableEmulatorRule.create();

  @Rule public TestPipeline readPipeline = TestPipeline.create();

  private static BigtableClientWrapper emulatorWrapper;

  private static final String PROJECT = "fakeProject";
  private static final String INSTANCE = "fakeInstance";
  private static final String TABLE = "beamTable";

  @BeforeClass
  public static void setUp() throws Exception {
    emulatorWrapper =
        new BigtableClientWrapper("fakeProject", "fakeInstance", BIGTABLE_EMULATOR.getPort(), null);
  }

  @AfterClass
  public static void tearDown() throws IOException {
    emulatorWrapper.closeSession();
  }

  @Test
  public void testCreatesSchemaCorrectly() {
    InMemoryMetaStore metaStore = new InMemoryMetaStore();
    metaStore.registerProvider(new BigtableTableProvider());

    BeamSqlCli cli = new BeamSqlCli().metaStore(metaStore);
    cli.execute(createFullTableString(TABLE, location()));

    Table table = metaStore.getTables().get("beamTable");
    assertNotNull(table);
    assertEquals(TEST_SCHEMA, table.getSchema());
  }

  @Test
  public void testSimpleSelect() {
    createReadTable(TABLE, emulatorWrapper);
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new BigtableTableProvider());
    sqlEnv.executeDdl(createFullTableString(TABLE, location()));
    String query =
        "SELECT key, \n"
            + "  bt.familyTest.boolColumn, \n"
            + "  bt.familyTest.longColumn.val AS longValue, \n"
            + "  bt.familyTest.longColumn.timestampMicros, \n"
            + "  bt.familyTest.longColumn.labels, \n"
            + "  bt.familyTest.stringColumn, \n"
            + "  bt.familyTest.doubleColumn \n"
            + "FROM beamTable bt";
    sqlEnv.parseQuery(query);
    PCollection<Row> queryOutput =
        BeamSqlRelUtils.toPCollection(readPipeline, sqlEnv.parseQuery(query));

    assertThat(queryOutput.getSchema(), equalTo(expectedFullSchema()));

    PCollection<Row> sorted =
        queryOutput
            .apply(MapElements.via(new SortByTimestamp()))
            .setRowSchema(expectedFullSchema());

    PAssert.that(sorted).containsInAnyOrder(expectedFullRow(KEY1), expectedFullRow(KEY2));
    readPipeline.run().waitUntilFinish();
  }

  private String location() {
    return BigtableTableTestUtils.location(PROJECT, INSTANCE, TABLE, BIGTABLE_EMULATOR.getPort());
  }

  private static class SortByTimestamp extends SimpleFunction<Row, Row> {
    @Override
    public Row apply(Row input) {
      return Row.fromRow(input)
          .withFieldValue(
              STRING_COLUMN,
              ImmutableList.copyOf(
                  input.getArray(STRING_COLUMN).stream().sorted().collect(toList())))
          .build();
    }
  }
}
