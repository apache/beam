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

import static org.apache.beam.sdk.extensions.sql.meta.provider.bigtable.BigtableTableTestUtils.FAMILY_TEST;
import static org.apache.beam.sdk.extensions.sql.meta.provider.bigtable.BigtableTableTestUtils.NOW;
import static org.apache.beam.sdk.extensions.sql.meta.provider.bigtable.BigtableTableTestUtils.TEST_FLAT_SCHEMA;
import static org.apache.beam.sdk.extensions.sql.meta.provider.bigtable.BigtableTableTestUtils.createFlatTableString;
import static org.apache.beam.sdk.extensions.sql.meta.provider.bigtable.BigtableTableTestUtils.createFullTableString;
import static org.apache.beam.sdk.extensions.sql.meta.provider.bigtable.BigtableTableTestUtils.expectedFullSchema;
import static org.apache.beam.sdk.io.gcp.bigtable.RowUtils.TIMESTAMP_MICROS;

import com.google.auth.Credentials;
import com.google.cloud.bigtable.emulator.v2.Emulator;
import java.util.UUID;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BigtableTableIT {
  private static BigtableTestOptions options;
  private static BigtableClientWrapper clientWrapper;
  private static final String TABLE_ID = "Beam" + UUID.randomUUID();
  private static Emulator emulator;

  @BeforeClass
  public static void setup() throws Exception {
    PipelineOptionsFactory.register(BigtableTestOptions.class);
    options = TestPipeline.testingPipelineOptions().as(BigtableTestOptions.class);

    if (options.isWithEmulator()) {
      emulator = Emulator.createBundled();
      emulator.start();
    }
    Credentials credentials =
        options.isWithEmulator() ? null : options.as(GcpOptions.class).getGcpCredential();
    Integer emulatorPort = options.isWithEmulator() ? emulator.getPort() : null;

    clientWrapper =
        new BigtableClientWrapper(
            options.getBigtableProject(), options.getInstanceId(), emulatorPort, credentials);

    clientWrapper.createTable(TABLE_ID, FAMILY_TEST);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    clientWrapper.deleteTable(TABLE_ID);
    clientWrapper.closeSession();
    if (emulator != null) {
      emulator.stop();
    }
  }

  @Test
  public void testWriteThenRead() {
    writeData();
    readFlatData();
    readData();
  }

  private void writeData() {
    Pipeline p = Pipeline.create(options);
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new BigtableTableProvider());
    sqlEnv.executeDdl(createFlatTableString(TABLE_ID, location()));

    String query =
        String.format(
            "INSERT INTO `%s`(key, boolColumn, longColumn, stringColumn, doubleColumn) "
                + "VALUES ('key1', FALSE, CAST(1 as bigint), 'string1', 1.0)",
            TABLE_ID);

    BeamSqlRelUtils.toPCollection(p, sqlEnv.parseQuery(query));
    p.run().waitUntilFinish();
  }

  private void readFlatData() {
    Pipeline p = Pipeline.create(options);
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new BigtableTableProvider());
    sqlEnv.executeDdl(createFlatTableString(TABLE_ID, location()));
    String query = "SELECT * FROM `" + TABLE_ID + "`";

    PCollection<Row> flatRows = BeamSqlRelUtils.toPCollection(p, sqlEnv.parseQuery(query));

    PAssert.that(flatRows).containsInAnyOrder(expectedFlatRow(1));
    p.run().waitUntilFinish();
  }

  private void readData() {
    Pipeline p = Pipeline.create(options);
    BeamSqlEnv sqlEnv = BeamSqlEnv.inMemory(new BigtableTableProvider());
    sqlEnv.executeDdl(createFullTableString(TABLE_ID, location()));
    String query =
        String.format(
            "SELECT key, "
                + "  t.familyTest.boolColumn, "
                + "  t.familyTest.longColumn.val AS longValue, "
                + "  t.familyTest.longColumn.timestampMicros, "
                + "  t.familyTest.longColumn.labels, "
                + "  t.familyTest.stringColumn, "
                + "  t.familyTest.doubleColumn "
                + "FROM `%s` t",
            TABLE_ID);

    PCollection<Row> rows =
        BeamSqlRelUtils.toPCollection(p, sqlEnv.parseQuery(query))
            .apply(MapElements.via(new ReplaceTimestamp()))
            .setRowSchema(expectedFullSchema());

    PAssert.that(rows).containsInAnyOrder(expectedFullRow(1));
    p.run().waitUntilFinish();
  }

  private Row expectedFullRow(int i) {
    return Row.withSchema(expectedFullSchema())
        .attachValues(
            "key" + i,
            i % 2 == 0,
            (long) i,
            NOW,
            ImmutableList.of(),
            ImmutableList.of("string" + i),
            (double) i);
  }

  private Row expectedFlatRow(int i) {
    return Row.withSchema(TEST_FLAT_SCHEMA)
        .attachValues("key" + i, i % 2 == 0, (long) i, "string" + i, (double) i);
  }

  private static class ReplaceTimestamp extends SimpleFunction<Row, Row> {
    @Override
    public Row apply(Row input) {
      return Row.fromRow(input).withFieldValue(TIMESTAMP_MICROS, NOW).build();
    }
  }

  private String location() {
    Integer emulatorPort = options.isWithEmulator() ? emulator.getPort() : null;
    return BigtableTableTestUtils.location(
        options.getBigtableProject(), options.getInstanceId(), TABLE_ID, emulatorPort);
  }

  /** Properties needed when using Bigtable with the Beam SDK. */
  public interface BigtableTestOptions extends TestPipelineOptions {
    @Description("Instance ID for Bigtable")
    @Default.String("fakeInstance")
    String getInstanceId();

    void setInstanceId(String value);

    @Description("Project for Bigtable")
    @Default.String("fakeProject")
    String getBigtableProject();

    void setBigtableProject(String value);

    @Description("Whether to use emulator")
    @Default.Boolean(true)
    Boolean isWithEmulator();

    void setWithEmulator(Boolean value);
  }
}
