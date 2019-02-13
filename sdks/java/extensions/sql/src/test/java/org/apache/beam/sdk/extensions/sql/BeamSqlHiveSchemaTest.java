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

import static org.apache.beam.sdk.io.hcatalog.test.HCatalogIOTestUtils.TEST_DATABASE;
import static org.apache.beam.sdk.io.hcatalog.test.HCatalogIOTestUtils.TEST_RECORDS_COUNT;
import static org.apache.beam.sdk.io.hcatalog.test.HCatalogIOTestUtils.TEST_TABLE;
import static org.apache.beam.sdk.io.hcatalog.test.HCatalogIOTestUtils.getExpectedRecordsAsKV;
import static org.apache.beam.sdk.io.hcatalog.test.HCatalogIOTestUtils.insertTestData;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import org.apache.beam.sdk.extensions.sql.impl.schema.BeamPCollectionTable;
import org.apache.beam.sdk.extensions.sql.meta.provider.ReadOnlyTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.hcatalog.HCatalogTableProvider;
import org.apache.beam.sdk.io.hcatalog.test.EmbeddedMetastoreService;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Test for HCatalogTableProvider. */
public class BeamSqlHiveSchemaTest implements Serializable {

  private static final Schema ROW_SCHEMA =
      Schema.builder().addInt32Field("f_int").addStringField("f_string").build();

  @ClassRule public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @Rule public final transient TestPipeline defaultPipeline = TestPipeline.create();

  @Rule public final transient TestPipeline readAfterWritePipeline = TestPipeline.create();

  @Rule public final TestPipeline pipeline = TestPipeline.create();

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

  @Test
  public void testSelectFromHCatalog() throws Exception {
    initializeHCatalog();

    PCollection<KV<String, Integer>> output =
        readAfterWritePipeline
            .apply(
                SqlTransform.query(
                        String.format(
                            "SELECT f_str, f_int FROM `hive`.`%s`.`%s`", TEST_DATABASE, TEST_TABLE))
                    .withTableProvider("hive", hiveTableProvider()))
            .apply(ParDo.of(new ToKV()));
    PAssert.that(output).containsInAnyOrder(getExpectedRecordsAsKV(TEST_RECORDS_COUNT));
    readAfterWritePipeline.run();
  }

  @Test
  public void testSelectFromImplicitDefaultDb() throws Exception {
    initializeHCatalog();

    PCollection<KV<String, Integer>> output =
        readAfterWritePipeline
            .apply(
                SqlTransform.query(
                        String.format("SELECT f_str, f_int FROM `hive`.`%s`", TEST_TABLE))
                    .withTableProvider("hive", hiveTableProvider()))
            .apply(ParDo.of(new ToKV()));
    PAssert.that(output).containsInAnyOrder(getExpectedRecordsAsKV(TEST_RECORDS_COUNT));
    readAfterWritePipeline.run();
  }

  @Test
  public void testSelectFromImplicitDefaultSchema() throws Exception {
    initializeHCatalog();

    PCollection<KV<String, Integer>> output =
        readAfterWritePipeline
            .apply(
                SqlTransform.query(
                        String.format(
                            "SELECT f_str, f_int FROM `%s`.`%s`", TEST_DATABASE, TEST_TABLE))
                    .withDefaultTableProvider("hive", hiveTableProvider()))
            .apply(ParDo.of(new ToKV()));
    PAssert.that(output).containsInAnyOrder(getExpectedRecordsAsKV(TEST_RECORDS_COUNT));
    readAfterWritePipeline.run();
  }

  @Test
  public void testSelectFromImplicitDefaultSchemaAndDB() throws Exception {
    initializeHCatalog();

    PCollection<KV<String, Integer>> output =
        readAfterWritePipeline
            .apply(
                SqlTransform.query(String.format("SELECT f_str, f_int FROM `%s`", TEST_TABLE))
                    .withDefaultTableProvider("hive", hiveTableProvider()))
            .apply(ParDo.of(new ToKV()));
    PAssert.that(output).containsInAnyOrder(getExpectedRecordsAsKV(TEST_RECORDS_COUNT));
    readAfterWritePipeline.run();
  }

  @Test
  public void testJoinPCollectionWithHCatalog() throws Exception {
    initializeHCatalog();

    PCollection<Row> inputMain =
        pipeline.apply("pcollection", create(row(1, "pcollection_1"), row(2, "pcollection_2")));

    PCollection<Row> result =
        inputMain.apply(
            SqlTransform.query(
                    "SELECT hive.f_int, (hive.f_str || ' ' || pcollection.f_string) AS f_string \n"
                        + "FROM `hive`.`default`.`mytable` AS hive \n"
                        + "   INNER JOIN \n"
                        + " PCOLLECTION AS pcollection \n"
                        + "   ON pcollection.f_int = hive.f_int")
                .withTableProvider("hive", hiveTableProvider()));

    PAssert.that(result)
        .containsInAnyOrder(row(1, "record 1 pcollection_1"), row(2, "record 2 pcollection_2"));
    pipeline.run();
  }

  @Test
  public void testJoinMultipleExtraProvidersWithMain() throws Exception {
    initializeHCatalog();

    PCollection<Row> inputMain =
        pipeline.apply("mainInput", create(row(1, "pcollection_1"), row(2, "pcollection_2")));

    PCollection<Row> inputExtra =
        pipeline.apply("extraInput", create(row(1, "_extra_table_1"), row(2, "_extra_table_2")));

    PCollection<Row> result =
        inputMain.apply(
            SqlTransform.query(
                    "SELECT \n"
                        + "   x_tbl.f_int as f_int, \n"
                        + "   (p_tbl.f_string || x_tbl.f_string || ' ' || h_tbl.f_str) AS f_string \n"
                        + "FROM \n"
                        + "     `extraSchema`.`extraTable` AS x_tbl \n"
                        + "  INNER JOIN \n"
                        + "     `hive`.`default`.`mytable` AS h_tbl \n"
                        + "        ON h_tbl.f_int = x_tbl.f_int \n"
                        + "  INNER JOIN \n"
                        + "     PCOLLECTION AS p_tbl \n"
                        + "        ON p_tbl.f_int = x_tbl.f_int")
                .withTableProvider("extraSchema", extraTableProvider("extraTable", inputExtra))
                .withTableProvider("hive", hiveTableProvider()));

    PAssert.that(result)
        .containsInAnyOrder(
            row(1, "pcollection_1_extra_table_1 record 1"),
            row(2, "pcollection_2_extra_table_2 record 2"));
    pipeline.run();
  }

  @Test
  public void testJoinMultipleExtraProvidersWithImplicitHiveDB() throws Exception {
    initializeHCatalog();

    PCollection<Row> inputMain =
        pipeline.apply("mainInput", create(row(1, "pcollection_1"), row(2, "pcollection_2")));

    PCollection<Row> inputExtra =
        pipeline.apply("extraInput", create(row(1, "_extra_table_1"), row(2, "_extra_table_2")));

    PCollection<Row> result =
        inputMain.apply(
            SqlTransform.query(
                    "SELECT \n"
                        + "   x_tbl.f_int as f_int, \n"
                        + "   (p_tbl.f_string || x_tbl.f_string || ' ' || h_tbl.f_str) AS f_string \n"
                        + "FROM \n"
                        + "     `extraSchema`.`extraTable` AS x_tbl \n"
                        + "  INNER JOIN \n"
                        + "     `hive`.`mytable` AS h_tbl \n"
                        + "        ON h_tbl.f_int = x_tbl.f_int \n"
                        + "  INNER JOIN \n"
                        + "     PCOLLECTION AS p_tbl \n"
                        + "        ON p_tbl.f_int = x_tbl.f_int")
                .withTableProvider("extraSchema", extraTableProvider("extraTable", inputExtra))
                .withTableProvider("hive", hiveTableProvider()));

    PAssert.that(result)
        .containsInAnyOrder(
            row(1, "pcollection_1_extra_table_1 record 1"),
            row(2, "pcollection_2_extra_table_2 record 2"));
    pipeline.run();
  }

  private void reCreateTestTable() throws Exception {
    service.executeQuery("drop table " + TEST_TABLE);
    service.executeQuery("create table " + TEST_TABLE + "(f_str string, f_int int)");
  }

  private void initializeHCatalog() throws Exception {
    reCreateTestTable();
    insertTestData(service.getHiveConfAsMap());
  }

  private TableProvider hiveTableProvider() {
    return HCatalogTableProvider.create(service.getHiveConfAsMap());
  }

  private Row row(int fIntValue, String fStringValue) {
    return Row.withSchema(ROW_SCHEMA).addValues(fIntValue, fStringValue).build();
  }

  private TableProvider extraTableProvider(String tableName, PCollection<Row> rows) {
    return new ReadOnlyTableProvider(
        "testExtraTableProvider", ImmutableMap.of(tableName, new BeamPCollectionTable<>(rows)));
  }

  private PTransform<PBegin, PCollection<Row>> create(Row... rows) {
    return Create.of(Arrays.asList(rows)).withRowSchema(ROW_SCHEMA);
  }

  /** Test rows are in the form Row(string f_str='record 156', int f_int=156). */
  public static class ToKV extends DoFn<Row, KV<String, Integer>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      Row row = c.element();
      c.output(KV.of(row.getValue(0), row.getValue(1)));
    }
  }
}
