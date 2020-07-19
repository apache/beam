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
package org.apache.beam.sdk.extensions.sql.meta.provider.text;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.nio.file.Files;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.meta.store.InMemoryMetaStore;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.NumberedShardedFile;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.base.Charsets;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Tests for {@link TextTableProvider}. */
public class TextTableProviderTest {

  @Rule public TestPipeline pipeline = TestPipeline.create();

  @Rule
  public TemporaryFolder tempFolder =
      new TemporaryFolder() {
        @Override
        protected void after() {}
      };

  private static final String SQL_CSV_SCHEMA = "(f_string VARCHAR, f_int INT)";
  private static final Schema CSV_SCHEMA =
      Schema.builder()
          .addNullableField("f_string", Schema.FieldType.STRING)
          .addNullableField("f_int", Schema.FieldType.INT32)
          .build();

  private static final Schema LINES_SCHEMA = Schema.builder().addStringField("f_string").build();
  private static final String SQL_LINES_SCHEMA = "(f_string VARCHAR)";

  private static final Schema JSON_SCHEMA =
      Schema.builder().addStringField("name").addInt32Field("age").build();
  private static final String SQL_JSON_SCHEMA = "(name VARCHAR, age INTEGER)";
  private static final String JSON_TEXT = "{\"name\":\"Jack\",\"age\":13}";
  private static final String INVALID_JSON_TEXT = "{\"name\":\"Jack\",\"age\":\"thirteen\"}";

  // Even though these have the same schema as LINES_SCHEMA, that is accidental; they exist for a
  // different purpose, to test Excel CSV format that does not ignore empty lines
  private static final Schema SINGLE_STRING_CSV_SCHEMA =
      Schema.builder().addStringField("f_string").build();
  private static final String SINGLE_STRING_SQL_SCHEMA = "(f_string VARCHAR)";

  /**
   * Tests {@code CREATE EXTERNAL TABLE TYPE text} with no format reads a default CSV.
   *
   * <p>The default format ignores empty lines, so that is an important part of this test.
   */
  @Test
  public void testLegacyDefaultCsv() throws Exception {
    Files.write(
        tempFolder.newFile("test.csv").toPath(),
        "hello,13\n\ngoodbye,42\n".getBytes(Charsets.UTF_8));

    BeamSqlEnv env = BeamSqlEnv.inMemory(new TextTableProvider());
    env.executeDdl(
        String.format(
            "CREATE EXTERNAL TABLE test %s TYPE text LOCATION '%s/*'",
            SQL_CSV_SCHEMA, tempFolder.getRoot()));

    PCollection<Row> rows =
        BeamSqlRelUtils.toPCollection(pipeline, env.parseQuery("SELECT * FROM test"));

    PAssert.that(rows)
        .containsInAnyOrder(
            Row.withSchema(CSV_SCHEMA).addValues("hello", 13).build(),
            Row.withSchema(CSV_SCHEMA).addValues("goodbye", 42).build());
    pipeline.run();
  }

  /**
   * Tests {@code CREATE EXTERNAL TABLE TYPE text} with a format other than "csv" or "lines" results
   * in a CSV read of that format.
   */
  @Test
  public void testLegacyTdfCsv() throws Exception {
    Files.write(
        tempFolder.newFile("test.csv").toPath(),
        "hello\t13\n\ngoodbye\t42\n".getBytes(Charsets.UTF_8));

    BeamSqlEnv env = BeamSqlEnv.inMemory(new TextTableProvider());
    env.executeDdl(
        String.format(
            "CREATE EXTERNAL TABLE test %s TYPE text LOCATION '%s/*' TBLPROPERTIES '{\"format\":\"TDF\"}'",
            SQL_CSV_SCHEMA, tempFolder.getRoot()));

    PCollection<Row> rows =
        BeamSqlRelUtils.toPCollection(pipeline, env.parseQuery("SELECT * FROM test"));

    rows.apply(
        MapElements.into(TypeDescriptors.voids())
            .via(
                r -> {
                  System.out.println(r.toString());
                  return null;
                }));

    PAssert.that(rows)
        .containsInAnyOrder(
            Row.withSchema(CSV_SCHEMA).addValues("hello", 13).build(),
            Row.withSchema(CSV_SCHEMA).addValues("goodbye", 42).build());
    pipeline.run();
  }

  /**
   * Tests {@code CREATE EXTERNAL TABLE TYPE text TBLPROPERTIES '{"format":"csv"}'} works as
   * expected.
   */
  @Test
  public void testExplicitCsv() throws Exception {
    Files.write(
        tempFolder.newFile("test.csv").toPath(),
        "hello,13\n\ngoodbye,42\n".getBytes(Charsets.UTF_8));

    BeamSqlEnv env = BeamSqlEnv.inMemory(new TextTableProvider());
    env.executeDdl(
        String.format(
            "CREATE EXTERNAL TABLE test %s TYPE text LOCATION '%s/*' TBLPROPERTIES '{\"format\":\"csv\"}'",
            SQL_CSV_SCHEMA, tempFolder.getRoot()));

    PCollection<Row> rows =
        BeamSqlRelUtils.toPCollection(pipeline, env.parseQuery("SELECT * FROM test"));

    PAssert.that(rows)
        .containsInAnyOrder(
            Row.withSchema(CSV_SCHEMA).addValues("hello", 13).build(),
            Row.withSchema(CSV_SCHEMA).addValues("goodbye", 42).build());
    pipeline.run();
  }

  /**
   * Tests {@code CREATE EXTERNAL TABLE TYPE text TBLPROPERTIES '{"format":"csv", "csvFormat":
   * "Excel"}'} works as expected.
   *
   * <p>Not that the different with "Excel" format is that blank lines are not ignored but have a
   * single string field.
   */
  @Test
  public void testExplicitCsvExcel() throws Exception {
    Files.write(
        tempFolder.newFile("test.csv").toPath(), "hello\n\ngoodbye\n".getBytes(Charsets.UTF_8));

    BeamSqlEnv env = BeamSqlEnv.inMemory(new TextTableProvider());
    env.executeDdl(
        String.format(
            "CREATE EXTERNAL TABLE test %s TYPE text LOCATION '%s/*' "
                + "TBLPROPERTIES '{\"format\":\"csv\", \"csvFormat\":\"Excel\"}'",
            SINGLE_STRING_SQL_SCHEMA, tempFolder.getRoot()));

    PCollection<Row> rows =
        BeamSqlRelUtils.toPCollection(pipeline, env.parseQuery("SELECT * FROM test"));

    PAssert.that(rows)
        .containsInAnyOrder(
            Row.withSchema(SINGLE_STRING_CSV_SCHEMA).addValues("hello").build(),
            Row.withSchema(SINGLE_STRING_CSV_SCHEMA).addValues("goodbye").build());
    pipeline.run();
  }

  /**
   * Tests {@code CREATE EXTERNAL TABLE TYPE text TBLPROPERTIES '{"format":"lines"}'} works as
   * expected.
   */
  @Test
  public void testLines() throws Exception {
    // Data that looks like CSV but isn't parsed as it
    Files.write(
        tempFolder.newFile("test.csv").toPath(), "hello,13\ngoodbye,42\n".getBytes(Charsets.UTF_8));

    BeamSqlEnv env = BeamSqlEnv.inMemory(new TextTableProvider());
    env.executeDdl(
        String.format(
            "CREATE EXTERNAL TABLE test %s TYPE text LOCATION '%s/*' TBLPROPERTIES '{\"format\":\"lines\"}'",
            SQL_LINES_SCHEMA, tempFolder.getRoot()));

    PCollection<Row> rows =
        BeamSqlRelUtils.toPCollection(pipeline, env.parseQuery("SELECT * FROM test"));

    PAssert.that(rows)
        .containsInAnyOrder(
            Row.withSchema(LINES_SCHEMA).addValues("hello,13").build(),
            Row.withSchema(LINES_SCHEMA).addValues("goodbye,42").build());
    pipeline.run();
  }

  @Test
  public void testJson() throws Exception {
    Files.write(tempFolder.newFile("test.json").toPath(), JSON_TEXT.getBytes(Charsets.UTF_8));

    BeamSqlEnv env = BeamSqlEnv.inMemory(new TextTableProvider());
    env.executeDdl(
        String.format(
            "CREATE EXTERNAL TABLE test %s TYPE text LOCATION '%s/*' TBLPROPERTIES '{\"format\":\"json\"}'",
            SQL_JSON_SCHEMA, tempFolder.getRoot()));

    PCollection<Row> rows =
        BeamSqlRelUtils.toPCollection(pipeline, env.parseQuery("SELECT * FROM test"));

    PAssert.that(rows)
        .containsInAnyOrder(Row.withSchema(JSON_SCHEMA).addValues("Jack", 13).build());
    pipeline.run();
  }

  @Test
  public void testInvalidJson() throws Exception {
    File deadLetterFile = new File(tempFolder.getRoot(), "dead-letter-file");
    Files.write(
        tempFolder.newFile("test.json").toPath(), INVALID_JSON_TEXT.getBytes(Charsets.UTF_8));

    BeamSqlEnv env = BeamSqlEnv.inMemory(new TextTableProvider());
    env.executeDdl(
        String.format(
            "CREATE EXTERNAL TABLE test %s TYPE text LOCATION '%s/*' "
                + "TBLPROPERTIES '{\"format\":\"json\", \"deadLetterFile\": \"%s\"}'",
            SQL_JSON_SCHEMA, tempFolder.getRoot(), deadLetterFile.getAbsoluteFile()));

    PCollection<Row> rows =
        BeamSqlRelUtils.toPCollection(pipeline, env.parseQuery("SELECT * FROM test"));

    PAssert.that(rows).empty();

    pipeline.run();
    assertThat(
        new NumberedShardedFile(deadLetterFile.getAbsoluteFile() + "*")
            .readFilesWithRetries(Sleeper.DEFAULT, BackOff.STOP_BACKOFF),
        containsInAnyOrder(INVALID_JSON_TEXT));
  }

  @Test
  public void testWriteLines() throws Exception {
    File destinationFile = new File(tempFolder.getRoot(), "lines-outputs");
    BeamSqlEnv env = BeamSqlEnv.inMemory(new TextTableProvider());
    env.executeDdl(
        String.format(
            "CREATE EXTERNAL TABLE test %s TYPE text LOCATION '%s' TBLPROPERTIES '{\"format\":\"lines\"}'",
            SQL_LINES_SCHEMA, destinationFile.getAbsolutePath()));

    BeamSqlRelUtils.toPCollection(
        pipeline, env.parseQuery("INSERT INTO test VALUES ('hello'), ('goodbye')"));
    pipeline.run();

    assertThat(
        new NumberedShardedFile(destinationFile.getAbsolutePath() + "*")
            .readFilesWithRetries(Sleeper.DEFAULT, BackOff.STOP_BACKOFF),
        containsInAnyOrder("hello", "goodbye"));
  }

  @Test
  public void testWriteCsv() throws Exception {
    File destinationFile = new File(tempFolder.getRoot(), "csv-outputs");
    BeamSqlEnv env = BeamSqlEnv.inMemory(new TextTableProvider());

    // NumberedShardedFile
    env.executeDdl(
        String.format(
            "CREATE EXTERNAL TABLE test %s TYPE text LOCATION '%s' TBLPROPERTIES '{\"format\":\"csv\"}'",
            SQL_CSV_SCHEMA, destinationFile.getAbsolutePath()));

    BeamSqlRelUtils.toPCollection(
        pipeline, env.parseQuery("INSERT INTO test VALUES ('hello', 42), ('goodbye', 13)"));
    pipeline.run();

    assertThat(
        new NumberedShardedFile(destinationFile.getAbsolutePath() + "*")
            .readFilesWithRetries(Sleeper.DEFAULT, BackOff.STOP_BACKOFF),
        containsInAnyOrder("hello,42", "goodbye,13"));
  }

  @Test
  public void testWriteJson() throws Exception {
    File destinationFile = new File(tempFolder.getRoot(), "json-outputs");
    BeamSqlEnv env = BeamSqlEnv.inMemory(new TextTableProvider());
    env.executeDdl(
        String.format(
            "CREATE EXTERNAL TABLE test %s TYPE text LOCATION '%s' TBLPROPERTIES '{\"format\":\"json\"}'",
            SQL_JSON_SCHEMA, destinationFile.getAbsolutePath()));

    BeamSqlRelUtils.toPCollection(
        pipeline, env.parseQuery("INSERT INTO test(name, age) VALUES ('Jack', 13)"));
    pipeline.run();

    assertThat(
        new NumberedShardedFile(destinationFile.getAbsolutePath() + "*")
            .readFilesWithRetries(Sleeper.DEFAULT, BackOff.STOP_BACKOFF),
        containsInAnyOrder(JSON_TEXT));
  }

  /**
   * To run this test, need to set the environment variable "GOOGLE_APPLICATION_CREDENTIALS" via edit configurations
   * @throws Exception
   */
  @Test
  public void testTpcdsNaiveQueryDirect() throws Exception {
    InMemoryMetaStore inMemoryMetaStore = new InMemoryMetaStore();
    inMemoryMetaStore.registerProvider(new TextTableProvider());
    BeamSqlEnv env =
            BeamSqlEnv
                    .builder(inMemoryMetaStore)
                    .setPipelineOptions(pipeline.getOptions())
                    .build();

    env.executeDdl(
            String.format(
                    "CREATE EXTERNAL TABLE item (%s) TYPE text LOCATION '%s' "
                            + "TBLPROPERTIES '{\"format\":\"csv\", \"csvformat\": \"InformixUnload\"}'",
                    "i_item_sk bigint,"
                            + "i_item_id varchar,"
                            + "i_rec_start_date varchar,"
                            + "i_rec_end_date varchar,"
                            + "i_item_desc varchar,"
                            + "i_current_price double,"
                            + "i_wholesale_cost double,"
                            + "i_brand_id bigint,"
                            + "i_brand varchar,"
                            + "i_class_id bigint,"
                            + "i_class varchar,"
                            + "i_category_id bigint,"
                            + "i_category varchar,"
                            + "i_manufact_id bigint,"
                            + "i_manufact varchar,"
                            + "i_size varchar,"
                            + "i_formulation varchar,"
                            + "i_color varchar,"
                            + "i_units varchar,"
                            + "i_container varchar,"
                            + "i_manager_id bigint,"
                            + "i_product_name varchar",
                    "gs://beamsql_tpcds_1/data/1G/item.dat"
            ));

    PCollection<Row> rows = BeamSqlRelUtils.toPCollection(pipeline, env.parseQuery("SELECT * FROM item LIMIT 15"));

//    PCollection<Row> a = rows.apply(MapElements
//            .into(TypeDescriptors.rows())
//            .via((Row row) -> {
//              System.out.println(row);
//              return row;
//            })).setRowSchema(rows.getSchema());

    PCollection<String> rowStrings = rows.apply(MapElements
            .into(TypeDescriptors.strings())
            .via((Row row) -> row.toString()));

    rowStrings.apply(TextIO.write().to("gs://beamsql_tpcds_1/tpcds_results/1G/item").withSuffix(".txt").withNumShards(1));

    pipeline.run();
  }

  /**
   * To run this test, need to set the environment variable "GOOGLE_APPLICATION_CREDENTIALS" via edit configurations
   * @throws Exception
   */
  @Test
  public void testTpcdsQ3DataFlow() throws Exception {
    InMemoryMetaStore inMemoryMetaStore = new InMemoryMetaStore();
    inMemoryMetaStore.registerProvider(new TextTableProvider());

    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setProject("apache-beam-testing");
    options.setStagingLocation("gs://beamsql_tpcds_1/binaries");
    options.setTempLocation("gs://beamsql_tpcds_2/temp");
    options.setRunner(DataflowRunner.class);
    options.setMaxNumWorkers(100);
    options.setRegion("us-west1");

    Pipeline testPipeline = Pipeline.create(options);

    BeamSqlEnv env =
            BeamSqlEnv
                    .builder(inMemoryMetaStore)
                    .setPipelineOptions(testPipeline.getOptions())
                    .build();

    env.executeDdl(
            String.format(
                    "CREATE EXTERNAL TABLE date_dim (%s) TYPE text LOCATION '%s' "
                            + "TBLPROPERTIES '{\"format\":\"csv\", \"csvformat\": \"InformixUnload\"}'",
                    "d_date_sk bigint, \n"
                            + "d_date_id varchar, \n"
                            + "d_date varchar, \n"
                            + "d_month_seq bigint, \n"
                            + "d_week_seq bigint, \n"
                            + "d_quarter_seq bigint, \n"
                            + "d_year bigint, \n"
                            + "d_dow bigint, \n"
                            + "d_moy bigint, \n"
                            + "d_dom bigint, \n"
                            + "d_qoy bigint, \n"
                            + "d_fy_year bigint, \n"
                            + "d_fy_quarter_seq bigint, \n"
                            + "d_fy_week_seq bigint, \n"
                            + "d_day_name varchar, \n"
                            + "d_quarter_name varchar, \n"
                            + "d_holiday varchar, \n"
                            + "d_weekend varchar, \n"
                            + "d_following_holiday varchar, \n"
                            + "d_first_dom bigint, \n"
                            + "d_last_dom bigint, \n"
                            + "d_same_day_ly bigint, \n"
                            + "d_same_day_lq bigint, \n"
                            + "d_current_day varchar, \n"
                            + "d_current_week varchar, \n"
                            + "d_current_month varchar, \n"
                            + "d_current_quarter varchar, \n"
                            + "d_current_year varchar",
                    "gs://beamsql_tpcds_1/data/1G/date_dim.dat"
            ));

    env.executeDdl(
            String.format(
                    "CREATE EXTERNAL TABLE store_sales (%s) TYPE text LOCATION '%s' "
                            + "TBLPROPERTIES '{\"format\":\"csv\", \"csvformat\": \"InformixUnload\"}'",
                    "ss_sold_date_sk bigint, \n"
                            + "ss_sold_time_sk bigint, \n"
                            + "ss_item_sk bigint, \n"
                            + "ss_customer_sk bigint, \n"
                            + "ss_cdemo_sk bigint, \n"
                            + "ss_hdemo_sk bigint, \n"
                            + "ss_addr_sk bigint, \n"
                            + "ss_store_sk bigint, \n"
                            + "ss_promo_sk bigint, \n"
                            + "ss_ticket_number bigint, \n"
                            + "ss_quantity bigint, \n"
                            + "ss_wholesale_cost double, \n"
                            + "ss_list_price double, \n"
                            + "ss_sales_price double, \n"
                            + "ss_ext_discount_amt double, \n"
                            + "ss_ext_sales_price double, \n"
                            + "ss_ext_wholesale_cost double, \n"
                            + "ss_ext_list_price double, \n"
                            + "ss_ext_tax double, \n"
                            + "ss_coupon_amt double, \n"
                            + "ss_net_paid double, \n"
                            + "ss_net_paid_inc_tax double, \n"
                            + "ss_net_profit double",
                    "gs://beamsql_tpcds_1/data/1G/store_sales.dat"
            ));

    env.executeDdl(
            String.format(
                    "CREATE EXTERNAL TABLE item (%s) TYPE text LOCATION '%s' "
                            + "TBLPROPERTIES '{\"format\":\"csv\", \"csvformat\": \"InformixUnload\"}'",
                    "i_item_sk bigint,\n"
                            + "i_item_id varchar, \n"
                            + "i_rec_start_date varchar, \n"
                            + "i_rec_end_date varchar, \n"
                            + "i_item_desc varchar, \n"
                            + "i_current_price double, \n"
                            + "i_wholesale_cost double, \n"
                            + "i_brand_id bigint, \n"
                            + "i_brand varchar,\n"
                            + "i_class_id bigint,\n"
                            + "i_class varchar,\n"
                            + "i_category_id bigint,\n"
                            + "i_category varchar,\n"
                            + "i_manufact_id bigint,\n"
                            + "i_manufact varchar,\n"
                            + "i_size varchar,\n"
                            + "i_formulation varchar,\n"
                            + "i_color varchar,\n"
                            + "i_units varchar,\n"
                            + "i_container varchar,\n"
                            + "i_manager_id bigint,\n"
                            + "i_product_name varchar",
                    "gs://beamsql_tpcds_1/data/1G/item.dat"
            ));



    String queryString3 = "select  dt.d_year \n" +
            "       ,item.i_brand_id brand_id \n" +
            "       ,item.i_brand brand\n" +
            "       ,sum(ss_ext_sales_price) sum_agg\n" +
            " from  date_dim dt \n" +
            "      ,store_sales\n" +
            "      ,item\n" +
            " where dt.d_date_sk = store_sales.ss_sold_date_sk\n" +
            "   and store_sales.ss_item_sk = item.i_item_sk\n" +
            "   and item.i_manufact_id = 436\n" +
            "   and dt.d_moy=12\n" +
            " group by dt.d_year\n" +
            "      ,item.i_brand\n" +
            "      ,item.i_brand_id\n" +
            " order by dt.d_year\n" +
            "         ,sum_agg desc\n" +
            "         ,brand_id\n" +
            " limit 100";

    PCollection<Row> rows = BeamSqlRelUtils.toPCollection(testPipeline, env.parseQuery(queryString3));

    PCollection<String> rowStrings = rows.apply(MapElements
            .into(TypeDescriptors.strings())
            .via((Row row) -> row.toString()));

    rowStrings.apply(TextIO.write().to("gs://beamsql_tpcds_1/tpcds_results/1G/Q3").withSuffix(".txt").withNumShards(1));

    testPipeline.run();
  }

  /**
   * To run this test, need to set the environment variable "GOOGLE_APPLICATION_CREDENTIALS" via edit configurations
   * @throws Exception
   */
  @Test
  public void testTpcdsQ55DataFlow() throws Exception {
    InMemoryMetaStore inMemoryMetaStore = new InMemoryMetaStore();
    inMemoryMetaStore.registerProvider(new TextTableProvider());

    DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setProject("apache-beam-testing");
    options.setStagingLocation("gs://beamsql_tpcds_1/binaries");
    options.setTempLocation("gs://beamsql_tpcds_2/temp");
    options.setRunner(DataflowRunner.class);
    options.setMaxNumWorkers(100);
    options.setRegion("us-west1");

    Pipeline testPipeline = Pipeline.create(options);

    BeamSqlEnv env =
            BeamSqlEnv
                    .builder(inMemoryMetaStore)
                    .setPipelineOptions(testPipeline.getOptions())
                    .build();

    env.executeDdl(
            String.format(
                    "CREATE EXTERNAL TABLE date_dim (%s) TYPE text LOCATION '%s' "
                            + "TBLPROPERTIES '{\"format\":\"csv\", \"csvformat\": \"InformixUnload\"}'",
                    "d_date_sk bigint, \n"
                            + "d_date_id varchar, \n"
                            + "d_date varchar, \n"
                            + "d_month_seq bigint, \n"
                            + "d_week_seq bigint, \n"
                            + "d_quarter_seq bigint, \n"
                            + "d_year bigint, \n"
                            + "d_dow bigint, \n"
                            + "d_moy bigint, \n"
                            + "d_dom bigint, \n"
                            + "d_qoy bigint, \n"
                            + "d_fy_year bigint, \n"
                            + "d_fy_quarter_seq bigint, \n"
                            + "d_fy_week_seq bigint, \n"
                            + "d_day_name varchar, \n"
                            + "d_quarter_name varchar, \n"
                            + "d_holiday varchar, \n"
                            + "d_weekend varchar, \n"
                            + "d_following_holiday varchar, \n"
                            + "d_first_dom bigint, \n"
                            + "d_last_dom bigint, \n"
                            + "d_same_day_ly bigint, \n"
                            + "d_same_day_lq bigint, \n"
                            + "d_current_day varchar, \n"
                            + "d_current_week varchar, \n"
                            + "d_current_month varchar, \n"
                            + "d_current_quarter varchar, \n"
                            + "d_current_year varchar",
                    "gs://beamsql_tpcds_1/data/1G/date_dim.dat"
            ));

    env.executeDdl(
            String.format(
                    "CREATE EXTERNAL TABLE store_sales (%s) TYPE text LOCATION '%s' "
                            + "TBLPROPERTIES '{\"format\":\"csv\", \"csvformat\": \"InformixUnload\"}'",
                    "ss_sold_date_sk bigint, \n"
                            + "ss_sold_time_sk bigint, \n"
                            + "ss_item_sk bigint, \n"
                            + "ss_customer_sk bigint, \n"
                            + "ss_cdemo_sk bigint, \n"
                            + "ss_hdemo_sk bigint, \n"
                            + "ss_addr_sk bigint, \n"
                            + "ss_store_sk bigint, \n"
                            + "ss_promo_sk bigint, \n"
                            + "ss_ticket_number bigint, \n"
                            + "ss_quantity bigint, \n"
                            + "ss_wholesale_cost double, \n"
                            + "ss_list_price double, \n"
                            + "ss_sales_price double, \n"
                            + "ss_ext_discount_amt double, \n"
                            + "ss_ext_sales_price double, \n"
                            + "ss_ext_wholesale_cost double, \n"
                            + "ss_ext_list_price double, \n"
                            + "ss_ext_tax double, \n"
                            + "ss_coupon_amt double, \n"
                            + "ss_net_paid double, \n"
                            + "ss_net_paid_inc_tax double, \n"
                            + "ss_net_profit double",
                    "gs://beamsql_tpcds_1/data/1G/store_sales.dat"
            ));

    env.executeDdl(
            String.format(
                    "CREATE EXTERNAL TABLE item (%s) TYPE text LOCATION '%s' "
                            + "TBLPROPERTIES '{\"format\":\"csv\", \"csvformat\": \"InformixUnload\"}'",
                    "i_item_sk bigint,\n"
                            + "i_item_id varchar, \n"
                            + "i_rec_start_date varchar, \n"
                            + "i_rec_end_date varchar, \n"
                            + "i_item_desc varchar, \n"
                            + "i_current_price double, \n"
                            + "i_wholesale_cost double, \n"
                            + "i_brand_id bigint, \n"
                            + "i_brand varchar,\n"
                            + "i_class_id bigint,\n"
                            + "i_class varchar,\n"
                            + "i_category_id bigint,\n"
                            + "i_category varchar,\n"
                            + "i_manufact_id bigint,\n"
                            + "i_manufact varchar,\n"
                            + "i_size varchar,\n"
                            + "i_formulation varchar,\n"
                            + "i_color varchar,\n"
                            + "i_units varchar,\n"
                            + "i_container varchar,\n"
                            + "i_manager_id bigint,\n"
                            + "i_product_name varchar",
                    "gs://beamsql_tpcds_1/data/1G/item.dat"
            ));



    String queryString55 = "select  i_brand_id brand_id, i_brand brand,\n" +
            " \tsum(ss_ext_sales_price) ext_price\n" +
            " from date_dim, store_sales, item\n" +
            " where d_date_sk = ss_sold_date_sk\n" +
            " \tand ss_item_sk = i_item_sk\n" +
            " \tand i_manager_id=36\n" +
            " \tand d_moy=12\n" +
            " \tand d_year=2001\n" +
            " group by i_brand, i_brand_id\n" +
            " order by ext_price desc, i_brand_id\n" +
            "limit 100";

    PCollection<Row> rows = BeamSqlRelUtils.toPCollection(testPipeline, env.parseQuery(queryString55));

    PCollection<String> rwoStrings = rows.apply(MapElements
            .into(TypeDescriptors.strings())
            .via((Row row) -> row.toString()));

    rwoStrings.apply(TextIO.write().to("gs://beamsql_tpcds_1/tpcds_results/1G/Q55").withSuffix(".txt").withNumShards(1));

    testPipeline.run();
  }
}
