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
package org.apache.beam.sdk.extensions.sql.meta.provider.delta;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import java.io.File;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.BeamSqlCli;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamEnumerableConverter;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.meta.catalog.InMemoryCatalogManager;
import org.apache.beam.sdk.io.delta.DeltaWriteTestUtils;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.schemas.logicaltypes.Timestamp;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Integration/functional tests for Delta Lake read support in Beam SQL. */
@RunWith(JUnit4.class)
public class BeamSqlDeltaTest {

  @Rule public TestPipeline readPipeline = TestPipeline.create();
  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private InMemoryCatalogManager catalogManager;
  private BeamSqlCli cli;
  private BeamSqlEnv sqlEnv;

  private static final Schema PERSON_SCHEMA =
      Schema.builder()
          .addStringField("name")
          .addInt32Field("age")
          .addDoubleField("score")
          .addStringField("country")
          .build();

  private static final StructType PERSON_DELTA_SCHEMA =
      new StructType()
          .add("name", StringType.STRING)
          .add("age", IntegerType.INTEGER)
          .add("score", DoubleType.DOUBLE)
          .add("country", StringType.STRING);

  private static final Row PERSON_1 =
      Row.withSchema(PERSON_SCHEMA).addValues("Alice", 30, 95.5, "USA").build();
  private static final Row PERSON_2 =
      Row.withSchema(PERSON_SCHEMA).addValues("Bob", 20, 70.0, "USA").build();
  private static final Row PERSON_3 =
      Row.withSchema(PERSON_SCHEMA).addValues("Charlie", 40, 85.0, "Canada").build();
  private static final Row PERSON_4 =
      Row.withSchema(PERSON_SCHEMA).addValues("Dave", 28, 88.0, "UK").build();

  @Before
  public void setUp() {
    catalogManager = new InMemoryCatalogManager();
    catalogManager.registerTableProvider(new DeltaTableProvider());
    cli = new BeamSqlCli().catalogManager(catalogManager);
    sqlEnv =
        BeamSqlEnv.builder(catalogManager)
            .setPipelineOptions(PipelineOptionsFactory.create())
            .build();
  }

  private void createDeltaTable(File tableDir, List<Row> rows) throws Exception {
    createDeltaTable(tableDir, rows, PERSON_DELTA_SCHEMA);
  }

  private void createDeltaTable(File tableDir, List<Row> rows, StructType deltaSchema)
      throws Exception {
    Engine engine = DefaultEngine.create(new Configuration());
    DeltaWriteTestUtils.writeAppendCommit(
        engine, tableDir.getAbsolutePath(), 0L, 100000000000L, deltaSchema, rows);
  }

  private void createTwoVersionDeltaTable(File tableDir) throws Exception {
    Engine engine = DefaultEngine.create(new Configuration());
    Schema schema = Schema.builder().addField("name", Schema.FieldType.STRING).build();
    StructType deltaSchema = new StructType().add("name", StringType.STRING);

    Row row1 = Row.withSchema(schema).addValues("v0-row1").build();
    Row row2 = Row.withSchema(schema).addValues("v0-row2").build();
    Row row3 = Row.withSchema(schema).addValues("v1-row3").build();

    // Commit 0 (version 0 at timestamp 100000000000L)
    DeltaWriteTestUtils.writeAppendCommit(
        engine,
        tableDir.getAbsolutePath(),
        0L,
        100000000000L,
        deltaSchema,
        Arrays.asList(row1, row2));

    // Commit 1 (version 1 at timestamp 200000000000L)
    DeltaWriteTestUtils.writeAppendCommit(
        engine, tableDir.getAbsolutePath(), 1L, 200000000000L, deltaSchema, Arrays.asList(row3));
  }

  @Test
  public void testSelectAll() throws Exception {
    File tableDir = tempFolder.newFolder("delta-table-all");
    createDeltaTable(tableDir, Arrays.asList(PERSON_1, PERSON_2, PERSON_3));

    sqlEnv.executeDdl(
        String.format(
            "CREATE EXTERNAL TABLE persons (\n"
                + "  name VARCHAR,\n"
                + "  age INTEGER,\n"
                + "  score DOUBLE,\n"
                + "  country VARCHAR\n"
                + ")\n"
                + "TYPE 'delta'\n"
                + "LOCATION '%s'",
            tableDir.getAbsolutePath()));

    BeamRelNode relNode = sqlEnv.parseQuery("SELECT * FROM persons");
    PCollection<Row> output = BeamSqlRelUtils.toPCollection(readPipeline, relNode);

    PAssert.that(output).containsInAnyOrder(PERSON_1, PERSON_2, PERSON_3);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testSelectWithProjection() throws Exception {
    File tableDir = tempFolder.newFolder("delta-table-project");
    createDeltaTable(tableDir, Arrays.asList(PERSON_1, PERSON_2, PERSON_3));

    sqlEnv.executeDdl(
        String.format(
            "CREATE EXTERNAL TABLE persons (\n"
                + "  name VARCHAR,\n"
                + "  age INTEGER,\n"
                + "  score DOUBLE,\n"
                + "  country VARCHAR\n"
                + ")\n"
                + "TYPE 'delta'\n"
                + "LOCATION '%s'",
            tableDir.getAbsolutePath()));

    BeamRelNode relNode = sqlEnv.parseQuery("SELECT name, score FROM persons");
    PCollection<Row> output = BeamSqlRelUtils.toPCollection(readPipeline, relNode);

    Schema projectedSchema =
        Schema.builder().addStringField("name").addDoubleField("score").build();

    PAssert.that(output)
        .containsInAnyOrder(
            Row.withSchema(projectedSchema).addValues("Alice", 95.5).build(),
            Row.withSchema(projectedSchema).addValues("Bob", 70.0).build(),
            Row.withSchema(projectedSchema).addValues("Charlie", 85.0).build());

    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testSelectWithFilter() throws Exception {
    File tableDir = tempFolder.newFolder("delta-table-filter");
    createDeltaTable(tableDir, Arrays.asList(PERSON_1, PERSON_2, PERSON_3));

    sqlEnv.executeDdl(
        String.format(
            "CREATE EXTERNAL TABLE persons (\n"
                + "  name VARCHAR,\n"
                + "  age INTEGER,\n"
                + "  score DOUBLE,\n"
                + "  country VARCHAR\n"
                + ")\n"
                + "TYPE 'delta'\n"
                + "LOCATION '%s'",
            tableDir.getAbsolutePath()));

    BeamRelNode relNode = sqlEnv.parseQuery("SELECT * FROM persons WHERE score >= 85.0");
    PCollection<Row> output = BeamSqlRelUtils.toPCollection(readPipeline, relNode);

    PAssert.that(output).containsInAnyOrder(PERSON_1, PERSON_3);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testSelectWithAggregation() throws Exception {
    File tableDir = tempFolder.newFolder("delta-table-agg");
    createDeltaTable(tableDir, Arrays.asList(PERSON_1, PERSON_2, PERSON_3));

    sqlEnv.executeDdl(
        String.format(
            "CREATE EXTERNAL TABLE persons (\n"
                + "  name VARCHAR,\n"
                + "  age INTEGER,\n"
                + "  score DOUBLE,\n"
                + "  country VARCHAR\n"
                + ")\n"
                + "TYPE 'delta'\n"
                + "LOCATION '%s'",
            tableDir.getAbsolutePath()));

    BeamRelNode relNode =
        sqlEnv.parseQuery(
            "SELECT country, COUNT(*) as cnt, AVG(score) as avg_score FROM persons GROUP BY country");
    PCollection<Row> output = BeamSqlRelUtils.toPCollection(readPipeline, relNode);

    Schema aggSchema =
        Schema.builder()
            .addStringField("country")
            .addInt64Field("cnt")
            .addDoubleField("avg_score")
            .build();

    PAssert.that(output)
        .containsInAnyOrder(
            Row.withSchema(aggSchema).addValues("USA", 2L, 82.75).build(),
            Row.withSchema(aggSchema).addValues("Canada", 1L, 85.0).build());

    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testReadWithVersionTimeTravel() throws Exception {
    File tableDir = tempFolder.newFolder("delta-table-ver");
    createTwoVersionDeltaTable(tableDir);

    Schema schema = Schema.builder().addField("name", Schema.FieldType.STRING).build();
    Row row1 = Row.withSchema(schema).addValues("v0-row1").build();
    Row row2 = Row.withSchema(schema).addValues("v0-row2").build();

    sqlEnv.executeDdl(
        String.format(
            "CREATE EXTERNAL TABLE table_v0 (\n"
                + "  name VARCHAR\n"
                + ")\n"
                + "TYPE 'delta'\n"
                + "LOCATION '%s'\n"
                + "TBLPROPERTIES '{\"version\": 0}'",
            tableDir.getAbsolutePath()));

    BeamRelNode relNode = sqlEnv.parseQuery("SELECT * FROM table_v0");
    PCollection<Row> output = BeamSqlRelUtils.toPCollection(readPipeline, relNode);

    PAssert.that(output).containsInAnyOrder(row1, row2);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testReadWithTimestampTimeTravel() throws Exception {
    File tableDir = tempFolder.newFolder("delta-table-ts");
    createTwoVersionDeltaTable(tableDir);

    Schema schema = Schema.builder().addField("name", Schema.FieldType.STRING).build();
    Row row1 = Row.withSchema(schema).addValues("v0-row1").build();
    Row row2 = Row.withSchema(schema).addValues("v0-row2").build();

    String timestampV0 = java.time.Instant.ofEpochMilli(150000000000L).toString();

    sqlEnv.executeDdl(
        String.format(
            "CREATE EXTERNAL TABLE table_ts (\n"
                + "  name VARCHAR\n"
                + ")\n"
                + "TYPE 'delta'\n"
                + "LOCATION '%s'\n"
                + "TBLPROPERTIES '{\"timestamp\": \"%s\"}'",
            tableDir.getAbsolutePath(), timestampV0));

    BeamRelNode relNode = sqlEnv.parseQuery("SELECT * FROM table_ts");
    PCollection<Row> output = BeamSqlRelUtils.toPCollection(readPipeline, relNode);

    PAssert.that(output).containsInAnyOrder(row1, row2);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testInsertIntoTableFails() throws Exception {
    File tableDir = tempFolder.newFolder("delta-table-insert");
    createDeltaTable(tableDir, Arrays.asList(PERSON_1));

    sqlEnv.executeDdl(
        String.format(
            "CREATE EXTERNAL TABLE persons (\n"
                + "  name VARCHAR,\n"
                + "  age INTEGER,\n"
                + "  score DOUBLE,\n"
                + "  country VARCHAR\n"
                + ")\n"
                + "TYPE 'delta'\n"
                + "LOCATION '%s'",
            tableDir.getAbsolutePath()));

    BeamRelNode insertRel =
        sqlEnv.parseQuery("INSERT INTO persons VALUES ('Dave', 28, 88.0, 'UK')");

    Pipeline pipeline = Pipeline.create();
    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class,
            () -> BeamSqlRelUtils.toPCollection(pipeline, insertRel));

    assertTrue(
        exception
            .getMessage()
            .contains("Writing to Delta Lake tables is currently not supported."));
  }

  @Test
  public void testCreateCatalogFails() {
    UnsupportedOperationException exception =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                cli.execute(
                    "CREATE CATALOG delta_cat TYPE delta PROPERTIES ('warehouse' = '/path')"));

    assertTrue(
        exception.getMessage().contains("Could not find type 'delta' for catalog 'delta_cat'."));
  }

  @Test
  public void testCreateTableWithoutLocationFails() {
    assertThrows(
        Exception.class,
        () -> sqlEnv.executeDdl("CREATE EXTERNAL TABLE no_location (name VARCHAR) TYPE 'delta'"));
  }

  @Test
  public void testSelectDateAndTimestamp() throws Exception {
    File tableDir = tempFolder.newFolder("delta-table-date-ts");
    Engine engine = DefaultEngine.create(new Configuration());

    Schema beamSchema =
        Schema.builder()
            .addInt32Field("id")
            .addField("ts_col", Schema.FieldType.logicalType(Timestamp.MICROS))
            .addField("date_col", Schema.FieldType.logicalType(SqlTypes.DATE))
            .build();

    StructType deltaSchema =
        new StructType()
            .add("id", IntegerType.INTEGER)
            .add("ts_col", TimestampType.TIMESTAMP)
            .add("date_col", DateType.DATE);

    Instant ts = Instant.parse("2026-08-31T12:00:00.000000Z");
    LocalDate date = LocalDate.of(2026, 8, 31);
    Row inputRow = Row.withSchema(beamSchema).addValues(1, ts, date).build();

    DeltaWriteTestUtils.writeAppendCommit(
        engine,
        tableDir.getAbsolutePath(),
        0L,
        100000000000L,
        deltaSchema,
        Arrays.asList(inputRow));

    sqlEnv.executeDdl(
        String.format(
            "CREATE EXTERNAL TABLE date_ts_table (\n"
                + "  id INTEGER,\n"
                + "  ts_col TIMESTAMP,\n"
                + "  date_col DATE\n"
                + ")\n"
                + "TYPE 'delta'\n"
                + "LOCATION '%s'",
            tableDir.getAbsolutePath()));

    BeamRelNode relNode = sqlEnv.parseQuery("SELECT id, ts_col, date_col FROM date_ts_table");
    PCollection<Row> output = BeamSqlRelUtils.toPCollection(readPipeline, relNode);

    Schema sqlOutputSchema =
        Schema.builder()
            .addInt32Field("id")
            .addField("ts_col", Schema.FieldType.DATETIME)
            .addField("date_col", Schema.FieldType.logicalType(SqlTypes.DATE))
            .build();
    Row expectedRow =
        Row.withSchema(sqlOutputSchema)
            .addValues(1, new org.joda.time.Instant(ts.toEpochMilli()), date)
            .build();

    PAssert.that(output).containsInAnyOrder(expectedRow);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testSelectTimestampNtz() throws Exception {
    File tableDir = tempFolder.newFolder("delta-table-ts-ntz");
    Engine engine = DefaultEngine.create(new Configuration());

    Schema beamSchema =
        Schema.builder()
            .addInt32Field("id")
            .addField("ts_ntz_col", Schema.FieldType.logicalType(SqlTypes.DATETIME))
            .build();

    StructType deltaSchema =
        new StructType()
            .add("id", IntegerType.INTEGER)
            .add("ts_ntz_col", TimestampNTZType.TIMESTAMP_NTZ);

    LocalDateTime dt = LocalDateTime.of(2026, 8, 31, 12, 0, 0);
    Row inputRow = Row.withSchema(beamSchema).addValues(1, dt).build();

    DeltaWriteTestUtils.writeAppendCommit(
        engine,
        tableDir.getAbsolutePath(),
        0L,
        100000000000L,
        deltaSchema,
        Arrays.asList(inputRow));

    sqlEnv.executeDdl(
        String.format(
            "CREATE EXTERNAL TABLE ts_ntz_table (\n"
                + "  id INTEGER,\n"
                + "  ts_ntz_col TIMESTAMP\n"
                + ")\n"
                + "TYPE 'delta'\n"
                + "LOCATION '%s'",
            tableDir.getAbsolutePath()));

    BeamRelNode relNode = sqlEnv.parseQuery("SELECT id, ts_ntz_col FROM ts_ntz_table");
    PCollection<Row> output = BeamSqlRelUtils.toPCollection(readPipeline, relNode);

    Schema sqlOutputSchema =
        Schema.builder()
            .addInt32Field("id")
            .addField("ts_ntz_col", Schema.FieldType.DATETIME)
            .build();
    Instant dtInst = dt.toInstant(java.time.ZoneOffset.UTC);
    Row expectedRow =
        Row.withSchema(sqlOutputSchema)
            .addValues(1, new org.joda.time.Instant(dtInst.toEpochMilli()))
            .build();

    PAssert.that(output).containsInAnyOrder(expectedRow);
    readPipeline.run().waitUntilFinish();
  }

  /**
   * Writes a Delta Lake table with three versions and registers it as {@code name}: version 0
   * inserts Alice and Bob, version 1 inserts Charlie, and version 2 updates Alice and deletes Bob.
   */
  private File createChangesTable(String name) throws Exception {
    File tableDir = tempFolder.newFolder(name);
    Engine engine = DefaultEngine.create(new Configuration());

    // Version 0 (at 100000000000L): insert PERSON_1 (Alice) and PERSON_2 (Bob)
    DeltaWriteTestUtils.writeAppendCommit(
        engine,
        tableDir.getAbsolutePath(),
        0L,
        100000000000L,
        PERSON_DELTA_SCHEMA,
        Arrays.asList(PERSON_1, PERSON_2));

    // Version 1 (at 200000000000L): insert PERSON_3 (Charlie)
    DeltaWriteTestUtils.writeAppendCommit(
        engine,
        tableDir.getAbsolutePath(),
        1L,
        200000000000L,
        PERSON_DELTA_SCHEMA,
        Collections.singletonList(PERSON_3));

    // Version 2 (at 300000000000L): update Alice's age/score and delete Bob
    Schema cdcFileBeamSchema =
        Schema.builder()
            .addFields(PERSON_SCHEMA.getFields())
            .addStringField("_change_type")
            .build();
    StructType cdcFileDeltaSchema = PERSON_DELTA_SCHEMA.add("_change_type", StringType.STRING);
    Row alicePre =
        Row.withSchema(cdcFileBeamSchema)
            .addValues("Alice", 30, 95.5, "USA", "update_preimage")
            .build();
    Row alicePost =
        Row.withSchema(cdcFileBeamSchema)
            .addValues("Alice", 31, 98.0, "USA", "update_postimage")
            .build();
    Row bobDelete =
        Row.withSchema(cdcFileBeamSchema).addValues("Bob", 20, 70.0, "USA", "delete").build();
    DeltaWriteTestUtils.writeCdcCommit(
        engine,
        tableDir.getAbsolutePath(),
        2L,
        300000000000L,
        PERSON_DELTA_SCHEMA,
        null,
        null,
        Arrays.asList(alicePre, alicePost, bobDelete),
        cdcFileDeltaSchema);

    sqlEnv.executeDdl(
        format(
            "CREATE EXTERNAL TABLE %s (\n"
                + "  name VARCHAR,\n"
                + "  age INTEGER,\n"
                + "  score DOUBLE,\n"
                + "  country VARCHAR\n"
                + ")\n"
                + "TYPE 'delta'\n"
                + "LOCATION '%s'",
            name, tableDir.getAbsolutePath()));
    return tableDir;
  }

  @Test
  public void testSelectChangesWithVersionAndTimestampRanges() throws Exception {
    createChangesTable("persons_cdc");
    String ts0 = new org.joda.time.Instant(100000000000L).toString();
    String ts1 = new org.joda.time.Instant(200000000000L).toString();
    String ts2 = new org.joda.time.Instant(300000000000L).toString();

    assertThat(
        rows(
            "SELECT * FROM CHANGES(persons_cdc, 0) WITH ('include_metadata_columns' ="
                + " ARRAY['_commit_timestamp', '_commit_version', '_change_type'])"),
        containsInAnyOrder(
            "Alice:30:95.5:USA:insert:0:" + ts0,
            "Bob:20:70.0:USA:insert:0:" + ts0,
            "Charlie:40:85.0:Canada:insert:1:" + ts1,
            "Alice:30:95.5:USA:update_preimage:2:" + ts2,
            "Alice:31:98.0:USA:update_postimage:2:" + ts2,
            "Bob:20:70.0:USA:delete:2:" + ts2));

    assertThat(
        rows("SELECT name, age, _change_type, _commit_version FROM CHANGES(persons_cdc, 0, 1)"),
        containsInAnyOrder("Alice:30:insert:0", "Bob:20:insert:0", "Charlie:40:insert:1"));

    assertThat(
        rows(
            "SELECT name, age, _change_type FROM CHANGES(persons_cdc, start_version => 2,"
                + " end_version => 2)"
                + " WHERE _change_type <> 'update_preimage'"),
        containsInAnyOrder("Alice:31:update_postimage", "Bob:20:delete"));

    assertThat(
        rows(
            format(
                "SELECT name, _change_type, _commit_version FROM CHANGES(persons_cdc, '%s', '%s')"
                    + " WITH ('include_metadata_columns' = ARRAY['_change_type',"
                    + " '_commit_version'])",
                ts1, ts1)),
        containsInAnyOrder("Charlie:insert:1"));

    assertThat(
        rows(
            format(
                "SELECT name, age FROM CHANGES(persons_cdc, start_timestamp => TIMESTAMP '%s')",
                ts2.replace("T", " ").replace("Z", ""))),
        containsInAnyOrder("Alice:30", "Alice:31", "Bob:20"));
  }

  @Test
  public void testSelectChangesWithEmptyTimestampRanges() throws Exception {
    createChangesTable("persons_cdc");
    String betweenV0AndV1 = new org.joda.time.Instant(150000000000L).toString();
    String alsoBetweenV0AndV1 = new org.joda.time.Instant(160000000000L).toString();

    // After the latest commit, before the first commit, and between two commits
    assertThat(rows("SELECT * FROM CHANGES(persons_cdc, '2999-01-01')"), empty());
    assertThat(rows("SELECT * FROM CHANGES(persons_cdc, '1970-01-01', '1971-01-01')"), empty());
    assertThat(
        rows(
            format(
                "SELECT * FROM CHANGES(persons_cdc, '%s', '%s')",
                betweenV0AndV1, alsoBetweenV0AndV1)),
        empty());
  }

  @Test
  public void testSelectChangesRangeErrors() throws Exception {
    createChangesTable("persons_cdc");
    String ts1 = new org.joda.time.Instant(200000000000L).toString();
    String ts2 = new org.joda.time.Instant(300000000000L).toString();

    assertQueryFailsWith(
        "SELECT * FROM CHANGES(persons_cdc, 3)", "has no version 3. Its latest version is 2.");
    assertQueryFailsWith(
        "SELECT * FROM CHANGES(persons_cdc, 0, 3)", "has no version 3. Its latest version is 2.");
    assertQueryFailsWith(
        "SELECT * FROM CHANGES(persons_cdc, 2, 1)",
        "CHANGES start version 2 is after its end version 1.");
    assertQueryFailsWith(
        format("SELECT * FROM CHANGES(persons_cdc, '%s', '%s')", ts2, ts1),
        format("CHANGES start timestamp %s is after its end timestamp %s.", ts2, ts1));
  }

  @Test
  public void testSelectChangesReadsTheVersionsResolvedWhenThePipelineIsBuilt() throws Exception {
    File tableDir = createChangesTable("persons_cdc");
    BeamRelNode relNode =
        sqlEnv.parseQuery(
            "SELECT name, _commit_version FROM CHANGES(persons_cdc, 1)"
                + " WITH ('include_metadata_columns' = ARRAY['_commit_version'])");
    PCollection<Row> output = BeamSqlRelUtils.toPCollection(readPipeline, relNode);

    // Version 3, committed after the pipeline was built, is not read.
    DeltaWriteTestUtils.writeAppendCommit(
        DefaultEngine.create(new Configuration()),
        tableDir.getAbsolutePath(),
        3L,
        400000000000L,
        PERSON_DELTA_SCHEMA,
        Collections.singletonList(PERSON_4));

    Schema schema = output.getSchema();
    PAssert.that(output)
        .containsInAnyOrder(
            Row.withSchema(schema).addValues("Charlie", 1L).build(),
            Row.withSchema(schema).addValues("Alice", 2L).build(),
            Row.withSchema(schema).addValues("Alice", 2L).build(),
            Row.withSchema(schema).addValues("Bob", 2L).build());
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testSelectChangesValidationErrors() throws Exception {
    File tableDir = tempFolder.newFolder("delta-table-cdc-validation");
    createDeltaTable(tableDir, Collections.singletonList(PERSON_1));

    sqlEnv.executeDdl(
        format(
            "CREATE EXTERNAL TABLE persons (\n"
                + "  name VARCHAR,\n"
                + "  age INTEGER,\n"
                + "  score DOUBLE,\n"
                + "  country VARCHAR\n"
                + ")\n"
                + "TYPE 'delta'\n"
                + "LOCATION '%s'",
            tableDir.getAbsolutePath()));
    sqlEnv.executeDdl(
        format(
            "CREATE EXTERNAL TABLE persons_v0 (\n"
                + "  name VARCHAR,\n"
                + "  age INTEGER,\n"
                + "  score DOUBLE,\n"
                + "  country VARCHAR\n"
                + ")\n"
                + "TYPE 'delta'\n"
                + "LOCATION '%s'\n"
                + "TBLPROPERTIES '{\"version\": 0}'",
            tableDir.getAbsolutePath()));

    assertParseFailsWith(
        "SELECT * FROM CHANGES(persons)",
        "needs a start: a version, as in CHANGES(t, 2), or a timestamp");
    assertParseFailsWith(
        "SELECT * FROM persons WITH ('include_metadata_columns' = ARRAY['_change_type'])",
        "WITH options on a Delta Lake read are only accepted with CHANGES(...)");
    assertParseFailsWith(
        "SELECT * FROM CHANGES(persons_v0, 0)",
        "pins the version it reads with the 'version' or 'timestamp' table property");
    assertParseFailsWith(
        "SELECT * FROM CHANGES(persons, 0) WITH ('bogus' = 1)",
        "Unknown option 'bogus' for a CHANGES read. Accepted options: [include_metadata_columns]");
    assertParseFailsWith(
        "SELECT * FROM CHANGES(persons, 0) WITH ('start_version' = 0)",
        "Option 'start_version' cannot be set in WITH: CHANGES' arguments set the range");
    assertParseFailsWith(
        "SELECT * FROM CHANGES(persons, 0) WITH ('include_metadata_columns' = ARRAY['_bogus'])",
        "Unknown metadata column '_bogus'. Supported metadata columns: [_change_type,"
            + " _commit_version, _commit_timestamp]");
  }

  private List<String> rows(String query) {
    return BeamEnumerableConverter.toRowList(sqlEnv.parseQuery(query)).stream()
        .map(Row::getValues)
        .map(values -> values.stream().map(String::valueOf).collect(joining(":")))
        .collect(toList());
  }

  private void assertParseFailsWith(String sql, String expectedMessage) {
    assertFailsWith(() -> sqlEnv.parseQuery(sql), expectedMessage);
  }

  private void assertQueryFailsWith(String sql, String expectedMessage) {
    assertFailsWith(() -> rows(sql), expectedMessage);
  }

  private static void assertFailsWith(ThrowingRunnable runnable, String expectedMessage) {
    RuntimeException exception = assertThrows(RuntimeException.class, runnable);
    String messages =
        Throwables.getCausalChain(exception).stream()
            .map(Throwable::getMessage)
            .collect(joining("\n"));
    assertThat(messages, containsString(expectedMessage));
  }
}
