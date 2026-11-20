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
package org.apache.beam.sdk.extensions.sql.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.ChangeRange;
import org.apache.beam.sdk.extensions.sql.meta.SchemaBaseBeamTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.catalog.InMemoryCatalogManager;
import org.apache.beam.sdk.extensions.sql.meta.provider.InMemoryMetaTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableProvider;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/**
 * Resolves table references with {@code CHANGES(...)} and {@code WITH} through {@link BeamSqlEnv}
 * and through a JDBC connection.
 */
@RunWith(Parameterized.class)
public class TableClauseResolutionTest {
  private static final List<List<Object>> QUERY_ROWS = new CopyOnWriteArrayList<>();
  private static final List<KV<String, List<Object>>> WRITES = new CopyOnWriteArrayList<>();
  private static final List<String> METADATA_COLUMNS = Arrays.asList("_op", "_version");

  @Parameters(name = "{0}")
  public static List<String> sessions() {
    return Arrays.asList("BeamSqlEnv", "JDBC");
  }

  @Parameter public String sessionType;

  private InMemoryCatalogManager catalogManager;
  private Session session;

  @Before
  public void setUp() throws Exception {
    QUERY_ROWS.clear();
    WRITES.clear();
    catalogManager = new InMemoryCatalogManager();
    catalogManager.registerTableProvider(new ClauseTableProvider());
    catalogManager.registerTableProvider(new TestTableProvider());
    session = newSession();
    session.execute("CREATE EXTERNAL TABLE orders (id BIGINT NOT NULL, name VARCHAR) TYPE clause");
    session.execute("CREATE EXTERNAL TABLE plain (id BIGINT, name VARCHAR) TYPE test");
  }

  @Test
  public void eachReferenceReadsWithItsOwnChangesAndOptions() throws Exception {
    assertEquals(
        Collections.singletonList(
            Arrays.asList(
                read(ImmutableMap.of("tag", "a")), read(ImmutableMap.of("tag", "b")), "one")),
        session.query(
            "SELECT x.clause, y.clause, z.name"
                + " FROM orders WITH ('tag' = 'a') AS x"
                + " JOIN orders WITH ('tag' = 'b') y ON x.id = y.id"
                + " JOIN orders AS z ON y.id = z.id"));
    assertEquals(
        Collections.singletonList(
            Arrays.asList(
                changes(ChangeRange.versions(3L, null), ImmutableMap.of("tag", "a")),
                changes(ChangeRange.all(), ImmutableMap.of()),
                changes(ChangeRange.versions(1L, 2L), ImmutableMap.of()),
                changes(
                    ChangeRange.timestamps(
                        Instant.parse("2026-01-01T00:00:00Z"),
                        Instant.parse("2026-01-02T10:00:00.500Z")),
                    ImmutableMap.of()),
                changes(
                    ChangeRange.timestamps(null, Instant.parse("2026-01-02T00:00:00Z")),
                    ImmutableMap.of()))),
        session.query(
            "SELECT x.clause, c.clause, orders.clause, t.clause, e.clause"
                + " FROM CHANGES(orders, 3) WITH ('tag' = 'a') AS x"
                + " JOIN CHANGES(orders) AS c ON x.id = c.id"
                + " JOIN CHANGES(orders, 1, 2) ON c.id = orders.id"
                + " JOIN CHANGES(orders, '2026-01-01', TIMESTAMP '2026-01-02 10:00:00.5') t"
                + " ON c.id = t.id"
                + " JOIN CHANGES(orders, end_timestamp => '2026-01-02') e ON c.id = e.id"));

    session.execute("CREATE CATALOG cat TYPE 'local'");
    session.execute("USE CATALOG cat");
    session.execute("CREATE DATABASE db");
    session.execute("USE DATABASE db");
    session.execute("CREATE EXTERNAL TABLE orders (id BIGINT NOT NULL, name VARCHAR) TYPE clause");
    assertEquals(
        Collections.singletonList(
            Arrays.asList(
                read(ImmutableMap.of("tag", "cat.db")),
                read(ImmutableMap.of("tag", "db")),
                read(ImmutableMap.of("tag", "none")))),
        session.query(
            "SELECT a.clause, b.clause, orders.clause"
                + " FROM cat.db.orders WITH ('tag' = 'cat.db') a"
                + " JOIN db.orders WITH ('tag' = 'db') b ON a.id = b.id"
                + " JOIN orders WITH ('tag' = 'none') ON a.id = orders.id"));
    assertEquals(
        Collections.singletonList(
            Arrays.asList(
                changes(ChangeRange.versions(null, 7L), ImmutableMap.of("tag", "cat.db")),
                changes(ChangeRange.all(), ImmutableMap.of("tag", "db")))),
        session.query(
            "SELECT a.clause, b.clause"
                + " FROM CHANGES(cat.db.orders, NULL, 7) WITH ('tag' = 'cat.db') a"
                + " JOIN CHANGES(db.orders) WITH ('tag' = 'db') b ON a.id = b.id"));
    // The first statement of a new session on the same catalog manager.
    assertEquals(
        Collections.singletonList(
            Collections.singletonList(changes(ChangeRange.versions(1L, null), ImmutableMap.of()))),
        newSession().query("SELECT clause FROM CHANGES(orders, 1)"));
  }

  @Test
  public void insertTargetWritesThroughForWrite() throws Exception {
    session.execute("INSERT INTO orders WITH ('tag' = 'w') SELECT id, name, 'note' FROM orders");

    assertEquals(
        Collections.singletonList(
            KV.of(write(ImmutableMap.of("tag", "w")), Arrays.asList(1L, "one", "note"))),
        WRITES);
    assertError(
        "INSERT INTO orders SELECT id, name, 'note' FROM orders",
        "Number of INSERT target columns (2) does not equal number of source items (3)");
  }

  @Test
  public void unsupportedClausesFailWithTheClauseText() {
    assertError(
        "SELECT * FROM CHANGES(plain, 1)",
        "Cannot resolve CHANGES(`plain`, start_version => 1):"
            + " InMemoryTable does not support CHANGES(...)");
    assertError(
        "SELECT * FROM plain WITH ('tag' = 'a')",
        "Cannot resolve `plain` WITH ('tag' = 'a'):"
            + " InMemoryTable does not support WITH options for reads");
    assertError(
        "INSERT INTO plain WITH ('tag' = 'a') SELECT * FROM plain",
        "Cannot resolve INTO `plain` WITH ('tag' = 'a'):"
            + " InMemoryTable does not support WITH options for writes");
  }

  /** {@link MetadataColumnResolverTest} covers how names bind. */
  @Test
  public void changesReadsTheMetadataColumnsThatTheQueryNames() throws Exception {
    assertEquals(
        Collections.singletonList(
            Arrays.asList(
                1L, "one", changes(ChangeRange.all(), ImmutableMap.of(), METADATA_COLUMNS), 7L)),
        session.query("SELECT *, c._version FROM CHANGES(orders) AS c WHERE _op = 'INSERT'"));
  }

  @Test
  public void unknownColumnsOfChangesFailWithItsMetadataColumns() {
    assertError(
        "SELECT _opp FROM CHANGES(orders)",
        "Column '_opp' not found in any table."
            + " CHANGES(orders) also has these metadata columns: _op, _version");
    assertError(
        "SELECT c._opp FROM CHANGES(orders) AS c",
        "Column '_opp' not found in table 'c'."
            + " CHANGES(orders) AS c also has these metadata columns: _op, _version");
  }

  private void assertError(String sql, String message) {
    Exception e = assertThrows(sql, Exception.class, () -> session.execute(sql));
    assertThat(
        sql,
        Throwables.getCausalChain(e).stream()
            .map(Throwable::getMessage)
            .collect(Collectors.joining("\n")),
        containsString(message));
  }

  private Session newSession() {
    return sessionType.equals("JDBC")
        ? new JdbcSession(JdbcDriver.connect(catalogManager, PipelineOptionsFactory.create()))
        : new EnvSession(
            BeamSqlEnv.builder(catalogManager)
                .setPipelineOptions(PipelineOptionsFactory.create())
                .build());
  }

  private static String read(Map<String, Object> options) {
    return "read " + options;
  }

  private static String changes(ChangeRange changes, Map<String, Object> options) {
    return "changes " + changes + " " + options;
  }

  private static String changes(
      ChangeRange changes, Map<String, Object> options, List<String> metadataColumns) {
    return changes(changes, options) + (metadataColumns.isEmpty() ? "" : " " + metadataColumns);
  }

  private static String write(Map<String, Object> options) {
    return "write " + options;
  }

  /** Runs SQL through one of the two execution paths. */
  private interface Session {
    void execute(String sql) throws Exception;

    List<List<Object>> query(String sql) throws Exception;
  }

  private static class EnvSession implements Session {
    private final BeamSqlEnv env;

    EnvSession(BeamSqlEnv env) {
      this.env = env;
    }

    @Override
    public void execute(String sql) throws ParseException {
      if (env.isDdl(sql)) {
        env.executeDdl(sql);
      } else {
        run(sql);
      }
    }

    @Override
    public List<List<Object>> query(String sql) throws ParseException {
      QUERY_ROWS.clear();
      run(sql);
      return new ArrayList<>(QUERY_ROWS);
    }

    /** Runs a query or an INSERT, collecting its output rows. */
    private void run(String sql) throws ParseException {
      Pipeline pipeline = Pipeline.create();
      BeamSqlRelUtils.toPCollection(pipeline, env.parseQuery(sql))
          .apply(ParDo.of(new CollectRows()));
      pipeline.run().waitUntilFinish();
    }
  }

  private static class JdbcSession implements Session {
    private final Connection connection;

    JdbcSession(Connection connection) {
      this.connection = connection;
    }

    @Override
    public void execute(String sql) throws Exception {
      try (Statement statement = connection.createStatement()) {
        statement.execute(sql);
      }
    }

    @Override
    public List<List<Object>> query(String sql) throws Exception {
      try (Statement statement = connection.createStatement();
          ResultSet result = statement.executeQuery(sql)) {
        List<List<Object>> rows = new ArrayList<>();
        while (result.next()) {
          List<Object> row = new ArrayList<>();
          for (int i = 1; i <= result.getMetaData().getColumnCount(); i++) {
            row.add(result.getObject(i));
          }
          rows.add(row);
        }
        return rows;
      }
    }
  }

  private static class CollectRows extends DoFn<Row, Void> {
    @ProcessElement
    public void processElement(@Element Row row) {
      QUERY_ROWS.add(row.getValues());
    }
  }

  /** Provides tables of type {@code clause}, which support CHANGES(...) and WITH. */
  private static class ClauseTableProvider extends InMemoryMetaTableProvider {
    @Override
    public String getTableType() {
      return "clause";
    }

    @Override
    public BeamSqlTable buildBeamSqlTable(Table table) {
      return new ClauseTable(table.getSchema(), null);
    }
  }

  /**
   * A table with the single row {@code (1, 'one')}. CHANGES(...) and WITH add a {@code clause}
   * column, which reads fill with a description of them and the hook that took them, and writes
   * record next to each row. CHANGES(...) can also read the metadata columns {@code _op}, which is
   * always 'INSERT', and {@code _version}, which is always 7.
   */
  private static class ClauseTable extends SchemaBaseBeamTable {
    private final @Nullable String clause;
    private final List<String> metadataColumns;

    ClauseTable(Schema schema, @Nullable String clause) {
      this(schema, clause, Collections.emptyList());
    }

    ClauseTable(Schema schema, @Nullable String clause, List<String> metadataColumns) {
      super(schema);
      this.clause = clause;
      this.metadataColumns = metadataColumns;
    }

    @Override
    public BeamSqlTable forRead(Map<String, Object> options) {
      return withClause(read(options));
    }

    @Override
    public BeamSqlTable forWrite(Map<String, Object> options) {
      return withClause(write(options));
    }

    @Override
    public List<String> getChangeMetadataColumns() {
      return METADATA_COLUMNS;
    }

    @Override
    public BeamSqlTable forChanges(
        ChangeRange changes, Map<String, Object> options, List<String> metadataColumns) {
      Schema.Builder withColumns =
          Schema.builder().addFields(schema.getFields()).addStringField("clause");
      for (String column : metadataColumns) {
        withColumns.addField(
            column, column.equals("_op") ? Schema.FieldType.STRING : Schema.FieldType.INT64);
      }
      return new ClauseTable(
          withColumns.build(), changes(changes, options, metadataColumns), metadataColumns);
    }

    private ClauseTable withClause(String clause) {
      return new ClauseTable(
          Schema.builder().addFields(schema.getFields()).addStringField("clause").build(), clause);
    }

    @Override
    public PCollection<Row> buildIOReader(PBegin begin) {
      Row.Builder row = Row.withSchema(schema).addValues(1L, "one");
      if (clause != null) {
        row.addValue(clause);
      }
      for (String column : metadataColumns) {
        row.addValue(column.equals("_op") ? "INSERT" : 7L);
      }
      return begin.apply(Create.of(row.build()).withRowSchema(schema));
    }

    @Override
    public POutput buildIOWriter(PCollection<Row> input) {
      input.apply(ParDo.of(new RecordWrites(clause)));
      return PDone.in(input.getPipeline());
    }

    @Override
    public PCollection.IsBounded isBounded() {
      return PCollection.IsBounded.BOUNDED;
    }
  }

  private static class RecordWrites extends DoFn<Row, Void> {
    private final @Nullable String clause;

    RecordWrites(@Nullable String clause) {
      this.clause = clause;
    }

    @ProcessElement
    public void processElement(@Element Row row) {
      WRITES.add(KV.of(clause, row.getValues()));
    }
  }
}
