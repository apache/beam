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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/**
 * Resolves table references with {@code WITH} options through {@link BeamSqlEnv} and through a JDBC
 * connection.
 */
@RunWith(Parameterized.class)
public class TableClauseResolutionTest {
  private static final List<List<Object>> QUERY_ROWS = new CopyOnWriteArrayList<>();
  private static final List<KV<String, List<Object>>> WRITES = new CopyOnWriteArrayList<>();

  @Parameters(name = "{0}")
  public static List<String> sessions() {
    return Arrays.asList("BeamSqlEnv", "JDBC");
  }

  @Parameter public String sessionType;

  private Session session;

  @Before
  public void setUp() throws Exception {
    QUERY_ROWS.clear();
    WRITES.clear();
    InMemoryCatalogManager catalogManager = new InMemoryCatalogManager();
    catalogManager.registerTableProvider(new ClauseTableProvider());
    catalogManager.registerTableProvider(new TestTableProvider());
    session =
        sessionType.equals("JDBC")
            ? new JdbcSession(JdbcDriver.connect(catalogManager, PipelineOptionsFactory.create()))
            : new EnvSession(
                BeamSqlEnv.builder(catalogManager)
                    .setPipelineOptions(PipelineOptionsFactory.create())
                    .build());
    session.execute("CREATE EXTERNAL TABLE orders (id BIGINT NOT NULL, name VARCHAR) TYPE clause");
    session.execute("CREATE EXTERNAL TABLE plain (id BIGINT, name VARCHAR) TYPE test");
  }

  @Test
  public void eachReferenceReadsWithItsOwnOptions() throws Exception {
    assertEquals(
        Collections.singletonList(
            Arrays.asList(
                read(ImmutableMap.of("tag", "a")), read(ImmutableMap.of("tag", "b")), "one")),
        session.query(
            "SELECT x.clause, y.clause, z.name"
                + " FROM orders WITH ('tag' = 'a') AS x"
                + " JOIN orders WITH ('tag' = 'b') y ON x.id = y.id"
                + " JOIN orders AS z ON y.id = z.id"));

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
  public void unsupportedOptionsFailWithTheClauseText() {
    assertError(
        "SELECT * FROM plain WITH ('tag' = 'a')",
        "Cannot resolve `plain` WITH ('tag' = 'a'):"
            + " InMemoryTable does not support WITH options for reads");
    assertError(
        "INSERT INTO plain WITH ('tag' = 'a') SELECT * FROM plain",
        "Cannot resolve INTO `plain` WITH ('tag' = 'a'):"
            + " InMemoryTable does not support WITH options for writes");
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

  private static String read(Map<String, Object> options) {
    return "read " + options;
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

  /** Provides tables of type {@code clause}, which support WITH options. */
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
   * A table with the single row {@code (1, 'one')}. WITH options add a {@code clause} column, which
   * reads fill with a description of them and the hook that took them, and writes record next to
   * each row.
   */
  private static class ClauseTable extends SchemaBaseBeamTable {
    private final @Nullable String clause;

    ClauseTable(Schema schema, @Nullable String clause) {
      super(schema);
      this.clause = clause;
    }

    @Override
    public BeamSqlTable forRead(Map<String, Object> options) {
      return withClause(read(options));
    }

    @Override
    public BeamSqlTable forWrite(Map<String, Object> options) {
      return withClause(write(options));
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
