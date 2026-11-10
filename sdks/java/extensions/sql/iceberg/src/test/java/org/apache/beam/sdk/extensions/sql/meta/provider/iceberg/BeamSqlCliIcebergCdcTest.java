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
package org.apache.beam.sdk.extensions.sql.meta.provider.iceberg;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineVisitor;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.JdbcDriver;
import org.apache.beam.sdk.extensions.sql.impl.planner.BeamRuleSets;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamEnumerableConverter;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamPushDownIOSourceRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.extensions.sql.impl.rule.BeamIOPushDownRule;
import org.apache.beam.sdk.extensions.sql.meta.catalog.InMemoryCatalogManager;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.TransformHierarchy;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.rel.RelNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.tools.RuleSet;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.tools.RuleSets;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Reads Iceberg tables with {@code CHANGES(...)} and writes them with {@code INSERT INTO ... WITH}.
 */
public class BeamSqlCliIcebergCdcTest {
  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private static final String CHANGE_TYPE = "'include_metadata_columns' = ARRAY['_change_type']";
  private static final String APPLY_CHANGES =
      "INSERT INTO orders WITH ('mode' = 'merge-on-read', 'change_type_column' = 'op',"
          + " 'sequence_number_column' = 'seq') VALUES ";

  /** The first commit of {@link #seedOrders()}, as id:name:amount:change type. */
  private static final List<String> FIRST_COMMIT =
      ImmutableList.of("1:a:5:INSERT", "2:b:20:INSERT", "3:c:30:INSERT", "4:d:40:INSERT");

  /** The second commit of {@link #seedOrders()}. */
  private static final List<String> SECOND_COMMIT =
      ImmutableList.of(
          "1:a:5:UPDATE_BEFORE",
          "1:a:15:UPDATE_AFTER",
          "2:b:20:UPDATE_BEFORE",
          "2:bb:20:UPDATE_AFTER",
          "3:c:30:DELETE",
          "4:d:40:UPDATE_BEFORE",
          "4:d:8:UPDATE_AFTER");

  private static final List<String> ALL_CHANGES =
      ImmutableList.<String>builder().addAll(FIRST_COMMIT).addAll(SECOND_COMMIT).build();

  private InMemoryCatalogManager catalogManager;
  private BeamSqlEnv sqlEnv;
  private BeamSqlEnv noPushDownEnv;
  private String warehouse;
  private long firstVersion;
  private long secondVersion;

  @Before
  public void setup() throws IOException {
    File warehouseFile = TEMPORARY_FOLDER.newFolder();
    assertTrue(warehouseFile.delete());
    warehouse = "file:" + warehouseFile + "/" + UUID.randomUUID();
    catalogManager = new InMemoryCatalogManager();
    sqlEnv =
        BeamSqlEnv.builder(catalogManager)
            .setPipelineOptions(PipelineOptionsFactory.create())
            .build();
    noPushDownEnv =
        BeamSqlEnv.builder(catalogManager)
            .setPipelineOptions(PipelineOptionsFactory.create())
            .setRuleSets(rulesWithoutPushDown())
            .build();
    execute(createCatalog("cdc_catalog"));
    execute("USE CATALOG cdc_catalog");
    execute("CREATE DATABASE db");
    execute("USE DATABASE db");
    noPushDownEnv.executeDdl("USE CATALOG cdc_catalog");
    noPushDownEnv.executeDdl("USE DATABASE db");
  }

  @Test
  public void changesReadsTheRequestedRange() {
    seedOrders();
    String first = commitTime(firstVersion);
    String second = commitTime(secondVersion);
    String firstWithOffset =
        Instant.ofEpochMilli(icebergTable("orders").snapshot(firstVersion).timestampMillis())
            .atOffset(ZoneOffset.of("-03:30"))
            .toString();
    Map<String, List<String>> ranges =
        ImmutableMap.<String, List<String>>builder()
            .put("CHANGES(orders)", ALL_CHANGES)
            .put(format("CHANGES(orders, %d)", firstVersion), ALL_CHANGES)
            .put(format("CHANGES(orders, %d, %d)", firstVersion, firstVersion), FIRST_COMMIT)
            .put(format("CHANGES(orders, NULL, %d)", firstVersion), FIRST_COMMIT)
            .put(
                format(
                    "CHANGES(orders, start_version => %d, end_version => %d)",
                    secondVersion, secondVersion),
                SECOND_COMMIT)
            .put(format("CHANGES(cdc_catalog.orders, '%s', '%s')", first, first), FIRST_COMMIT)
            .put(
                format("CHANGES(orders, '%s', '%s')", firstWithOffset, firstWithOffset),
                FIRST_COMMIT)
            .put(format("CHANGES(orders, TIMESTAMP '%s', NULL)", second), SECOND_COMMIT)
            .put(format("CHANGES(orders, '%s')", first.substring(0, 10)), ALL_CHANGES)
            .put(
                format(
                    "CHANGES(orders, start_timestamp => TIMESTAMP '%s', end_timestamp => '%s')",
                    second, second),
                SECOND_COMMIT)
            .put(format("CHANGES(orders, end_timestamp => '%s')", first), FIRST_COMMIT)
            .put("CHANGES(orders, '2999-01-01')", ImmutableList.of())
            .put("CHANGES(orders, NULL, '2000-01-01')", ImmutableList.of())
            .build();

    assertThat(rows("SELECT * FROM orders"), containsInAnyOrder("1:a:15", "2:bb:20", "4:d:8"));
    assertThat(
        rows("SELECT c.id, c.name, c.amount FROM CHANGES(orders) AS c"),
        containsInAnyOrder(withoutChangeTypes(ALL_CHANGES)));
    ranges.forEach(
        (changes, expected) ->
            assertThat(
                changes,
                rows(format("SELECT * FROM %s WITH (%s)", changes, CHANGE_TYPE)),
                containsInAnyOrder(expected.toArray())));
    assertThat(
        rows(
            format(
                "SELECT * FROM CHANGES(orders, end_version => %d)"
                    + " WITH ('starting_strategy' = 'latest', %s)",
                secondVersion, CHANGE_TYPE)),
        containsInAnyOrder(SECOND_COMMIT.toArray()));
    RuntimeException reversed =
        assertThrows(
            RuntimeException.class,
            () -> rows(format("SELECT * FROM CHANGES(orders, '%s', '%s')", second, first)));
    assertThat(
        Throwables.getStackTraceAsString(reversed),
        containsString("'from_timestamp' must not be later than 'to_timestamp'"));
  }

  @Test
  public void pushedAndUnpushedPlansAgree() {
    seedOrders();

    String canonicalOrder =
        format(
            "SELECT * FROM CHANGES(orders, %d) WITH ('include_metadata_columns' ="
                + " ARRAY['_commit_snapshot_sequence_number', '_commit_snapshot_id',"
                + " '_change_type']) WHERE id = 3",
            secondVersion);

    assertEquals(
        ImmutableList.of(
            "id",
            "name",
            "amount",
            "_change_type",
            "_commit_snapshot_id",
            "_commit_snapshot_sequence_number"),
        sqlEnv.parseQuery(canonicalOrder).getRowType().getFieldNames());
    assertSamePlans(canonicalOrder, "3:c:30:DELETE:" + secondVersion + ":2");

    assertSamePlans(
        changesQuery("name, amount, _change_type", ""),
        "a:5:UPDATE_BEFORE",
        "a:15:UPDATE_AFTER",
        "b:20:UPDATE_BEFORE",
        "bb:20:UPDATE_AFTER",
        "c:30:DELETE",
        "d:40:UPDATE_BEFORE",
        "d:8:UPDATE_AFTER");
    assertSamePlans(
        changesQuery("amount, name, _change_type", ""),
        "5:a:UPDATE_BEFORE",
        "15:a:UPDATE_AFTER",
        "20:b:UPDATE_BEFORE",
        "20:bb:UPDATE_AFTER",
        "30:c:DELETE",
        "40:d:UPDATE_BEFORE",
        "8:d:UPDATE_AFTER");

    // Order 2 changed only its name. The read pairs update images on whole rows, so the update
    // stays even though the projected images are equal.
    assertSamePlans(
        changesQuery("id, amount, _change_type", ""),
        "1:5:UPDATE_BEFORE",
        "1:15:UPDATE_AFTER",
        "2:20:UPDATE_BEFORE",
        "2:20:UPDATE_AFTER",
        "3:30:DELETE",
        "4:40:UPDATE_BEFORE",
        "4:8:UPDATE_AFTER");

    String metadataOnly = canonicalOrder.replace("SELECT *", "SELECT _change_type");

    assertEquals(ImmutableList.of("id", "_change_type"), readColumns(metadataOnly));
    assertSamePlans(metadataOnly, "DELETE");

    String keyFilter = changesQuery("id, amount, _change_type", "WHERE id >= 3");

    assertThat(sqlEnv.explain(keyFilter), containsString("supported=>=($0, 3), unsupported=}"));
    assertSamePlans(keyFilter, "3:30:DELETE", "4:40:UPDATE_BEFORE", "4:8:UPDATE_AFTER");

    String metadataFilter = changesQuery("id, name, _change_type", "WHERE _change_type = 'DELETE'");

    assertThat(sqlEnv.explain(metadataFilter), containsString("IcebergFilter{supported=, "));
    assertSamePlans(metadataFilter, "3:c:DELETE");
  }

  @Test
  public void pushedFilterTurnsCrossingUpdatesIntoInsertsAndDeletes() {
    seedOrders();
    String query = changesQuery("id, name, amount, _change_type", "WHERE amount > 10");

    // Order 1 rose from 5 to 15 and order 4 fell from 40 to 8.
    assertThat(
        pushedRows(query),
        containsInAnyOrder(
            "1:a:15:INSERT",
            "2:b:20:UPDATE_BEFORE",
            "2:bb:20:UPDATE_AFTER",
            "3:c:30:DELETE",
            "4:d:40:DELETE"));
    assertThat(
        rows(noPushDownEnv, query),
        containsInAnyOrder(
            "1:a:15:UPDATE_AFTER",
            "2:b:20:UPDATE_BEFORE",
            "2:bb:20:UPDATE_AFTER",
            "3:c:30:DELETE",
            "4:d:40:UPDATE_BEFORE"));
  }

  @Test
  public void temporalPredicatesArePushedIntoChanges() {
    execute(
        "CREATE EXTERNAL TABLE events (id BIGINT, ts TIMESTAMP, dt DATE, tm TIME,"
            + " PRIMARY KEY (id) NOT ENFORCED) TYPE 'iceberg'");
    execute(
        "INSERT INTO events VALUES"
            + " (1, TIMESTAMP '2026-01-01 10:00:00', DATE '2026-01-01', TIME '10:00:00'),"
            + " (2, TIMESTAMP '2026-01-02 10:00:00', DATE '2026-01-02', TIME '11:00:00')");

    for (String predicate :
        ImmutableList.of(
            "ts > TIMESTAMP '2026-01-01 12:00:00'",
            "dt > DATE '2026-01-01'",
            "tm > TIME '10:30:00'")) {
      String query = "SELECT id FROM CHANGES(events) WHERE " + predicate;
      assertThat(sqlEnv.explain(query), containsString("IcebergFilter{supported=>("));
      assertSamePlans(query, "2");
    }
  }

  @Test
  public void streamingChangesAreUnbounded() throws SQLException {
    seedOrders();
    String query =
        "SELECT * FROM CHANGES(orders) WITH ('streaming' = true, 'starting_strategy' ="
            + " 'earliest', 'poll_interval_seconds' = 1, "
            + CHANGE_TYPE
            + ") LIMIT "
            + ALL_CHANGES.size();

    assertEquals(PCollection.IsBounded.UNBOUNDED, sqlEnv.parseQuery(query).isBounded());
    List<String> rows = new ArrayList<>();
    try (Connection connection =
            JdbcDriver.connect(catalogManager, PipelineOptionsFactory.create());
        ResultSet result = connection.createStatement().executeQuery(query)) {
      while (result.next()) {
        rows.add(
            format(
                "%s:%s:%s:%s",
                result.getObject(1),
                result.getObject(2),
                result.getObject(3),
                result.getObject(4)));
      }
    }
    assertThat(rows, containsInAnyOrder(ALL_CHANGES.toArray()));

    execute(
        "CREATE EXTERNAL TABLE orders_mirror (id BIGINT NOT NULL, name VARCHAR, amount INTEGER)"
            + " TYPE 'iceberg'");
    execute(
        "CREATE EXTERNAL TABLE timed_mirror (id BIGINT NOT NULL, name VARCHAR, amount INTEGER)"
            + " TYPE 'iceberg' TBLPROPERTIES '{ \"beam.write.triggering_frequency_seconds\" : 30 }'");
    String mirror =
        "INSERT INTO cdc_catalog.db.%s WITH ('mode' = 'merge-on-read',"
            + " 'equality_columns' = ARRAY['id']%s)"
            + " SELECT * FROM CHANGES(cdc_catalog.db.orders, "
            + firstVersion
            + ") WITH ('include_metadata_columns' = ARRAY['_commit_snapshot_sequence_number'],"
            + " 'streaming' = true)";

    BeamRelNode streaming =
        sqlEnv.parseQuery(format(mirror, "orders_mirror", ", 'triggering_frequency_seconds' = 30"));
    assertEquals(PCollection.IsBounded.UNBOUNDED, streaming.isBounded());
    BeamSqlRelUtils.toPCollection(Pipeline.create(), streaming);
    BeamSqlRelUtils.toPCollection(
        Pipeline.create(), sqlEnv.parseQuery(format(mirror, "timed_mirror", "")));

    BeamRelNode withoutFrequency = sqlEnv.parseQuery(format(mirror, "orders_mirror", ""));
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> BeamSqlRelUtils.toPCollection(Pipeline.create(), withoutFrequency));
    assertThat(
        e.getMessage(),
        containsString(
            "A streaming INSERT into Iceberg table 'db.orders_mirror' needs"
                + " 'triggering_frequency_seconds'"));
  }

  @Test
  public void eachReferenceReadsItsOwnChanges() {
    seedOrders();
    assertEquals(
        ImmutableList.of("3:DELETE:" + firstVersion),
        rows(
            format(
                "SELECT c.id, c._change_type, s._commit_snapshot_id"
                    + " FROM CHANGES(orders, %d) WITH (%s) AS c"
                    + " JOIN CHANGES(orders, %d, %d)"
                    + " WITH ('include_metadata_columns' = ARRAY['_commit_snapshot_id']) AS s"
                    + " ON c.id = s.id WHERE c._change_type = 'DELETE'",
                secondVersion, CHANGE_TYPE, firstVersion, firstVersion)));
  }

  @Test
  public void mirrorsApplyTheChanges() {
    seedOrders();
    execute(
        "CREATE EXTERNAL TABLE orders_mirror (id BIGINT NOT NULL, name VARCHAR, amount INTEGER)"
            + " TYPE 'iceberg'");

    execute(
        format(
            "INSERT INTO cdc_catalog.db.orders_mirror WITH ('mode' = 'merge-on-read',"
                + " 'equality_columns' = ARRAY['id'], 'triggering_frequency_seconds' = 30)"
                + " SELECT * FROM CHANGES(cdc_catalog.db.orders, start_version => %d)"
                + " WITH ('include_metadata_columns' = ARRAY['_commit_snapshot_sequence_number'])",
            firstVersion));

    assertThat(
        rows("SELECT * FROM orders_mirror"), containsInAnyOrder("1:a:15", "2:bb:20", "4:d:8"));

    execute(
        "CREATE EXTERNAL TABLE orders_typed_mirror (id BIGINT, name VARCHAR, amount INTEGER,"
            + " PRIMARY KEY (id) NOT ENFORCED) TYPE 'iceberg'");

    execute(
        "INSERT INTO orders_typed_mirror WITH ('mode' = 'MERGE-ON-READ', 'change_type_column' ="
            + " '_change_type') SELECT * FROM CHANGES(orders) WITH ('include_metadata_columns' ="
            + " ARRAY['_change_type', '_commit_snapshot_sequence_number'])");

    assertThat(
        rows("SELECT * FROM orders_typed_mirror"),
        containsInAnyOrder("1:a:15", "2:bb:20", "4:d:8"));
  }

  @Test
  public void invalidChangesReadsFailWithClearErrors() {
    seedOrders();
    execute("CREATE EXTERNAL TABLE no_key (id BIGINT, name VARCHAR) TYPE 'iceberg'");

    assertFailsWith(
        "SELECT * FROM CHANGES(orders) WITH ('bogus' = 1)",
        "Unknown option 'bogus' for a CHANGES read. Accepted options: [include_metadata_columns,"
            + " poll_interval_seconds, starting_strategy, streaming, watermark_column,"
            + " watermark_column_time_unit]");
    assertFailsWith(
        "SELECT * FROM CHANGES(orders) WITH ('mode' = 'merge-on-read')",
        "Unknown option 'mode' for a CHANGES read");
    assertFailsWith(
        "SELECT * FROM CHANGES(orders) WITH ('from_snapshot' = 1)",
        "Option 'from_snapshot' cannot be set in WITH: CHANGES' arguments set the range");
    assertFailsWith(
        "SELECT * FROM CHANGES(orders) WITH ('filter' = 'id > 1')",
        "Option 'filter' cannot be set in WITH: WHERE filters the rows");
    assertFailsWith(
        "SELECT * FROM CHANGES(orders, 1) WITH ('starting_strategy' = 'latest')",
        "Option 'starting_strategy' cannot be set in WITH: CHANGES' start bound sets the start");
    assertFailsWith(
        "SELECT * FROM CHANGES(orders, start_timestamp => '2026-01-01')"
            + " WITH ('starting_strategy' = 'latest')",
        "Option 'starting_strategy' cannot be set in WITH: CHANGES' start bound sets the start");
    assertFailsWith(
        "SELECT * FROM CHANGES(orders) WITH ('streaming' = 'yes')",
        "Option 'streaming' must be of type BOOLEAN");
    assertFailsWith(
        "SELECT * FROM CHANGES(orders) WITH ('include_metadata_columns' = ARRAY['_bogus'])",
        "Unknown metadata column '_bogus'");
    assertFailsWith(
        "SELECT * FROM CHANGES(no_key) AS c",
        "CHANGES needs a primary key, and Iceberg table 'db.no_key' has none. Declare one with"
            + " PRIMARY KEY (...) NOT ENFORCED when creating the table, or set the table's"
            + " identifier fields in the Iceberg catalog.");
  }

  /**
   * Two commits. The first inserts orders 1 to 4. The second raises order 1's amount from 5 to 15,
   * renames order 2, deletes order 3 and lowers order 4's amount from 40 to 8.
   */
  private void seedOrders() {
    execute(
        "CREATE EXTERNAL TABLE orders (id BIGINT, name VARCHAR, amount INTEGER,"
            + " PRIMARY KEY (id) NOT ENFORCED) TYPE 'iceberg'");
    execute(
        APPLY_CHANGES
            + "(1, 'a', 5, 'INSERT', 1), (2, 'b', 20, 'INSERT', 1), (3, 'c', 30, 'INSERT', 1),"
            + " (4, 'd', 40, 'INSERT', 1)");
    firstVersion = icebergTable("orders").currentSnapshot().snapshotId();
    execute(
        APPLY_CHANGES
            + "(1, 'a', 5, 'UPDATE_BEFORE', 2), (1, 'a', 15, 'UPDATE_AFTER', 2),"
            + " (2, 'b', 20, 'UPDATE_BEFORE', 2), (2, 'bb', 20, 'UPDATE_AFTER', 2),"
            + " (3, 'c', 30, 'DELETE', 2),"
            + " (4, 'd', 40, 'UPDATE_BEFORE', 2), (4, 'd', 8, 'UPDATE_AFTER', 2)");
    secondVersion = icebergTable("orders").currentSnapshot().snapshotId();
    assertNotEquals(firstVersion, secondVersion);
  }

  private String changesQuery(String columns, String where) {
    return format(
        "SELECT %s FROM CHANGES(orders, %d) WITH (%s) %s",
        columns, secondVersion, CHANGE_TYPE, where);
  }

  /** Asserts that the query returns the rows with and without push-down. */
  private void assertSamePlans(String query, String... expected) {
    assertThat(pushedRows(query), containsInAnyOrder(expected));
    assertThat(rows(noPushDownEnv, query), containsInAnyOrder(expected));
  }

  private List<String> pushedRows(String query) {
    assertFalse(hasPushDown(noPushDownEnv.parseQuery(query)));
    return pushedRows(sqlEnv, query);
  }

  private static List<String> pushedRows(BeamSqlEnv env, String query) {
    assertTrue(hasPushDown(env.parseQuery(query)));
    return rows(env, query);
  }

  /** The columns that the query's Iceberg read outputs. */
  private List<String> readColumns(String query) {
    Pipeline pipeline = Pipeline.create();
    BeamSqlRelUtils.toPCollection(pipeline, sqlEnv.parseQuery(query));
    List<String> columns = new ArrayList<>();
    pipeline.traverseTopologically(
        new PipelineVisitor.Defaults() {
          @Override
          public void leaveCompositeTransform(TransformHierarchy.Node node) {
            if (node.getFullName().endsWith("Read Iceberg changes")) {
              PCollection<?> read =
                  (PCollection<?>) Iterables.getOnlyElement(node.getOutputs().values());
              columns.addAll(read.getSchema().getFieldNames());
            }
          }
        });
    return columns;
  }

  private static boolean hasPushDown(RelNode node) {
    return node instanceof BeamPushDownIOSourceRel
        || node.getInputs().stream().anyMatch(BeamSqlCliIcebergCdcTest::hasPushDown);
  }

  private static Collection<RuleSet> rulesWithoutPushDown() {
    return BeamRuleSets.getRuleSets().stream()
        .map(
            rules ->
                RuleSets.ofList(
                    StreamSupport.stream(rules.spliterator(), false)
                        .filter(rule -> !(rule instanceof BeamIOPushDownRule))
                        .collect(toList())))
        .collect(toList());
  }

  private static Object[] withoutChangeTypes(List<String> changes) {
    return changes.stream().map(change -> change.substring(0, change.lastIndexOf(':'))).toArray();
  }

  private void execute(String sql) {
    execute(sqlEnv, sql);
  }

  private static void execute(BeamSqlEnv env, String sql) {
    if (env.isDdl(sql)) {
      env.executeDdl(sql);
      return;
    }
    Pipeline pipeline = Pipeline.create();
    BeamSqlRelUtils.toPCollection(pipeline, env.parseQuery(sql));
    pipeline.run().waitUntilFinish();
  }

  private List<String> rows(String query) {
    return rows(sqlEnv, query);
  }

  /** Each row as its values joined by ':'. */
  private static List<String> rows(BeamSqlEnv env, String query) {
    return BeamEnumerableConverter.toRowList(env.parseQuery(query)).stream()
        .map(Row::getValues)
        .map(values -> values.stream().map(String::valueOf).collect(joining(":")))
        .collect(toList());
  }

  private void assertFailsWith(String sql, String message) {
    RuntimeException e = assertThrows(RuntimeException.class, () -> sqlEnv.parseQuery(sql));
    String messages =
        Throwables.getCausalChain(e).stream().map(Throwable::getMessage).collect(joining("\n"));
    assertThat(messages, containsString("Cannot resolve"));
    assertThat(messages, containsString(message));
  }

  private String createCatalog(String name) {
    return format(
        "CREATE CATALOG %s TYPE iceberg PROPERTIES ('type' = 'hadoop', 'warehouse' = '%s')",
        name, warehouse);
  }

  private String commitTime(long version) {
    Snapshot snapshot = icebergTable("orders").snapshot(version);
    return DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
        .withZone(ZoneOffset.UTC)
        .format(Instant.ofEpochMilli(snapshot.timestampMillis()));
  }

  private Table icebergTable(String name) {
    IcebergCatalog catalog = (IcebergCatalog) catalogManager.currentCatalog();
    return catalog.catalogConfig.catalog().loadTable(TableIdentifier.of("db", name));
  }
}
