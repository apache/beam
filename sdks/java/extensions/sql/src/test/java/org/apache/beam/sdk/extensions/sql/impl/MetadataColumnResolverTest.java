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

import static org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.util.Static.RESOURCE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.impl.parser.BeamSqlParser;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.ChangeRange;
import org.apache.beam.sdk.extensions.sql.meta.SchemaBaseBeamTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.InMemoryMetaTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.store.InMemoryMetaStore;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.config.Lex;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.jdbc.CalciteSchema;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlDialect;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlNode;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlSelect;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlUtil;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.parser.SqlParseException;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.parser.SqlParser;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests how {@link MetadataColumnResolver} rewrites statements. Every table here has the columns of
 * its definition, and its changes can also read the metadata columns {@code _op} and {@code
 * _version}.
 */
@RunWith(JUnit4.class)
public class MetadataColumnResolverTest {
  private static final SqlParser.Config CONFIG =
      SqlParser.config().withLex(Lex.JAVA).withParserFactory(BeamSqlParser.FACTORY);
  private static final SqlDialect UNQUOTED = new SqlDialect(SqlDialect.EMPTY_CONTEXT);
  private static final String OP = "CHANGES(`orders`, metadata_columns => ARRAY['_op'])";

  /** Statements and how they look once resolved. */
  private static final String[][] RESOLVED = {
    {
      "SELECT _version FROM CHANGES(orders) WHERE _op = 'x' GROUP BY _version ORDER BY _version",
      "SELECT _version FROM CHANGES(`orders`, metadata_columns => ARRAY['_op', '_version'])"
          + " AS orders WHERE _op = 'x' GROUP BY _version ORDER BY _version"
    },
    // A star leaves out the metadata columns that the statement names.
    {
      "SELECT *, c._op FROM CHANGES(orders) AS c", "SELECT c.id, c.name, c._op FROM " + OP + " AS c"
    },
    {
      "SELECT * FROM CHANGES(orders) AS c, orders AS o WHERE c._op = o.name",
      "SELECT c.id, c.name, o.* FROM " + OP + " AS c, orders AS o WHERE c._op = o.name"
    },
    {
      "SELECT c.*, o.id FROM CHANGES(orders) AS c JOIN orders AS o ON c._op = o.name",
      "SELECT c.id, c.name, o.id FROM " + OP + " AS c INNER JOIN orders AS o ON c._op = o.name"
    },
    // Each read takes the names that bind to it. An unqualified name binds to every read in its
    // SELECT that has it, and the validator reports it as ambiguous.
    {
      "SELECT a._op, b._version FROM CHANGES(orders) AS a, CHANGES(orders, 1) AS b",
      "SELECT a._op, b._version FROM "
          + OP
          + " AS a, CHANGES(`orders`, start_version => 1,"
          + " metadata_columns => ARRAY['_version']) AS b"
    },
    {
      "SELECT _op FROM CHANGES(orders) AS a, CHANGES(orders) AS b",
      "SELECT _op FROM " + OP + " AS a, " + OP + " AS b"
    },
    // A column of the table wins over a metadata column with the same name.
    {
      "SELECT _op, _version FROM CHANGES(ops)",
      "SELECT _op, _version FROM CHANGES(`ops`, metadata_columns => ARRAY['_version']) AS ops"
    },
    // A name binds in its own SELECT first, then in the SELECTs around it.
    {
      "SELECT x._op FROM (SELECT _op FROM CHANGES(orders)) AS x",
      "SELECT x._op FROM (SELECT _op FROM " + OP + " AS orders) AS x"
    },
    {
      "SELECT id FROM CHANGES(orders) AS c WHERE EXISTS (SELECT name FROM orders WHERE _op = name)",
      "SELECT id FROM " + OP + " AS c WHERE EXISTS (SELECT name FROM orders WHERE _op = name)"
    },
    // A name doesn't reach a read in a subquery of FROM, and a read with a column alias list
    // can't take more columns.
    {
      "SELECT _op FROM (SELECT * FROM CHANGES(orders)) AS x",
      "SELECT _op FROM (SELECT * FROM CHANGES(`orders`) AS orders) AS x"
    },
    {
      "SELECT _op FROM CHANGES(orders) AS c (a, b)", "SELECT _op FROM CHANGES(`orders`) AS c (a, b)"
    },
  };

  private JdbcConnection connection;

  @Before
  public void setUp() {
    InMemoryMetaStore metaStore = new InMemoryMetaStore();
    metaStore.registerProvider(new ChangesTableProvider());
    metaStore.createTable(table("orders", "name"));
    metaStore.createTable(table("ops", "_op"));
    connection = JdbcDriver.connect(metaStore, PipelineOptionsFactory.create());
  }

  @Test
  public void namesBindToTheReadsThatHaveThem() throws Exception {
    for (String[] statement : RESOLVED) {
      SqlNode node = parse(statement[0]);
      resolver().resolve(node);
      assertEquals(
          statement[0], statement[1], node.toSqlString(UNQUOTED).getSql().replace('\n', ' '));
    }
  }

  @Test
  public void starsAndNaturalJoinsThatWouldChangeFailClearly() {
    assertResolveFails(
        "SELECT c._op FROM CHANGES(orders) AS c NATURAL JOIN orders",
        "A NATURAL JOIN would join on metadata column '_op' of CHANGES(orders) AS c if the other"
            + " side has a column with that name. Join with USING or ON instead");
    assertResolveFails(
        "SELECT * FROM CHANGES(orders) AS c JOIN orders USING (id) WHERE c._op = 'x'",
        "* cannot leave out metadata column '_op' of CHANGES(orders) AS c, because the FROM clause"
            + " has a JOIN ... USING or an item without an alias. Write c.* or list the columns"
            + " instead");
    assertResolveFails(
        "SELECT * FROM CHANGES(orders) AS c, UNNEST(ARRAY[1]) WHERE c._op = 'x'",
        "* cannot leave out metadata column '_op' of CHANGES(orders) AS c");
  }

  @Test
  public void explainAddsTheMetadataColumnsToUnknownColumnErrors() throws Exception {
    SqlSelect select = (SqlSelect) parse("SELECT _opp, c.idd FROM CHANGES(orders) AS c");
    SqlIdentifier unqualified = (SqlIdentifier) select.getSelectList().get(0);
    SqlIdentifier qualified = (SqlIdentifier) select.getSelectList().get(1);
    MetadataColumnResolver resolver = resolver();
    resolver.resolve(select);

    assertEquals(
        "From line 1, column 8 to line 1, column 11: Column '_opp' not found in any table."
            + " CHANGES(orders) AS c also has these metadata columns: _op, _version",
        resolver.explain(
            SqlUtil.newContextException(
                unqualified.getParserPosition(), RESOURCE.columnNotFound("_opp"))));
    assertEquals(
        "From line 1, column 16 to line 1, column 18: Column 'idd' not found in table 'c'."
            + " CHANGES(orders) AS c also has these metadata columns: _op, _version",
        resolver.explain(
            SqlUtil.newContextException(
                qualified.getComponentParserPosition(1),
                RESOURCE.columnNotFoundInTable("idd", "c"))));
    assertNull(
        resolver.explain(
            SqlUtil.newContextException(
                unqualified.getParserPosition(), RESOURCE.columnAmbiguous("_opp"))));
  }

  private void assertResolveFails(String sql, String message) {
    IllegalArgumentException e =
        assertThrows(sql, IllegalArgumentException.class, () -> resolver().resolve(parse(sql)));
    assertThat(sql, e.getMessage(), containsString(message));
  }

  private MetadataColumnResolver resolver() {
    return new MetadataColumnResolver(
        new CalciteCatalogReader(
            CalciteSchema.from(connection.getRootSchema()),
            ImmutableList.of(connection.getCurrentSchemaPlus().getName()),
            connection.getTypeFactory(),
            connection.config()));
  }

  private static SqlNode parse(String sql) throws SqlParseException {
    return SqlParser.create(sql, CONFIG).parseStmt();
  }

  private static Table table(String name, String column) {
    return Table.builder()
        .type("changes")
        .name(name)
        .schema(Schema.builder().addInt64Field("id").addStringField(column).build())
        .build();
  }

  private static class ChangesTableProvider extends InMemoryMetaTableProvider {
    @Override
    public String getTableType() {
      return "changes";
    }

    @Override
    public BeamSqlTable buildBeamSqlTable(Table table) {
      return new ChangesTable(table.getSchema());
    }
  }

  private static class ChangesTable extends SchemaBaseBeamTable {
    ChangesTable(Schema schema) {
      super(schema);
    }

    @Override
    public List<String> getChangeMetadataColumns() {
      return ImmutableList.of("_op", "_version");
    }

    @Override
    public BeamSqlTable forChanges(
        ChangeRange changes, Map<String, Object> options, List<String> metadataColumns) {
      Schema.Builder withColumns = Schema.builder().addFields(schema.getFields());
      metadataColumns.forEach(withColumns::addStringField);
      return new ChangesTable(withColumns.build());
    }

    @Override
    public PCollection<Row> buildIOReader(PBegin begin) {
      throw new UnsupportedOperationException();
    }

    @Override
    public POutput buildIOWriter(PCollection<Row> input) {
      throw new UnsupportedOperationException();
    }

    @Override
    public PCollection.IsBounded isBounded() {
      return PCollection.IsBounded.BOUNDED;
    }
  }
}
