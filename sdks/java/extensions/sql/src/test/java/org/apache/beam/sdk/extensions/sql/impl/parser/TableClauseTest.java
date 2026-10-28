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
package org.apache.beam.sdk.extensions.sql.impl.parser;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.TableNameExtractionUtils;
import org.apache.beam.sdk.extensions.sql.impl.TableName;
import org.apache.beam.sdk.extensions.sql.impl.parser.TableClause.Role;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.config.Lex;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlCall;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlDialect;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlSelect;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.parser.SqlParseException;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.parser.SqlParser;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.util.Util;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests {@code WITH} clauses of table references, which the parser folds into table names as the
 * canonical text of a {@link TableClause}. Statements are rendered without identifier quoting, so a
 * folded name component shows up as text such as {@code cat.db.`orders` WITH ('k' = 1)}.
 */
@RunWith(JUnit4.class)
public class TableClauseTest {
  private static final SqlParser.Config CONFIG =
      SqlParser.config().withLex(Lex.JAVA).withParserFactory(BeamSqlParser.FACTORY);
  private static final SqlDialect UNQUOTED = new SqlDialect(SqlDialect.EMPTY_CONTEXT);
  private static final String COMBINED =
      "WITH cannot be combined with table hints, EXTEND, FOR SYSTEM_TIME AS OF or OVER";
  private static final String ON_CTE = "WITH options cannot be applied to common table expression";
  private static final String NOT_A_LITERAL =
      "Option 'k' must be a string, numeric or boolean literal,"
          + " or an ARRAY[...] or MAP[...] of them";

  /** Statements that use the WITH keyword otherwise, and how they render. */
  private static final String[][] UNCHANGED_STATEMENTS = {
    {"WITH t AS (SELECT 1 AS a) SELECT * FROM t", "WITH t AS (SELECT 1 AS a) SELECT * FROM t"},
    {
      "INSERT INTO t WITH x AS (SELECT 1 AS a) SELECT * FROM x",
      "INSERT INTO t WITH x AS (SELECT 1 AS a) SELECT * FROM x"
    },
    {
      "INSERT INTO t (a) WITH x AS (SELECT 1 AS a) SELECT * FROM x",
      "INSERT INTO t (a) WITH x AS (SELECT 1 AS a) SELECT * FROM x"
    },
    {
      "SELECT * FROM UNNEST(ARRAY[1, 2]) WITH ORDINALITY AS t (x, o)",
      "SELECT * FROM UNNEST(ARRAY[1, 2]) WITH ORDINALITY AS t (x, o)"
    },
    {"SELECT * FROM ```t```", "SELECT * FROM `t`"},
  };

  /** Statements with WITH options and how they render once folded. */
  private static final String[][] FOLDED_STATEMENTS = {
    {
      "SELECT x.a FROM t WITH ('k' = 1) AS x(a, b)",
      "SELECT x.a FROM `t` WITH ('k' = 1) AS x (a, b)"
    },
    {
      "SELECT * FROM t WITH ('k' = 1) TABLESAMPLE BERNOULLI(10)",
      "SELECT * FROM `t` WITH ('k' = 1) AS t TABLESAMPLE BERNOULLI(10.0)"
    },
    {
      "SELECT * FROM t WITH ('k' = 1) MATCH_RECOGNIZE (PATTERN (A) DEFINE A AS true)",
      "SELECT * FROM `t` WITH ('k' = 1) AS t MATCH_RECOGNIZE( PATTERN (A) DEFINE A AS TRUE)"
    },
    {
      "SELECT * FROM a WITH ('k' = 1) x JOIN cat.db.b WITH ('k' = 2) ON x.id = b.id",
      "SELECT * FROM `a` WITH ('k' = 1) AS x"
          + " INNER JOIN cat.db.`b` WITH ('k' = 2) AS b ON x.id = b.id"
    },
    {
      "WITH x AS (SELECT * FROM a WITH ('k' = 1)) SELECT * FROM x, b WITH ('k' = 2)",
      "WITH x AS (SELECT * FROM `a` WITH ('k' = 1) AS a)"
          + " SELECT * FROM x, `b` WITH ('k' = 2) AS b"
    },
    {
      "WITH t AS (SELECT * FROM t WITH ('k' = 1) AS c) SELECT * FROM t",
      "WITH t AS (SELECT * FROM `t` WITH ('k' = 1) AS c) SELECT * FROM t"
    },
    {
      "WITH t AS (SELECT 1 AS a) SELECT * FROM db.t WITH ('k' = 1) AS t",
      "WITH t AS (SELECT 1 AS a) SELECT * FROM db.`t` WITH ('k' = 1) AS t"
    },
    {
      "SELECT * FROM (WITH t AS (SELECT 1 AS a) SELECT * FROM t), t WITH ('k' = 1) AS c",
      "SELECT * FROM (WITH t AS (SELECT 1 AS a) SELECT * FROM t), `t` WITH ('k' = 1) AS c"
    },
    {
      "INSERT INTO cat.db.mirror WITH ('mode' = 'merge-on-read')"
          + " SELECT * FROM cat.db.orders WITH ('k' = ARRAY['a'])",
      "INSERT INTO cat.db.INTO `mirror` WITH ('mode' = 'merge-on-read')"
          + " SELECT * FROM cat.db.`orders` WITH ('k' = ARRAY['a']) AS orders"
    },
    {
      "INSERT INTO t WITH ('k' = 1) (a, b) SELECT 1, 2",
      "INSERT INTO INTO `t` WITH ('k' = 1) (a, b) SELECT 1, 2"
    },
    {
      "INSERT INTO t WITH ('k' = 1) WITH x AS (SELECT 1 AS a) SELECT * FROM x",
      "INSERT INTO INTO `t` WITH ('k' = 1) WITH x AS (SELECT 1 AS a) SELECT * FROM x"
    },
  };

  /**
   * Malformed or misplaced WITH clauses, and part of the parse error each one raises. A ^ marks the
   * column the error is reported at.
   */
  private static final String[][] REJECTED_STATEMENTS = {
    {"SELECT * FROM t WITH ('k' = 1, ^'k' = 2)", "Duplicate option 'k'"},
    {
      "SELECT * FROM t WITH ('k' = 9223372036854775808)",
      "Option 'k' value 9223372036854775808 is out of range"
    },
    {"SELECT * FROM t WITH ('k' = 1e400)", "is out of range"},
    {"SELECT * FROM t WITH ('k' = -1e-400)", "is out of range"},
    {"SELECT * FROM t WITH ('m' = MAP['a', 1, 'a', 2])", "Duplicate MAP key 'a'"},
    {"SELECT * FROM t WITH (k = 1)", "Encountered \"k\""},
    {"SELECT * FROM t /*+ OPTIONS('a'='b') */ WITH ('k' = 1)", COMBINED},
    {"INSERT INTO t EXTEND (c INT) WITH ('k' = 1) SELECT 1, 2", COMBINED},
    {"SELECT * FROM t AS c ^WITH ('k' = 1)", "Encountered \"WITH\""},
    {"SELECT * FROM t WITH ('k' = 1) WITH ('j' = 2)", "'WITH'"},
    {"SELECT * FROM (SELECT * FROM t) WITH ('k' = 1)", "'WITH'"},
    {"INSERT INTO t (a) WITH ('k' = 1) SELECT 1", "'WITH'"},
    {
      "SELECT * FROM ^```t`` WITH ('k' = 1)`",
      "Table name '`t` WITH ('k' = 1)' is reserved for WITH options"
    },
    {
      "SELECT * FROM db.```t`` WITH ('k' = 1)` /*+ OPTIONS('a'='b') */ EXTEND (c INT)",
      "is reserved for WITH options"
    },
    {"INSERT INTO ```t`` WITH ('k' = 1)` SELECT 1", "is reserved for WITH options"},
    {"WITH t AS (SELECT 1 AS a) SELECT * FROM ^t WITH ('k' = 1)", ON_CTE + " 't'"},
    {
      "WITH s AS (SELECT 1 AS a), t AS (SELECT * FROM s WITH ('k' = 1)) SELECT * FROM t",
      ON_CTE + " 's'"
    },
    {"WITH RECURSIVE t AS (SELECT * FROM t WITH ('k' = 1)) SELECT * FROM t", ON_CTE + " 't'"},
    {
      "WITH t AS (SELECT 1 AS a) SELECT a FROM t ORDER BY (SELECT a FROM ^t WITH ('k' = 1))",
      ON_CTE + " 't'"
    },
  };

  @Test
  public void statementsWithoutOptionsParseAsBefore() throws Exception {
    for (String[] statement : UNCHANGED_STATEMENTS) {
      assertEquals(statement[0], statement[1], render(statement[0]));
    }
  }

  @Test
  public void optionsFoldIntoTheLastNameComponent() throws Exception {
    for (String[] statement : FOLDED_STATEMENTS) {
      assertEquals(statement[0], statement[1], render(statement[0]));
    }
  }

  @Test
  public void malformedOrMisplacedOptionsAreRejected() {
    for (String[] statement : REJECTED_STATEMENTS) {
      assertParseError(statement[0], statement[1]);
    }
    for (String value :
        new String[] {
          "NULL", "1 + 2", "x", "TIMESTAMP '2020-01-01 00:00:00'", "ARRAY[1, NULL]", "MAP['a']"
        }) {
      assertParseError("SELECT * FROM t WITH ('k' = " + value + ")", NOT_A_LITERAL);
    }
  }

  @Test
  public void optionValuesBecomeCanonicalJavaValuesInClauseOrder() throws Exception {
    SqlCall from =
        (SqlCall)
            ((SqlSelect)
                    SqlParser.create(
                            "SELECT * FROM t WITH ('s' = 'it''s', 'n' = -7, 'd' = 1.50,"
                                + " 'e' = 1e3, 'b' = true, 'c' = 'con' 'cat', 'a' = ARRAY[1, 'x'],"
                                + " 'm' = MAP['k', ARRAY[2.5]], 'h' = 9223372036854775807)",
                            CONFIG)
                        .parseQuery())
                .getFrom();
    SqlIdentifier name = from.operand(0);
    TableClause clause = checkStateNotNull(TableClause.decode(Util.last(name.names)));

    assertEquals(
        ImmutableMap.<String, Object>builder()
            .put("s", "it's")
            .put("n", -7L)
            .put("d", 1.5)
            .put("e", 1000.0)
            .put("b", true)
            .put("c", "concat")
            .put("a", ImmutableList.of(1L, "x"))
            .put("m", ImmutableMap.of("k", ImmutableList.of(2.5)))
            .put("h", Long.MAX_VALUE)
            .build(),
        clause.options());
    assertEquals(
        "`t` WITH ('s' = 'it''s', 'n' = -7, 'd' = 1.5, 'e' = 1000.0, 'b' = TRUE,"
            + " 'c' = 'concat', 'a' = ARRAY[1, 'x'], 'm' = MAP['k', ARRAY[2.5]],"
            + " 'h' = 9223372036854775807)",
        clause.encode());
  }

  @Test
  public void decodeReadsBackExactlyWhatEncodeWrites() {
    Map<String, Object> options =
        ImmutableMap.<String, Object>builder()
            .put("string", "it's `quoted`\n WITH ('x' = 1) 日本")
            .put("empty", "")
            .put("min", Long.MIN_VALUE)
            .put("max", Long.MAX_VALUE)
            .put("double", -0.001)
            .put("large", 1.0E20)
            .put("small", 1.0E-5)
            .put("zero", 0.0)
            .put("boolean", false)
            .put("list", ImmutableList.of(1L, "x", true, ImmutableList.of()))
            .put("map", ImmutableMap.of("k", ImmutableList.of(2.5), 3L, ImmutableMap.of()))
            .build();
    TableClause[] clauses = {
      TableClause.create("orders", Role.READ, options),
      TableClause.create("my `odd` 'name' 日本", Role.READ, ImmutableMap.of("k", 1L)),
      TableClause.create("orders", Role.WRITE, options),
    };
    for (TableClause clause : clauses) {
      assertEquals(clause.encode(), clause, TableClause.decode(clause.encode()));
    }

    String[] plainNames = {
      "orders",
      "`orders`",
      "`orders",
      "`orders` WITH ('a' = 1",
      "`orders`  WITH ('a' = 1)",
      "`orders` WITH ('a' = NULL)",
      "`orders` WITH ('a' = 1e3)",
      "INTO db.`orders` WITH ('a' = 1)",
    };
    for (String name : plainNames) {
      assertNull(name, TableClause.decode(name));
    }
  }

  @Test
  public void tableNamesAreExtractedWithoutOptions() throws Exception {
    assertEquals(
        Arrays.asList(
            TableName.create(Arrays.asList("cat", "db", "a")),
            TableName.create(Collections.singletonList("b")),
            TableName.create(Collections.singletonList("c"))),
        TableNameExtractionUtils.extractTableNamesFromNode(
            SqlParser.create(
                    "SELECT * FROM cat.db.a WITH ('k' = 1) x"
                        + " JOIN b WITH ('k' = 1) ON x.id = b.id, c",
                    CONFIG)
                .parseQuery()));
  }

  private static String render(String sql) throws SqlParseException {
    return SqlParser.create(sql, CONFIG)
        .parseStmt()
        .toSqlString(UNQUOTED)
        .getSql()
        .replace('\n', ' ');
  }

  /** Asserts that parsing fails with the message, at the column of the ^ in the SQL if any. */
  private static void assertParseError(String sqlWithCaret, String message) {
    String sql = sqlWithCaret.replace("^", "");
    SqlParseException e =
        assertThrows(sql, SqlParseException.class, () -> SqlParser.create(sql, CONFIG).parseStmt());
    assertThat(sql, e.getMessage(), containsString(message));
    if (sqlWithCaret.contains("^")) {
      assertEquals(sql, sqlWithCaret.indexOf('^') + 1, e.getPos().getColumnNum());
    }
  }
}
