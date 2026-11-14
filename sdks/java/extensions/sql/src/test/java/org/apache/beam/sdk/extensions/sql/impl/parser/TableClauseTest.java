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
import org.apache.beam.sdk.extensions.sql.meta.ChangeRange;
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
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests {@code CHANGES(...)} calls and {@code WITH} clauses of table references, which the parser
 * folds into table names as the canonical text of a {@link TableClause}. Statements are rendered
 * without identifier quoting, so a folded name component shows up as text such as {@code
 * cat.db.CHANGES(`orders`)}.
 */
@RunWith(JUnit4.class)
public class TableClauseTest {
  private static final SqlParser.Config CONFIG =
      SqlParser.config().withLex(Lex.JAVA).withParserFactory(BeamSqlParser.FACTORY);
  private static final SqlDialect UNQUOTED = new SqlDialect(SqlDialect.EMPTY_CONTEXT);
  private static final String NOT_A_TABLE =
      "CHANGES expects a table name as its first argument: CHANGES(table [, start [, end]])";
  private static final String NOT_A_BOUND =
      "A CHANGES bound must be a version (an integer literal),"
          + " a timestamp (a string or TIMESTAMP literal) or NULL";
  private static final String MIXED = "CHANGES bounds must both be versions or both be timestamps";
  private static final String COMBINED =
      "WITH cannot be combined with table hints, EXTEND, FOR SYSTEM_TIME AS OF or OVER";
  private static final String ON_CTE =
      "CHANGES and WITH cannot be applied to common table expression";
  private static final String NOT_A_LITERAL =
      "Option 'k' must be a string, numeric or boolean literal,"
          + " or an ARRAY[...] or MAP[...] of them";

  /** Statements and how the parser without CHANGES(...) and WITH rendered them. */
  private static final String[][] UNCHANGED_STATEMENTS = {
    {"WITH t AS (SELECT 1 AS a) SELECT * FROM t", "WITH t AS (SELECT 1 AS a) SELECT * FROM t"},
    {"SELECT * FROM changes", "SELECT * FROM changes"},
    {"SELECT * FROM t changes", "SELECT * FROM t AS changes"},
    {"SELECT * FROM t AS changes (a, b)", "SELECT * FROM t AS changes (a, b)"},
    {
      "SELECT changes.changes FROM db.changes changes WHERE changes IS NULL",
      "SELECT changes.changes FROM db.changes AS changes WHERE changes IS NULL"
    },
    {"SELECT * FROM changes (x INT)", "SELECT * FROM changes EXTEND (x INTEGER)"},
    {"SELECT changes(1)", "SELECT changes(1)"},
    {
      "WITH changes AS (SELECT 1 AS a) SELECT * FROM changes",
      "WITH changes AS (SELECT 1 AS a) SELECT * FROM changes"
    },
    {
      "INSERT INTO changes (changes) SELECT changes FROM changes",
      "INSERT INTO changes (changes) SELECT changes FROM changes"
    },
    {"DESCRIBE changes", "DESCRIBE TABLE changes"},
    {"DESCRIBE changes a", "DESCRIBE TABLE changes a"},
    {"SELECT * FROM f(1, x => 2) AS t", "SELECT * FROM TABLE(f(1, x => 2)) AS t"},
    {
      "SELECT * FROM db.CHANGES(t, 1), TABLE(db.CHANGES(t, 1))",
      "SELECT * FROM TABLE(db.CHANGES(t, 1)), TABLE(db.CHANGES(t, 1))"
    },
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

  /** Statements with CHANGES(...) or WITH and how they render once folded. */
  private static final String[][] FOLDED_STATEMENTS = {
    {"SELECT * FROM CHANGES(t)", "SELECT * FROM CHANGES(`t`) AS t"},
    {
      "SELECT * FROM changes(cat.db.orders, 123)",
      "SELECT * FROM cat.db.CHANGES(`orders`, start_version => 123) AS orders"
    },
    {
      "SELECT * FROM CHANGES(db.orders, 0, 9223372036854775807)",
      "SELECT * FROM db.CHANGES(`orders`, start_version => 0,"
          + " end_version => 9223372036854775807) AS orders"
    },
    {
      "SELECT * FROM CHANGES(orders, ' 2026-10-01', '2026-10-05 12:30:00.120')",
      "SELECT * FROM CHANGES(`orders`, start_timestamp => TIMESTAMP '2026-10-01 00:00:00',"
          + " end_timestamp => TIMESTAMP '2026-10-05 12:30:00.12') AS orders"
    },
    {
      "SELECT * FROM CHANGES(orders, '2026-10-01T12:30', TIMESTAMP '2026-10-01T12:30:00.5Z')",
      "SELECT * FROM CHANGES(`orders`, start_timestamp => TIMESTAMP '2026-10-01 12:30:00',"
          + " end_timestamp => TIMESTAMP '2026-10-01 12:30:00.5') AS orders"
    },
    {
      "SELECT * FROM CHANGES(orders, '2026-10-01T02:00+05:30', '2026-10-01 22:15:00.25-04:00')",
      "SELECT * FROM CHANGES(`orders`, start_timestamp => TIMESTAMP '2026-09-30 20:30:00',"
          + " end_timestamp => TIMESTAMP '2026-10-02 02:15:00.25') AS orders"
    },
    {
      "SELECT * FROM CHANGES(orders, TIMESTAMP '2026-10-01 01:02:03', NULL)",
      "SELECT * FROM CHANGES(`orders`, start_timestamp => TIMESTAMP '2026-10-01 01:02:03')"
          + " AS orders"
    },
    {
      "SELECT * FROM CHANGES(orders, NULL, 456)",
      "SELECT * FROM CHANGES(`orders`, end_version => 456) AS orders"
    },
    {"SELECT * FROM CHANGES(orders, NULL, NULL)", "SELECT * FROM CHANGES(`orders`) AS orders"},
    {
      "SELECT * FROM CHANGES(orders, start_version => 123, end_version => 456)",
      "SELECT * FROM CHANGES(`orders`, start_version => 123, end_version => 456) AS orders"
    },
    {
      "SELECT * FROM CHANGES(orders, END_VERSION => 456, Start_Version => NULL)",
      "SELECT * FROM CHANGES(`orders`, end_version => 456) AS orders"
    },
    {
      "SELECT * FROM CHANGES(orders, start_timestamp => '2026-10-01',"
          + " end_timestamp => TIMESTAMP '2026-10-05 00:00:00.5')",
      "SELECT * FROM CHANGES(`orders`, start_timestamp => TIMESTAMP '2026-10-01 00:00:00',"
          + " end_timestamp => TIMESTAMP '2026-10-05 00:00:00.5') AS orders"
    },
    {
      "SELECT * FROM CHANGES(orders, end_timestamp => '2026-10-' '05')",
      "SELECT * FROM CHANGES(`orders`, end_timestamp => TIMESTAMP '2026-10-05 00:00:00')"
          + " AS orders"
    },
    {
      "SELECT * FROM CHANGES(orders, 123, end_version => NULL)",
      "SELECT * FROM CHANGES(`orders`, start_version => 123) AS orders"
    },
    {
      "SELECT * FROM CHANGES(`my db`.`Orders`, NULL, end_timestamp => '9999-12-31 23:59:59.999')",
      "SELECT * FROM my db.CHANGES(`Orders`, end_timestamp => TIMESTAMP '9999-12-31 23:59:59.999')"
          + " AS Orders"
    },
    {"SELECT * FROM CHANGES(changes) changes", "SELECT * FROM CHANGES(`changes`) AS changes"},
    {
      "SELECT c.id FROM CHANGES(orders, 123) WITH ('streaming' = true) AS c",
      "SELECT c.id FROM CHANGES(`orders`, start_version => 123) WITH ('streaming' = TRUE) AS c"
    },
    {
      "SELECT c.x FROM CHANGES(orders) WITH ('k' = 1) c (x, y)",
      "SELECT c.x FROM CHANGES(`orders`) WITH ('k' = 1) AS c (x, y)"
    },
    {
      "SELECT * FROM CHANGES(t) x TABLESAMPLE BERNOULLI(10)",
      "SELECT * FROM CHANGES(`t`) AS x TABLESAMPLE BERNOULLI(10.0)"
    },
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
      "SELECT * FROM CHANGES(a, 1) x JOIN CHANGES(a, 2) WITH ('k' = 2) y ON x.id = y.id",
      "SELECT * FROM CHANGES(`a`, start_version => 1) AS x"
          + " INNER JOIN CHANGES(`a`, start_version => 2) WITH ('k' = 2) AS y ON x.id = y.id"
    },
    {
      "SELECT * FROM a WHERE id IN (SELECT id FROM CHANGES(b) c)",
      "SELECT * FROM a WHERE id IN (SELECT id FROM CHANGES(`b`) AS c)"
    },
    {
      "WITH x AS (SELECT * FROM a WITH ('k' = 1)) SELECT * FROM x, b WITH ('k' = 2)",
      "WITH x AS (SELECT * FROM `a` WITH ('k' = 1) AS a)"
          + " SELECT * FROM x, `b` WITH ('k' = 2) AS b"
    },
    {
      "WITH x AS (SELECT * FROM CHANGES(a, 1)) SELECT * FROM x, b WITH ('k' = 1)",
      "WITH x AS (SELECT * FROM CHANGES(`a`, start_version => 1) AS a)"
          + " SELECT * FROM x, `b` WITH ('k' = 1) AS b"
    },
    {
      "MERGE INTO t USING CHANGES(s) x ON t.id = x.id WHEN MATCHED THEN UPDATE SET a = x.a",
      "MERGE INTO t USING CHANGES(`s`) AS x ON t.id = x.id WHEN MATCHED THEN UPDATE SET a = x.a"
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
      "WITH t AS (SELECT * FROM CHANGES(t) AS c) SELECT * FROM t",
      "WITH t AS (SELECT * FROM CHANGES(`t`) AS c) SELECT * FROM t"
    },
    {
      "WITH t AS (SELECT 1 AS a) SELECT * FROM CHANGES(db.t) AS t",
      "WITH t AS (SELECT 1 AS a) SELECT * FROM db.CHANGES(`t`) AS t"
    },
    {
      "SELECT * FROM (WITH t AS (SELECT 1 AS a) SELECT * FROM t), CHANGES(t) AS c",
      "SELECT * FROM (WITH t AS (SELECT 1 AS a) SELECT * FROM t), CHANGES(`t`) AS c"
    },
    {
      "INSERT INTO cat.db.mirror WITH ('mode' = 'merge-on-read')"
          + " SELECT * FROM cat.db.orders WITH ('k' = ARRAY['a'])",
      "INSERT INTO cat.db.INTO `mirror` WITH ('mode' = 'merge-on-read')"
          + " SELECT * FROM cat.db.`orders` WITH ('k' = ARRAY['a']) AS orders"
    },
    {
      "INSERT INTO cat.db.mirror WITH ('mode' = 'merge-on-read')"
          + " SELECT * FROM CHANGES(cat.db.orders, 123) WITH ('k' = ARRAY['a'])",
      "INSERT INTO cat.db.INTO `mirror` WITH ('mode' = 'merge-on-read')"
          + " SELECT * FROM cat.db.CHANGES(`orders`, start_version => 123) WITH ('k' = ARRAY['a'])"
          + " AS orders"
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
   * Malformed or misplaced CHANGES(...) calls and WITH clauses, and part of the parse error each
   * one raises. A ^ marks the column the error is reported at.
   */
  private static final String[][] REJECTED_STATEMENTS = {
    {"SELECT * FROM ^CHANGES()", NOT_A_TABLE},
    {"SELECT * FROM CHANGES(^'t')", NOT_A_TABLE},
    {"SELECT * FROM CHANGES(^t.a + 1)", NOT_A_TABLE},
    {"SELECT * FROM CHANGES(^t.*)", NOT_A_TABLE},
    {"SELECT * FROM CHANGES(TABLE t)", NOT_A_TABLE},
    {"SELECT * FROM CHANGES(^start_version => 1)", NOT_A_TABLE},
    {
      "SELECT * FROM CHANGES(t, 1, 2, ^3)",
      "CHANGES takes at most 2 positional bounds: CHANGES(table [, start [, end]])"
    },
    {
      "SELECT * FROM CHANGES(t, end_version => 2, ^1)",
      "A positional CHANGES bound cannot follow a named one"
    },
    {"SELECT * FROM CHANGES(t, 1, ^start_version => 2)", "CHANGES start bound is given twice"},
    {
      "SELECT * FROM CHANGES(t, end_version => 1, ^end_version => 2)",
      "CHANGES end bound is given twice"
    },
    {
      "SELECT * FROM CHANGES(t, NULL, NULL, ^start_timestamp => NULL)",
      "CHANGES start bound is given twice"
    },
    {
      "SELECT * FROM CHANGES(t, start_version => 1, ^START_TIMESTAMP => '2026-01-01')",
      "CHANGES start bound is given twice"
    },
    {
      "SELECT * FROM CHANGES(t, ^from_version => 1)",
      "Unknown CHANGES parameter 'from_version'."
          + " Accepted parameters: start_version, end_version, start_timestamp, end_timestamp"
    },
    {
      "SELECT * FROM CHANGES(t, ^metadata_columns => ARRAY['_change_type'])",
      "Unknown CHANGES parameter 'metadata_columns'."
    },
    {"SELECT * FROM CHANGES(t, ^1.5)", NOT_A_BOUND},
    {"SELECT * FROM CHANGES(t, ^1e3)", NOT_A_BOUND},
    {"SELECT * FROM CHANGES(t, NULL, ^x)", NOT_A_BOUND},
    {"SELECT * FROM CHANGES(t, ^TRUE)", NOT_A_BOUND},
    {"SELECT * FROM CHANGES(t, ^UNKNOWN)", NOT_A_BOUND},
    {"SELECT * FROM CHANGES(t, ^DATE '2026-01-01')", NOT_A_BOUND},
    {"SELECT * FROM CHANGES(t, ^CAST(NULL AS BIGINT))", NOT_A_BOUND},
    {
      "SELECT * FROM CHANGES(t, start_version => ^'1')",
      "CHANGES parameter 'start_version' must be an integer literal or NULL"
    },
    {
      "SELECT * FROM CHANGES(t, end_version => ^TIMESTAMP '2026-01-01 00:00:00')",
      "CHANGES parameter 'end_version' must be an integer literal or NULL"
    },
    {
      "SELECT * FROM CHANGES(t, End_Timestamp => ^1)",
      "CHANGES parameter 'end_timestamp' must be a string or TIMESTAMP literal or NULL"
    },
    {
      "SELECT * FROM CHANGES(t, ^-1)",
      "CHANGES version -1 must be between 0 and 9223372036854775807"
    },
    {
      "SELECT * FROM CHANGES(t, NULL, ^9223372036854775808)",
      "CHANGES version 9223372036854775808 must be between 0 and 9223372036854775807"
    },
    {
      "SELECT * FROM CHANGES(t, ^'yesterday')",
      "Illegal TIMESTAMP literal 'yesterday': expected yyyy-MM-dd, optionally followed by T or"
          + " a space, HH:mm[:ss[.SSS]] and an optional Z, +hh:mm or -hh:mm"
    },
    {"SELECT * FROM CHANGES(t, ^'2026-10-01Z')", "Illegal TIMESTAMP literal '2026-10-01Z'"},
    {
      "SELECT * FROM CHANGES(t, ^'2026-02-30')",
      "Illegal TIMESTAMP literal '2026-02-30': Invalid date 'FEBRUARY 30'"
    },
    {"SELECT * FROM CHANGES(t, ^'2026/10/01')", "Illegal TIMESTAMP literal '2026/10/01'"},
    {"SELECT * FROM CHANGES(t, ^'2026-1-1')", "Illegal TIMESTAMP literal '2026-1-1'"},
    {
      "SELECT * FROM CHANGES(t, ^'2026-01-01 00:00:00.1234')",
      "CHANGES supports at most 3 fractional digits"
    },
    {
      "SELECT * FROM CHANGES(t, ^'2026-01-01T00:00:00.123000+01:00')",
      "CHANGES supports at most 3 fractional digits"
    },
    {
      "SELECT * FROM CHANGES(t, end_timestamp => ^TIMESTAMP '12026-01-01 00:00:00')",
      "Illegal TIMESTAMP literal '12026-01-01 00:00:00'"
    },
    {
      "SELECT * FROM CHANGES(t, NULL, ^'9999-12-31T23:00-05:00')",
      "CHANGES supports years 0000 to 9999 in UTC"
    },
    {"SELECT * FROM CHANGES(t, 1, ^'2026-01-01')", MIXED},
    {"SELECT * FROM CHANGES(t, end_timestamp => '2026-01-01', start_version => ^1)", MIXED},
    {
      "SELECT * FROM ^f(1) WITH ('k' = 1)", "WITH cannot follow a table function other than CHANGES"
    },
    {
      "SELECT * FROM db.^CHANGES(t) WITH ('k' = 1)",
      "WITH cannot follow a table function other than CHANGES"
    },
    {
      "SELECT * FROM TABLE(^CHANGES(t, 1))",
      "CHANGES cannot be called in TABLE(...), only as FROM CHANGES(table [, start [, end]])"
    },
    {"SELECT * FROM a, LATERAL TABLE(^changes(t))", "CHANGES cannot be called in TABLE(...)"},
    {"SELECT * FROM CHANGES(t) AS c ^WITH ('k' = 1)", "Encountered \"WITH\""},
    {"SELECT * FROM CHANGES(t) WITH ('k' = 1) ^WITH ('j' = 2)", "'WITH'"},
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
      "Table name '`t` WITH ('k' = 1)' is reserved for CHANGES and WITH"
    },
    {
      "SELECT * FROM db.```t`` WITH ('k' = 1)` /*+ OPTIONS('a'='b') */ EXTEND (c INT)",
      "is reserved for CHANGES and WITH"
    },
    {"INSERT INTO ```t`` WITH ('k' = 1)` SELECT 1", "is reserved for CHANGES and WITH"},
    {
      "SELECT * FROM db.`CHANGES(``t``)` /*+ OPTIONS('a'='b') */ EXTEND (c INT)",
      "Table name 'CHANGES(`t`)' is reserved"
    },
    {"SELECT * FROM CHANGES(^`CHANGES(``t``)`)", "Table name 'CHANGES(`t`)' is reserved"},
    {
      "INSERT INTO `CHANGES(``t``, start_version => 1)` SELECT 1",
      "Table name 'CHANGES(`t`, start_version => 1)' is reserved"
    },
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
    {"WITH t AS (SELECT 1 AS a) SELECT * FROM CHANGES(^t) AS c", ON_CTE + " 't'"},
    {
      "WITH s AS (SELECT 1 AS a), t AS (SELECT * FROM CHANGES(s) AS c) SELECT * FROM t",
      ON_CTE + " 's'"
    },
    {
      "WITH t AS (SELECT 1 AS a) SELECT * FROM u WHERE a IN (SELECT a FROM CHANGES(t, 1) c)",
      ON_CTE + " 't'"
    },
    {"WITH RECURSIVE t AS (SELECT * FROM CHANGES(t) AS c) SELECT * FROM t", ON_CTE + " 't'"},
  };

  @Test
  public void statementsWithoutChangesOrWithParseAsBefore() throws Exception {
    for (String[] statement : UNCHANGED_STATEMENTS) {
      assertEquals(statement[0], statement[1], render(statement[0]));
    }
  }

  @Test
  public void changesAndWithFoldIntoTheLastNameComponent() throws Exception {
    for (String[] statement : FOLDED_STATEMENTS) {
      assertEquals(statement[0], statement[1], render(statement[0]));
    }
  }

  @Test
  public void malformedOrMisplacedChangesAndWithAreRejected() {
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
      TableClause.createChanges("orders", ChangeRange.all(), ImmutableMap.of()),
      TableClause.createChanges(
          "my `odd` 'name' 日本", ChangeRange.versions(0L, Long.MAX_VALUE), options),
      TableClause.createChanges("orders", ChangeRange.versions(null, 5L), ImmutableMap.of()),
      TableClause.createChanges("orders", ChangeRange.timestamps(new Instant(-1), null), options),
      TableClause.createChanges(
          "orders", ChangeRange.timestamps(null, new Instant(1)), ImmutableMap.of()),
      TableClause.createChanges("orders", ChangeRange.versions(1L, 5L), options)
          .withMetadataColumns(ImmutableList.of("_change_type", "it's `odd`")),
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
      "INTO `orders`",
      "CHANGES(`orders`",
      "CHANGES(`orders`) ",
      "changes(`orders`)",
      "CHANGES (`orders`)",
      "CHANGES(db.`orders`)",
      "CHANGES(`orders`, 1)",
      "CHANGES(`orders`,start_version => 1)",
      "CHANGES(`orders`, start_version => 01)",
      "CHANGES(`orders`, start_version => NULL)",
      "CHANGES(`orders`, start_version => 99999999999999999999)",
      "CHANGES(`orders`, end_version => 5, start_version => 1)",
      "CHANGES(`orders`, start_timestamp => '2026-01-01 00:00:00')",
      "CHANGES(`orders`, start_timestamp => TIMESTAMP '2026-01-01')",
      "CHANGES(`orders`) WITH ('a' = 1",
      "INTO CHANGES(`orders`) WITH ('a' = 1)",
      "CHANGES(`orders`, metadata_columns => ARRAY['a'], start_version => 1)",
      "CHANGES(`orders`, metadata_columns => ARRAY[1])",
    };
    for (String name : plainNames) {
      assertNull(name, TableClause.decode(name));
    }
  }

  @Test
  public void tableNamesAreExtractedWithoutChangesOrWith() throws Exception {
    assertEquals(
        Arrays.asList(
            TableName.create(Arrays.asList("cat", "db", "a")),
            TableName.create(Collections.singletonList("b")),
            TableName.create(Collections.singletonList("c"))),
        TableNameExtractionUtils.extractTableNamesFromNode(
            SqlParser.create(
                    "SELECT * FROM CHANGES(cat.db.a, 1) x"
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
