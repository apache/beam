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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.google.auto.value.AutoValue;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.sql.integrationtest.BeamSqlBuiltinFunctionsIntegrationTestBase;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * DSL compliance tests for the row-level operators of {@link
 * org.apache.calcite.sql.fun.SqlStdOperatorTable}.
 */
public class BeamSqlDslSqlStdOperatorsTest extends BeamSqlBuiltinFunctionsIntegrationTestBase {
  private static final BigDecimal ZERO = BigDecimal.valueOf(0.0);
  private static final BigDecimal ONE = BigDecimal.valueOf(1.0);
  private static final BigDecimal ONE2 = BigDecimal.valueOf(1.0).multiply(BigDecimal.valueOf(1.0));
  private static final BigDecimal ONE10 =
      BigDecimal.ONE.divide(BigDecimal.ONE, 10, RoundingMode.HALF_EVEN);
  private static final BigDecimal TWO = BigDecimal.valueOf(2.0);
  private static final BigDecimal TWO0 = BigDecimal.ONE.add(BigDecimal.ONE);

  @Rule public ExpectedException thrown = ExpectedException.none();

  /** Calcite operators are identified by name and kind. */
  @AutoValue
  abstract static class SqlOperatorId {
    abstract String name();

    abstract SqlKind kind();
  }

  private static SqlOperatorId sqlOperatorId(String nameAndKind) {
    return sqlOperatorId(nameAndKind, SqlKind.valueOf(nameAndKind));
  }

  private static SqlOperatorId sqlOperatorId(String name, SqlKind kind) {
    return new AutoValue_BeamSqlDslSqlStdOperatorsTest_SqlOperatorId(name, kind);
  }

  private static SqlOperatorId sqlOperatorId(SqlOperatorTest annotation) {
    return sqlOperatorId(annotation.name(), SqlKind.valueOf(annotation.kind()));
  }

  private static SqlOperatorId sqlOperatorId(SqlOperator sqlOperator) {
    return sqlOperatorId(sqlOperator.getName(), sqlOperator.getKind());
  }

  private static final List<SqlOperatorId> NON_ROW_OPERATORS =
      ImmutableList.of(
              SqlStdOperatorTable.ELEMENT_SLICE, // internal
              SqlStdOperatorTable.EXCEPT,
              SqlStdOperatorTable.EXCEPT_ALL,
              SqlStdOperatorTable.INTERSECT,
              SqlStdOperatorTable.INTERSECT_ALL,
              SqlStdOperatorTable.LITERAL_CHAIN, // internal
              SqlStdOperatorTable.PATTERN_CONCAT, // "," PATTERN_CONCAT
              SqlStdOperatorTable.UNION,
              SqlStdOperatorTable.UNION_ALL)
          .stream()
          .map(op -> sqlOperatorId(op))
          .collect(Collectors.toList());

  /**
   * LEGACY ADAPTER - DO NOT USE DIRECTLY. Use {@code getAnnotationsByType(SqlOperatorTest.class)},
   * a more reliable method for retrieving repeated annotations.
   *
   * <p>This is a virtual annotation that is only present when there are more than one {@link
   * SqlOperatorTest} annotations. When there is just one {@link SqlOperatorTest} annotation the
   * proxying is not in place.
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.METHOD})
  private @interface SqlOperatorTests {
    SqlOperatorTest[] value();
  }

  /**
   * Annotation that declares a test method has the tests for the {@link SqlOperatorId}.
   *
   * <p>It is almost identical to {@link SqlOperatorId} but complex types cannot be part of
   * annotations and there are minor benefits to having a non-annotation class for passing around
   * the ids.
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.METHOD})
  @Repeatable(SqlOperatorTests.class)
  private @interface SqlOperatorTest {
    String name();

    String kind();
  }

  private Set<SqlOperatorId> getTestedOperators() {
    Set<SqlOperatorId> testedOperators = new HashSet<>();

    for (Method method : getClass().getMethods()) {
      testedOperators.addAll(
          Arrays.stream(method.getAnnotationsByType(SqlOperatorTest.class))
              .map(annotation -> sqlOperatorId(annotation))
              .collect(Collectors.toList()));
    }

    return testedOperators;
  }

  private Set<SqlOperatorId> getDeclaredOperators() {
    Set<SqlOperatorId> declaredOperators = new HashSet<>();

    declaredOperators.addAll(
        SqlStdOperatorTable.instance()
            .getOperatorList()
            .stream()
            .map(operator -> sqlOperatorId(operator.getName(), operator.getKind()))
            .collect(Collectors.toList()));

    return declaredOperators;
  }

  private final Comparator<SqlOperatorId> orderByNameThenKind =
      Ordering.compound(
          ImmutableList.of(
              Comparator.comparing((SqlOperatorId operator) -> operator.name()),
              Comparator.comparing((SqlOperatorId operator) -> operator.kind())));

  /** Smoke test that the whitelists and utility functions actually work. */
  @Test
  @SqlOperatorTest(name = "CARDINALITY", kind = "OTHER_FUNCTION")
  public void testAnnotationEquality() throws Exception {
    Method thisMethod = getClass().getMethod("testAnnotationEquality");
    SqlOperatorTest sqlOperatorTest = thisMethod.getAnnotationsByType(SqlOperatorTest.class)[0];
    assertThat(
        sqlOperatorId(sqlOperatorTest),
        equalTo(sqlOperatorId("CARDINALITY", SqlKind.OTHER_FUNCTION)));
  }

  /**
   * Tests that all operators in {@link SqlStdOperatorTable} have DSL-level tests. Operators in the
   * table are uniquely identified by (kind, name).
   */
  @Ignore("https://issues.apache.org/jira/browse/BEAM-4573")
  @Test
  public void testThatAllOperatorsAreTested() {
    Set<SqlOperatorId> untestedOperators = new HashSet<>();
    untestedOperators.addAll(getDeclaredOperators());

    // Query-level operators need their own larger test suites
    untestedOperators.removeAll(NON_ROW_OPERATORS);
    untestedOperators.removeAll(getTestedOperators());

    if (!untestedOperators.isEmpty()) {
      // Sorting is just to make failures more readable until we have 100% coverage
      List<SqlOperatorId> untestedList = Lists.newArrayList(untestedOperators);
      untestedList.sort(orderByNameThenKind);
      fail(
          String.format(
              "No tests declared for %s operators:\n\t%s",
              untestedList.size(), Joiner.on("\n\t").join(untestedList)));
    }
  }

  /**
   * Tests that we didn't typo an annotation, that all things we claim to test are real operators.
   */
  @Test
  public void testThatOperatorsExist() {
    Set<SqlOperatorId> undeclaredOperators = new HashSet<>();
    undeclaredOperators.addAll(getTestedOperators());
    undeclaredOperators.removeAll(getDeclaredOperators());

    if (!undeclaredOperators.isEmpty()) {
      // Sorting is just to make failures more readable
      List<SqlOperatorId> undeclaredList = Lists.newArrayList(undeclaredOperators);
      undeclaredList.sort(orderByNameThenKind);
      fail(
          "Tests declared for nonexistent operators:\n\t" + Joiner.on("\n\t").join(undeclaredList));
    }
  }

  @Test
  @SqlOperatorTest(name = "OR", kind = "OR")
  @SqlOperatorTest(name = "NOT", kind = "NOT")
  @SqlOperatorTest(name = "AND", kind = "AND")
  public void testLogicOperators() {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("1 = 1 AND 1 = 1", true)
            .addExpr("1 = 1 OR 1 = 2", true)
            .addExpr("NOT 1 = 2", true)
            .addExpr("(NOT 1 = 2) AND (2 = 1 OR 3 = 3)", true)
            .addExpr("2 = 2 AND 2 = 1", false)
            .addExpr("1 = 2 OR 3 = 2", false)
            .addExpr("NOT 1 = 1", false)
            .addExpr("(NOT 2 = 2) AND (1 = 2 OR 2 = 3)", false)
            .addExpr("'a' = 'a' AND 'a' = 'a'", true)
            .addExpr("'a' = 'a' OR 'a' = 'b'", true)
            .addExpr("NOT 'a' = 'b'", true)
            .addExpr("(NOT 'a' = 'b') AND ('b' = 'a' OR 'c' = 'c')", true)
            .addExpr("'b' = 'b' AND 'b' = 'a'", false)
            .addExpr("'a' = 'b' OR 'c' = 'b'", false)
            .addExpr("NOT 'a' = 'a'", false)
            .addExpr("(NOT 'b' = 'b') AND ('a' = 'b' OR 'b' = 'c')", false)
            .addExpr("1.0 = 1.0 AND 1.0 = 1.0", true)
            .addExpr("1.0 = 1.0 OR 1.0 = 2.0", true)
            .addExpr("NOT 1.0 = 2.0", true)
            .addExpr("(NOT 1.0 = 2.0) AND (2.0 = 1.0 OR 3.0 = 3.0)", true)
            .addExpr("2.0 = 2.0 AND 2.0 = 1.0", false)
            .addExpr("1.0 = 2.0 OR 3.0 = 2.0", false)
            .addExpr("NOT 1.0 = 1.0", false)
            .addExpr("(NOT 2.0 = 2.0) AND (1.0 = 2.0 OR 2.0 = 3.0)", false)
            .addExpr("NOT true", false)
            .addExpr("NOT false", true)
            .addExpr("true AND true", true)
            .addExpr("true AND false", false)
            .addExpr("false AND false", false)
            .addExpr("true OR true", true)
            .addExpr("true OR false", true)
            .addExpr("false OR false", false)
            .addExpr("(NOT false) AND (true OR false)", true)
            .addExpr("(NOT true) AND (true OR false)", false)
            .addExpr("(NOT false) OR (true and false)", true)
            .addExpr("(NOT true) OR (true and false)", false);

    checker.buildRunAndCheck();
  }

  @Test
  @SqlOperatorTest(name = "+", kind = "PLUS")
  @SqlOperatorTest(name = "-", kind = "MINUS")
  @SqlOperatorTest(name = "*", kind = "TIMES")
  @SqlOperatorTest(name = "/", kind = "DIVIDE")
  @SqlOperatorTest(name = "MOD", kind = "MOD")
  public void testArithmeticOperator() {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("1 + 1", 2)
            .addExpr("1.0 + 1", TWO)
            .addExpr("1 + 1.0", TWO)
            .addExpr("1.0 + 1.0", TWO)
            .addExpr("c_tinyint + c_tinyint", (byte) 2)
            .addExpr("c_smallint + c_smallint", (short) 2)
            .addExpr("c_bigint + c_bigint", 2L)
            .addExpr("c_decimal + c_decimal", TWO0)
            .addExpr("c_tinyint + c_decimal", TWO0)
            .addExpr("c_float + c_decimal", 2.0)
            .addExpr("c_double + c_decimal", 2.0)
            .addExpr("c_float + c_float", 2.0f)
            .addExpr("c_double + c_float", 2.0)
            .addExpr("c_double + c_double", 2.0)
            .addExpr("c_float + c_bigint", 2.0f)
            .addExpr("c_double + c_bigint", 2.0)
            .addExpr("1 - 1", 0)
            .addExpr("1.0 - 1", ZERO)
            .addExpr("1 - 0.0", ONE)
            .addExpr("1.0 - 1.0", ZERO)
            .addExpr("c_tinyint - c_tinyint", (byte) 0)
            .addExpr("c_smallint - c_smallint", (short) 0)
            .addExpr("c_bigint - c_bigint", 0L)
            .addExpr("c_decimal - c_decimal", BigDecimal.ZERO)
            .addExpr("c_tinyint - c_decimal", BigDecimal.ZERO)
            .addExpr("c_float - c_decimal", 0.0)
            .addExpr("c_double - c_decimal", 0.0)
            .addExpr("c_float - c_float", 0.0f)
            .addExpr("c_double - c_float", 0.0)
            .addExpr("c_double - c_double", 0.0)
            .addExpr("c_float - c_bigint", 0.0f)
            .addExpr("c_double - c_bigint", 0.0)
            .addExpr("1 * 1", 1)
            .addExpr("1.0 * 1", ONE)
            .addExpr("1 * 1.0", ONE)
            .addExpr("1.0 * 1.0", ONE2)
            .addExpr("c_tinyint * c_tinyint", (byte) 1)
            .addExpr("c_smallint * c_smallint", (short) 1)
            .addExpr("c_bigint * c_bigint", 1L)
            .addExpr("c_decimal * c_decimal", BigDecimal.ONE)
            .addExpr("c_tinyint * c_decimal", BigDecimal.ONE)
            .addExpr("c_float * c_decimal", 1.0)
            .addExpr("c_double * c_decimal", 1.0)
            .addExpr("c_float * c_float", 1.0f)
            .addExpr("c_double * c_float", 1.0)
            .addExpr("c_double * c_double", 1.0)
            .addExpr("c_float * c_bigint", 1.0f)
            .addExpr("c_double * c_bigint", 1.0)
            .addExpr("1 / 1", 1)
            .addExpr("1.0 / 1", ONE10)
            .addExpr("1 / 1.0", ONE10)
            .addExpr("1.0 / 1.0", ONE10)
            .addExpr("c_tinyint / c_tinyint", (byte) 1)
            .addExpr("c_smallint / c_smallint", (short) 1)
            .addExpr("c_bigint / c_bigint", 1L)
            .addExpr("c_decimal / c_decimal", ONE10)
            .addExpr("c_tinyint / c_decimal", ONE10)
            .addExpr("c_float / c_decimal", 1.0)
            .addExpr("c_double / c_decimal", 1.0)
            .addExpr("c_float / c_float", 1.0f)
            .addExpr("c_double / c_float", 1.0)
            .addExpr("c_double / c_double", 1.0)
            .addExpr("c_float / c_bigint", 1.0f)
            .addExpr("c_double / c_bigint", 1.0)
            .addExpr("mod(1, 1)", 0)
            .addExpr("mod(1.0, 1)", 0)
            .addExpr("mod(1, 1.0)", ZERO)
            .addExpr("mod(1.0, 1.0)", ZERO)
            .addExpr("mod(c_tinyint, c_tinyint)", (byte) 0)
            .addExpr("mod(c_smallint, c_smallint)", (short) 0)
            .addExpr("mod(c_bigint, c_bigint)", 0L)
            .addExpr("mod(c_decimal, c_decimal)", ZERO)
            .addExpr("mod(c_tinyint, c_decimal)", ZERO)
            // Test overflow
            .addExpr("c_tinyint_max + c_tinyint_max", (byte) -2)
            .addExpr("c_smallint_max + c_smallint_max", (short) -2)
            .addExpr("c_integer_max + c_integer_max", -2)
            .addExpr("c_bigint_max + c_bigint_max", -2L);

    checker.buildRunAndCheck();
  }

  @Test
  @SqlOperatorTest(name = "<", kind = "LESS_THAN")
  @SqlOperatorTest(name = ">", kind = "GREATER_THAN")
  @SqlOperatorTest(name = "<=", kind = "LESS_THAN_OR_EQUAL")
  @SqlOperatorTest(name = "<>", kind = "NOT_EQUALS")
  @SqlOperatorTest(name = "=", kind = "EQUALS")
  @SqlOperatorTest(name = ">=", kind = "GREATER_THAN_OR_EQUAL")
  @SqlOperatorTest(name = "LIKE", kind = "LIKE")
  @SqlOperatorTest(name = "IS NOT NULL", kind = "IS_NOT_NULL")
  @SqlOperatorTest(name = "IS NULL", kind = "IS_NULL")
  @SqlOperatorTest(name = "IS TRUE", kind = "IS_TRUE")
  @SqlOperatorTest(name = "IS NOT TRUE", kind = "IS_NOT_TRUE")
  @SqlOperatorTest(name = "IS FALSE", kind = "IS_FALSE")
  @SqlOperatorTest(name = "IS NOT FALSE", kind = "IS_NOT_FALSE")
  @SqlOperatorTest(name = "IS UNKNOWN", kind = "IS_NULL")
  @SqlOperatorTest(name = "IS NOT UNKNOWN", kind = "IS_NOT_NULL")
  @SqlOperatorTest(name = "IS DISTINCT FROM", kind = "IS_DISTINCT_FROM")
  @SqlOperatorTest(name = "IS NOT DISTINCT FROM", kind = "IS_NOT_DISTINCT_FROM")
  public void testComparisonOperatorFunction() {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("1 < 2", true)
            .addExpr("2 < 1", false)
            .addExpr("'a' < 'b'", true)
            .addExpr("'b' < 'a'", false)
            .addExpr("1.0 < 2.0", true)
            .addExpr("2.0 < 1.0", false)
            .addExpr("9223372036854775806 < 9223372036854775807", true)
            .addExpr("9223372036854775807 < 9223372036854775806", false)
            .addExpr("false < true", true)
            .addExpr("true < false", false)
            .addExpr("1 > 2", false)
            .addExpr("2 > 1", true)
            .addExpr("'a' > 'b'", false)
            .addExpr("'b' > 'a'", true)
            .addExpr("1.0 > 2.0", false)
            .addExpr("2.0 > 1.0", true)
            .addExpr("9223372036854775806 > 9223372036854775807", false)
            .addExpr("9223372036854775807 > 9223372036854775806", true)
            .addExpr("false > true", false)
            .addExpr("true > false", true)
            .addExpr("1 <> 2", true)
            .addExpr("1 <> 1", false)
            .addExpr("'a' <> 'b'", true)
            .addExpr("'a' <> 'a'", false)
            .addExpr("1.0 <> 2.0", true)
            .addExpr("1.0 <> 1.0", false)
            .addExpr("9223372036854775806 <> 9223372036854775807", true)
            .addExpr("9223372036854775806 <> 9223372036854775806", false)
            .addExpr("false <> true", true)
            .addExpr("false <> false", false)
            .addExpr("1 = 1", true)
            .addExpr("2 = 1", false)
            .addExpr("'a' = 'a'", true)
            .addExpr("'b' = 'a'", false)
            .addExpr("1.0 = 1.0", true)
            .addExpr("2.0 = 1.0", false)
            .addExpr("9223372036854775807 = 9223372036854775807", true)
            .addExpr("9223372036854775807 = 9223372036854775806", false)
            .addExpr("true = true", true)
            .addExpr("true = false", false)
            .addExpr("1 >= 2", false)
            .addExpr("2 >= 2", true)
            .addExpr("2 >= 1", true)
            .addExpr("'a' >= 'b'", false)
            .addExpr("'b' >= 'b'", true)
            .addExpr("'b' >= 'a'", true)
            .addExpr("1.0 >= 2.0", false)
            .addExpr("2.0 >= 1.0", true)
            .addExpr("2.0 >= 2.0", true)
            .addExpr("9223372036854775806 >= 9223372036854775807", false)
            .addExpr("9223372036854775807 >= 9223372036854775806", true)
            .addExpr("9223372036854775807 >= 9223372036854775807", true)
            .addExpr("false >= true", false)
            .addExpr("true >= false", true)
            .addExpr("true >= true", true)
            .addExpr("'string_true_test' LIKE 'string_true_test'", true)
            .addExpr("'string_true_test' LIKE 'string_false_test'", false)
            .addExpr("'string_false_test' LIKE 'string_false_test'", true)
            .addExpr("'string_false_test' LIKE 'string_true_test'", false)
            .addExpr("'string_true_test' LIKE 'string_true_test%'", true)
            .addExpr("'string_true_test' LIKE 'string_false_test%'", false)
            .addExpr("'string_true_test' LIKE 'string_true%'", true)
            .addExpr("'string_true_test' LIKE 'string_false%'", false)
            .addExpr("'string_true_test' LIKE 'string%test'", true)
            .addExpr("'string_true_test' LIKE '%test'", true)
            .addExpr("'string_true_test' LIKE '%string_true_test'", true)
            .addExpr("'string_true_test' LIKE '%string_false_test'", false)
            .addExpr("'string_true_test' LIKE '%false_test'", false)
            .addExpr("'string_false_test' LIKE '%false_test'", true)
            .addExpr("'string_true_test' LIKE 'string_tr_e_test'", true)
            .addExpr("'string_true_test' LIKE 'string______test'", true)
            .addExpr("'string_false_test' LIKE 'string______test'", false)
            .addExpr("'string_false_test' LIKE 'string_______test'", true)
            .addExpr("'string_false_test' LIKE 'string_false_te__'", true)
            .addExpr("'string_false_test' LIKE 'string_false_te___'", false)
            .addExpr("'string_false_test' LIKE 'string_false_te_'", false)
            .addExpr("'string_true_test' LIKE 'string_true_te__'", true)
            .addExpr("'string_true_test' LIKE '_ring_true_te__'", false)
            .addExpr("'string_false_test' LIKE '__ring_false_te__'", true)
            .addExpr("'string_true_test' LIKE '_%ring_true_te__'", true)
            .addExpr("'string_true_test' LIKE '_%tring_true_te__'", true)
            .addExpr("'string_false_test' LIKE 'string_false_te%__'", true)
            .addExpr("'string_false_test' LIKE 'string_false_te__%'", true)
            .addExpr("'string_false_test' LIKE 'string_false_t%__'", true)
            .addExpr("'string_false_test' LIKE 'string_false_t__%'", true)
            .addExpr("'string_false_test' LIKE 'string_false_te_%'", true)
            .addExpr("'string_false_test' LIKE 'string_false_te%_'", true)
            .addExpr("'string_true_test' LIKE 'string_%test'", true)
            .addExpr("'string_true_test' LIKE 'string%_test'", true)
            .addExpr("'string_true_test' LIKE 'string_%_test'", true)
            .addExpr("1 IS NOT NULL", true)
            .addExpr("true IS NOT NULL", true)
            .addExpr("1.0 IS NOT NULL", true)
            .addExpr("'a' IS NOT NULL", true)
            .addExpr("NULL IS NOT NULL", false)
            .addExpr("1 IS NULL", false)
            .addExpr("true IS NULL", false)
            .addExpr("1.0 IS NULL", false)
            .addExpr("'a' IS NULL", false)
            .addExpr("NULL IS NULL", true)
            .addExpr("true IS TRUE", true)
            .addExpr("false IS TRUE", false)
            .addExpr("true IS NOT TRUE", false)
            .addExpr("false IS NOT TRUE", true)
            .addExpr("true IS FALSE", false)
            .addExpr("false IS FALSE", true)
            .addExpr("true IS NOT FALSE", true)
            .addExpr("false IS NOT FALSE", false)
            .addExpr("3 = 5 IS NOT UNKNOWN", true)
            .addExpr("5 = 5 IS NOT UNKNOWN", true)
            .addExpr("(NOT 5 = 5) IS NOT UNKNOWN", true)
            .addExpr("(3 = NULL) IS UNKNOWN", false)
            .addExpr("(3 = NULL) IS NOT UNKNOWN", false)
            .addExpr("(NULL = NULL) IS NOT UNKNOWN", false)
            .addExpr("(NOT NULL = NULL) IS NOT UNKNOWN", false)
            .addExpr("1 IS DISTINCT FROM 2", true)
            .addExpr("1.0 IS DISTINCT FROM 2.0", true)
            .addExpr("'a' IS DISTINCT FROM 'b'", true)
            .addExpr("true IS DISTINCT FROM false", true)
            .addExpr("1 IS NOT DISTINCT FROM 2", false)
            .addExpr("1.0 IS NOT DISTINCT FROM 2.0", false)
            .addExpr("'a' IS NOT DISTINCT FROM 'b'", false)
            .addExpr("true IS NOT DISTINCT FROM false", false)
            .addExpr("date '2018-01-01' > DATE '2017-12-31' ", true)
            .addExpr("date '2018-01-01' >= DATE '2017-12-31' ", true)
            .addExpr("date '2018-01-01' < DATE '2017-12-31' ", false)
            .addExpr("date '2018-01-01' <= DATE '2017-12-31' ", false)
            .addExpr("date '2018-06-24' = DATE '2018-06-24' ", true)
            .addExpr("Date '2018-06-24' <> DATE '2018-06-24' ", false)
            .addExpr("TIME '20:17:40' < Time '15:05:57' ", false)
            .addExpr("TIME '00:00:01' >= time '00:00:01' ", true)
            .addExpr("TIMESTAMP '2017-12-31 23:59:59' < TIMESTAMP '2018-01-01 00:00:00' ", true);

    checker.buildRunAndCheck();
  }

  @Test
  @SqlOperatorTest(name = "CHARACTER_LENGTH", kind = "OTHER_FUNCTION")
  @SqlOperatorTest(name = "CHAR_LENGTH", kind = "OTHER_FUNCTION")
  @SqlOperatorTest(name = "INITCAP", kind = "OTHER_FUNCTION")
  @SqlOperatorTest(name = "LOWER", kind = "OTHER_FUNCTION")
  @SqlOperatorTest(name = "POSITION", kind = "OTHER_FUNCTION")
  @SqlOperatorTest(name = "OVERLAY", kind = "OTHER_FUNCTION")
  @SqlOperatorTest(name = "SUBSTRING", kind = "OTHER_FUNCTION")
  @SqlOperatorTest(name = "TRIM", kind = "TRIM")
  @SqlOperatorTest(name = "UPPER", kind = "OTHER_FUNCTION")
  public void testStringFunctions() throws Exception {
    SqlExpressionChecker checker =
        new SqlExpressionChecker()
            .addExpr("'hello' || ' world' = 'hello world'")
            .addExpr("CHAR_LENGTH('hello') = 5")
            .addExpr("CHARACTER_LENGTH('hello') = 5")
            .addExpr("INITCAP('hello world') = 'Hello World'")
            .addExpr("LOWER('HELLO') = 'hello'")
            .addExpr("POSITION('world' IN 'helloworld') = 6")
            .addExpr("POSITION('world' IN 'helloworldworld' FROM 7) = 11")
            .addExpr("POSITION('world' IN 'hello') = 0")
            .addExpr("TRIM(' hello ') = 'hello'")
            // https://issues.apache.org/jira/browse/BEAM-4704
            .addExpr("TRIM(LEADING 'eh' FROM 'hehe__hehe') = '__hehe'")
            .addExpr("TRIM(TRAILING 'eh' FROM 'hehe__hehe') = 'hehe__'")
            .addExpr("TRIM(BOTH 'eh' FROM 'hehe__hehe') = '__'")
            .addExpr("TRIM(BOTH ' ' FROM ' hello ') = 'hello'")
            .addExpr("OVERLAY('w3333333rce' PLACING 'resou' FROM 3) = 'w3resou3rce'")
            .addExpr("OVERLAY('w3333333rce' PLACING 'resou' FROM 3 FOR 4) = 'w3resou33rce'")
            .addExpr("OVERLAY('w3333333rce' PLACING 'resou' FROM 3 FOR 5) = 'w3resou3rce'")
            .addExpr("OVERLAY('w3333333rce' PLACING 'resou' FROM 3 FOR 7) = 'w3resouce'")
            .addExpr("SUBSTRING('hello' FROM 2) = 'ello'")
            .addExpr("SUBSTRING('hello' FROM -1) = 'o'")
            .addExpr("SUBSTRING('hello' FROM 2 FOR 2) = 'el'")
            .addExpr("SUBSTRING('hello' FROM 2 FOR 100) = 'ello'")
            .addExpr("SUBSTRING('hello' FROM -3 for 2) = 'll'")
            .addExpr("UPPER('hello') = 'HELLO'");

    checker.check(pipeline);
    pipeline.run();
  }

  @Test
  @SqlOperatorTest(name = "ARRAY", kind = "ARRAY_VALUE_CONSTRUCTOR")
  @SqlOperatorTest(name = "CARDINALITY", kind = "OTHER_FUNCTION")
  @SqlOperatorTest(name = "ELEMENT", kind = "OTHER_FUNCTION")
  public void testArrayFunctions() {
    ExpressionChecker checker =
        new ExpressionChecker()
            //            Calcite throws a parse error on this syntax for an empty array
            //            .addExpr(
            //                "ARRAY []", ImmutableList.of(),
            // Schema.FieldType.array(Schema.FieldType.BOOLEAN))
            .addExpr(
                "ARRAY ['a', 'b']",
                ImmutableList.of("a", "b"),
                Schema.FieldType.array(Schema.FieldType.STRING))
            .addExpr("CARDINALITY(ARRAY ['a', 'b', 'c'])", 3)
            .addExpr("ELEMENT(ARRAY [1])", 1);

    checker.buildRunAndCheck();
  }

  @Test
  @SqlOperatorTest(name = "DAYOFMONTH", kind = "OTHER")
  @SqlOperatorTest(name = "DAYOFWEEK", kind = "OTHER")
  @SqlOperatorTest(name = "DAYOFYEAR", kind = "OTHER")
  @SqlOperatorTest(name = "EXTRACT", kind = "EXTRACT")
  @SqlOperatorTest(name = "YEAR", kind = "OTHER")
  @SqlOperatorTest(name = "QUARTER", kind = "OTHER")
  @SqlOperatorTest(name = "MONTH", kind = "OTHER")
  @SqlOperatorTest(name = "WEEK", kind = "OTHER")
  @SqlOperatorTest(name = "HOUR", kind = "OTHER")
  @SqlOperatorTest(name = "MINUTE", kind = "OTHER")
  @SqlOperatorTest(name = "SECOND", kind = "OTHER")
  public void testBasicDateTimeFunctions() {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("EXTRACT(YEAR FROM ts)", 1986L)
            .addExpr("YEAR(ts)", 1986L)
            .addExpr("QUARTER(ts)", 1L)
            .addExpr("MONTH(ts)", 2L)
            .addExpr("WEEK(ts)", 7L)
            .addExpr("DAYOFMONTH(ts)", 15L)
            .addExpr("DAYOFYEAR(ts)", 46L)
            .addExpr("DAYOFWEEK(ts)", 7L)
            .addExpr("HOUR(ts)", 11L)
            .addExpr("MINUTE(ts)", 35L)
            .addExpr("SECOND(ts)", 26L);
    checker.buildRunAndCheck();
  }

  @Test
  // More needed @SqlOperatorTest(name = "FLOOR", kind = "FLOOR")
  // More needed @SqlOperatorTest(name = "CEIL", kind = "CEIL")
  public void testFloorAndCeil() {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("FLOOR(ts TO MONTH)", parseDate("1986-02-01 00:00:00"))
            .addExpr("FLOOR(ts TO YEAR)", parseDate("1986-01-01 00:00:00"))
            .addExpr("CEIL(ts TO MONTH)", parseDate("1986-03-01 00:00:00"))
            .addExpr("CEIL(ts TO YEAR)", parseDate("1987-01-01 00:00:00"));
    checker.buildRunAndCheck();
  }

  @Test
  @Ignore("https://issues.apache.org/jira/browse/BEAM-4622")
  public void testFloorAndCeilResolutionLimit() {
    thrown.expect(IllegalArgumentException.class);
    ExpressionChecker checker =
        new ExpressionChecker().addExpr("FLOOR(ts TO DAY)", parseDate("1986-02-01 00:00:00"));
    checker.buildRunAndCheck();
  }

  @Test
  @SqlOperatorTest(name = "TIMESTAMPADD", kind = "TIMESTAMP_ADD")
  public void testDatetimePlusFunction() {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr(
                "TIMESTAMPADD(SECOND, 3, TIMESTAMP '1984-04-19 01:02:03')",
                parseDate("1984-04-19 01:02:06"))
            .addExpr(
                "TIMESTAMPADD(MINUTE, 3, TIMESTAMP '1984-04-19 01:02:03')",
                parseDate("1984-04-19 01:05:03"))
            .addExpr(
                "TIMESTAMPADD(HOUR, 3, TIMESTAMP '1984-04-19 01:02:03')",
                parseDate("1984-04-19 04:02:03"))
            .addExpr(
                "TIMESTAMPADD(DAY, 3, TIMESTAMP '1984-04-19 01:02:03')",
                parseDate("1984-04-22 01:02:03"))
            .addExpr(
                "TIMESTAMPADD(MONTH, 2, TIMESTAMP '1984-01-19 01:02:03')",
                parseDate("1984-03-19 01:02:03"))
            .addExpr(
                "TIMESTAMPADD(YEAR, 2, TIMESTAMP '1985-01-19 01:02:03')",
                parseDate("1987-01-19 01:02:03"));
    checker.buildRunAndCheck();
  }

  @Test
  @SqlOperatorTest(name = "DATETIME_PLUS", kind = "PLUS")
  public void testDatetimeInfixPlus() {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr(
                "TIMESTAMP '1984-01-19 01:02:03' + INTERVAL '3' SECOND",
                parseDate("1984-01-19 01:02:06"))
            .addExpr(
                "TIMESTAMP '1984-01-19 01:02:03' + INTERVAL '2' MINUTE",
                parseDate("1984-01-19 01:04:03"))
            .addExpr(
                "TIMESTAMP '1984-01-19 01:02:03' + INTERVAL '2' HOUR",
                parseDate("1984-01-19 03:02:03"))
            .addExpr(
                "TIMESTAMP '1984-01-19 01:02:03' + INTERVAL '2' DAY",
                parseDate("1984-01-21 01:02:03"))
            .addExpr(
                "TIMESTAMP '1984-01-19 01:02:03' + INTERVAL '2' MONTH",
                parseDate("1984-03-19 01:02:03"))
            .addExpr(
                "TIMESTAMP '1984-01-19 01:02:03' + INTERVAL '2' YEAR",
                parseDate("1986-01-19 01:02:03"));
    checker.buildRunAndCheck();
  }

  @Test
  @SqlOperatorTest(name = "TIMESTAMPDIFF", kind = "TIMESTAMP_DIFF")
  public void testTimestampDiff() {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr(
                "TIMESTAMPDIFF(SECOND, TIMESTAMP '1984-04-19 01:01:58', "
                    + "TIMESTAMP '1984-04-19 01:01:58')",
                0)
            .addExpr(
                "TIMESTAMPDIFF(SECOND, TIMESTAMP '1984-04-19 01:01:58', "
                    + "TIMESTAMP '1984-04-19 01:01:59')",
                1)
            .addExpr(
                "TIMESTAMPDIFF(SECOND, TIMESTAMP '1984-04-19 01:01:58', "
                    + "TIMESTAMP '1984-04-19 01:02:00')",
                2)
            .addExpr(
                "TIMESTAMPDIFF(MINUTE, TIMESTAMP '1984-04-19 01:01:58', "
                    + "TIMESTAMP '1984-04-19 01:02:57')",
                0)
            .addExpr(
                "TIMESTAMPDIFF(MINUTE, TIMESTAMP '1984-04-19 01:01:58', "
                    + "TIMESTAMP '1984-04-19 01:02:58')",
                1)
            .addExpr(
                "TIMESTAMPDIFF(MINUTE, TIMESTAMP '1984-04-19 01:01:58', "
                    + "TIMESTAMP '1984-04-19 01:03:58')",
                2)
            .addExpr(
                "TIMESTAMPDIFF(HOUR, TIMESTAMP '1984-04-19 01:01:58', "
                    + "TIMESTAMP '1984-04-19 02:01:57')",
                0)
            .addExpr(
                "TIMESTAMPDIFF(HOUR, TIMESTAMP '1984-04-19 01:01:58', "
                    + "TIMESTAMP '1984-04-19 02:01:58')",
                1)
            .addExpr(
                "TIMESTAMPDIFF(HOUR, TIMESTAMP '1984-04-19 01:01:58', "
                    + "TIMESTAMP '1984-04-19 03:01:58')",
                2)
            .addExpr(
                "TIMESTAMPDIFF(DAY, TIMESTAMP '1984-04-19 01:01:58', "
                    + "TIMESTAMP '1984-04-20 01:01:57')",
                0)
            .addExpr(
                "TIMESTAMPDIFF(DAY, TIMESTAMP '1984-04-19 01:01:58', "
                    + "TIMESTAMP '1984-04-20 01:01:58')",
                1)
            .addExpr(
                "TIMESTAMPDIFF(DAY, TIMESTAMP '1984-04-19 01:01:58', "
                    + "TIMESTAMP '1984-04-21 01:01:58')",
                2)
            .addExpr(
                "TIMESTAMPDIFF(MONTH, TIMESTAMP '1984-01-19 01:01:58', "
                    + "TIMESTAMP '1984-02-19 01:01:57')",
                0)
            .addExpr(
                "TIMESTAMPDIFF(MONTH, TIMESTAMP '1984-01-19 01:01:58', "
                    + "TIMESTAMP '1984-02-19 01:01:58')",
                1)
            .addExpr(
                "TIMESTAMPDIFF(MONTH, TIMESTAMP '1984-01-19 01:01:58', "
                    + "TIMESTAMP '1984-03-19 01:01:58')",
                2)
            .addExpr(
                "TIMESTAMPDIFF(YEAR, TIMESTAMP '1981-01-19 01:01:58', "
                    + "TIMESTAMP '1982-01-19 01:01:57')",
                0)
            .addExpr(
                "TIMESTAMPDIFF(YEAR, TIMESTAMP '1981-01-19 01:01:58', "
                    + "TIMESTAMP '1982-01-19 01:01:58')",
                1)
            .addExpr(
                "TIMESTAMPDIFF(YEAR, TIMESTAMP '1981-01-19 01:01:58', "
                    + "TIMESTAMP '1983-01-19 01:01:58')",
                2)
            .addExpr(
                "TIMESTAMPDIFF(YEAR, TIMESTAMP '1981-01-19 01:01:58', "
                    + "TIMESTAMP '1980-01-19 01:01:58')",
                -1)
            .addExpr(
                "TIMESTAMPDIFF(YEAR, TIMESTAMP '1981-01-19 01:01:58', "
                    + "TIMESTAMP '1979-01-19 01:01:58')",
                -2);
    checker.buildRunAndCheck();
  }

  @Test
  // More needed @SqlOperatorTest(name = "-", kind = "MINUS")
  public void testTimestampMinusInterval() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr(
                "TIMESTAMP '1984-04-19 01:01:58' - INTERVAL '2' SECOND",
                parseDate("1984-04-19 01:01:56"))
            .addExpr(
                "TIMESTAMP '1984-04-19 01:01:58' - INTERVAL '1' MINUTE",
                parseDate("1984-04-19 01:00:58"))
            .addExpr(
                "TIMESTAMP '1984-04-19 01:01:58' - INTERVAL '4' HOUR",
                parseDate("1984-04-18 21:01:58"))
            .addExpr(
                "TIMESTAMP '1984-04-19 01:01:58' - INTERVAL '5' DAY",
                parseDate("1984-04-14 01:01:58"))
            .addExpr(
                "TIMESTAMP '1984-01-19 01:01:58' - INTERVAL '2' MONTH",
                parseDate("1983-11-19 01:01:58"))
            .addExpr(
                "TIMESTAMP '1984-01-19 01:01:58' - INTERVAL '1' YEAR",
                parseDate("1983-01-19 01:01:58"));
    checker.buildRunAndCheck();
  }
}
