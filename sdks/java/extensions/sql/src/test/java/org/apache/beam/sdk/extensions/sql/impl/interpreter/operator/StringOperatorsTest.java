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

package org.apache.beam.sdk.extensions.sql.impl.interpreter.operator;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.BeamSqlFnExecutorTestBase;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test of BeamSqlUpperExpression. */
@RunWith(Enclosed.class)
public class StringOperatorsTest extends BeamSqlFnExecutorTestBase {

  /** Tests for UPPER. */
  @RunWith(JUnit4.class)
  public static class UpperTest {
    @Test
    public void testUpper() {
      assertThat(
          StringOperators.UPPER
              .apply(ImmutableList.of(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hello")))
              .getValue(),
          equalTo("HELLO"));
    }
  }

  /** Tests for LOWER. */
  @RunWith(JUnit4.class)
  public static class LowerTest {
    @Test
    public void testLower() {
      assertThat(
          StringOperators.LOWER
              .apply(ImmutableList.of(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "HELLo")))
              .getValue(),
          equalTo("hello"));
    }
  }

  /** Tests for TRIM. */
  @RunWith(JUnit4.class)
  public static class TrimTest {

    @Test
    public void testAcceptOne() {
      assertTrue(
          StringOperators.TRIM.accept(
              ImmutableList.of(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, " hello "))));
    }

    @Test
    public void testAcceptThree() {
      assertTrue(
          StringOperators.TRIM.accept(
              ImmutableList.of(
                  BeamSqlPrimitive.of(SqlTypeName.SYMBOL, SqlTrimFunction.Flag.BOTH),
                  BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "he"),
                  BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hehe__hehe"))));
    }

    @Test
    public void testRejectTwo() {
      assertFalse(
          StringOperators.TRIM.accept(
              ImmutableList.of(
                  BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "he"),
                  BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hehe__hehe"))));
    }

    @Test
    public void testLeading() throws Exception {
      assertThat(
          StringOperators.TRIM
              .apply(
                  ImmutableList.of(
                      BeamSqlPrimitive.of(SqlTypeName.SYMBOL, SqlTrimFunction.Flag.LEADING),
                      BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "eh"),
                      BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hehe__hehe")))
              .getValue(),
          equalTo("__hehe"));
    }

    @Test
    public void testTrailing() {
      assertThat(
          StringOperators.TRIM
              .apply(
                  ImmutableList.of(
                      BeamSqlPrimitive.of(SqlTypeName.SYMBOL, SqlTrimFunction.Flag.TRAILING),
                      BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "eh"),
                      BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hehe__hehe")))
              .getValue(),
          equalTo("hehe__"));
    }

    @Test
    public void testBoth() {
      assertThat(
          StringOperators.TRIM
              .apply(
                  ImmutableList.of(
                      BeamSqlPrimitive.of(SqlTypeName.SYMBOL, SqlTrimFunction.Flag.BOTH),
                      BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "eh"),
                      BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hehe__hehe")))
              .getValue(),
          equalTo("__"));
    }

    @Test
    public void testDefault() {
      assertThat(
          StringOperators.TRIM
              .apply(ImmutableList.of(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, " hello ")))
              .getValue(),
          equalTo("hello"));
    }
  }

  /** Tests for CHAR_LENGTH. */
  @RunWith(JUnit4.class)
  public static class CharLengthTest {

    @Test
    public void testSimple() {
      assertThat(
          StringOperators.CHAR_LENGTH
              .apply(ImmutableList.of(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hello")))
              .getValue(),
          equalTo(5));
    }

    @Test
    public void testAccept() {
      assertTrue(
          StringOperators.CHAR_LENGTH.accept(
              ImmutableList.of(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hello"))));
    }

    @Test
    public void testRejectNonString() {
      assertFalse(
          StringOperators.CHAR_LENGTH.accept(
              ImmutableList.of(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 1))));
    }

    @Test
    public void testRejectTooManyArgs() {
      assertFalse(
          StringOperators.CHAR_LENGTH.accept(
              ImmutableList.of(
                  BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hello"),
                  BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hello"))));
    }
  }

  /** Tests for Concat operator. */
  @RunWith(JUnit4.class)
  public static class BeamSqlConcatExpressionTest extends BeamSqlFnExecutorTestBase {

    @Test
    public void accept() throws Exception {
      assertTrue(
          StringOperators.CONCAT.accept(
              ImmutableList.of(
                  BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hello"),
                  BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "world"))));
    }

    @Test
    public void rejectNonString() throws Exception {
      assertFalse(
          StringOperators.CONCAT.accept(
              ImmutableList.of(
                  BeamSqlPrimitive.of(SqlTypeName.INTEGER, 1),
                  BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hello"))));
    }

    @Test
    public void rejectTooMany() throws Exception {
      assertFalse(
          StringOperators.CONCAT.accept(
              ImmutableList.of(
                  BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hello"),
                  BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "world"),
                  BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hello"))));
    }

    @Test
    public void testApply() throws Exception {
      assertThat(
          StringOperators.CONCAT
              .apply(
                  ImmutableList.of(
                      BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hello"),
                      BeamSqlPrimitive.of(SqlTypeName.VARCHAR, " world")))
              .getValue(),
          equalTo("hello world"));
    }
  }

  /** Test for SUBSTRING. */
  public static class SubstringTest {

    @Test
    public void testAcceptTwoArgs() throws Exception {
      assertTrue(
          StringOperators.SUBSTRING.accept(
              ImmutableList.of(
                  BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hello"),
                  BeamSqlPrimitive.of(SqlTypeName.INTEGER, 1))));
    }

    @Test
    public void testAcceptThreeArgs() {
      assertTrue(
          StringOperators.SUBSTRING.accept(
              ImmutableList.of(
                  BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hello"),
                  BeamSqlPrimitive.of(SqlTypeName.INTEGER, 1),
                  BeamSqlPrimitive.of(SqlTypeName.INTEGER, 2))));
    }

    @Test
    public void testApplyWhole() {
      assertThat(
          StringOperators.SUBSTRING
              .apply(
                  ImmutableList.of(
                      BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hello"),
                      BeamSqlPrimitive.of(SqlTypeName.INTEGER, 1)))
              .getValue(),
          equalTo("hello"));
    }

    @Test
    public void testApplySubstring() {
      assertThat(
          StringOperators.SUBSTRING
              .apply(
                  ImmutableList.of(
                      BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hello"),
                      BeamSqlPrimitive.of(SqlTypeName.INTEGER, 1),
                      BeamSqlPrimitive.of(SqlTypeName.INTEGER, 2)))
              .getValue(),
          equalTo("he"));
    }

    @Test
    public void testApplyExactLength() {
      assertThat(
          StringOperators.SUBSTRING
              .apply(
                  ImmutableList.of(
                      BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hello"),
                      BeamSqlPrimitive.of(SqlTypeName.INTEGER, 1),
                      BeamSqlPrimitive.of(SqlTypeName.INTEGER, 5)))
              .getValue(),
          equalTo("hello"));
    }

    @Test
    public void testApplyExceedsLength() {
      assertThat(
          StringOperators.SUBSTRING
              .apply(
                  ImmutableList.of(
                      BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hello"),
                      BeamSqlPrimitive.of(SqlTypeName.INTEGER, 1),
                      BeamSqlPrimitive.of(SqlTypeName.INTEGER, 100)))
              .getValue(),
          equalTo("hello"));
    }

    @Test
    public void testApplyNegativeLength() {
      assertThat(
          StringOperators.SUBSTRING
              .apply(
                  ImmutableList.of(
                      BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hello"),
                      BeamSqlPrimitive.of(SqlTypeName.INTEGER, 1),
                      BeamSqlPrimitive.of(SqlTypeName.INTEGER, 0)))
              .getValue(),
          equalTo(""));
    }

    @Test
    public void testApplyNegativeStartpoint() {
      assertThat(
          StringOperators.SUBSTRING
              .apply(
                  ImmutableList.of(
                      BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hello"),
                      BeamSqlPrimitive.of(SqlTypeName.INTEGER, -1)))
              .getValue(),
          equalTo("o"));
    }
  }

  /** Test for POSITION. */
  public static class PositionTest {
    @Test
    public void testAcceptTwoArgs() throws Exception {
      assertTrue(
          StringOperators.POSITION.accept(
              ImmutableList.of(
                  BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hello"),
                  BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "worldhello"))));
    }

    @Test
    public void testAcceptTrheeArgs() throws Exception {
      assertTrue(
          StringOperators.POSITION.accept(
              ImmutableList.of(
                  BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hello"),
                  BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "worldhello"),
                  BeamSqlPrimitive.of(SqlTypeName.INTEGER, 1))));
    }

    @Test
    public void testReject() {
      assertFalse(
          StringOperators.POSITION.accept(
              ImmutableList.of(
                  BeamSqlPrimitive.of(SqlTypeName.INTEGER, 1),
                  BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "worldhello"))));
    }

    @Test
    public void testRejectTwoMany() {
      assertFalse(
          StringOperators.POSITION.accept(
              ImmutableList.of(
                  BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hello"),
                  BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "worldhello"),
                  BeamSqlPrimitive.of(SqlTypeName.INTEGER, 1),
                  BeamSqlPrimitive.of(SqlTypeName.INTEGER, 1))));
    }

    @Test
    public void testBasic() {
      assertThat(
          StringOperators.POSITION
              .apply(
                  ImmutableList.of(
                      BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hello"),
                      BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "worldhello")))
              .getValue(),
          equalTo(6));
    }

    @Test
    public void testThreeArgs() {
      assertThat(
          StringOperators.POSITION
              .apply(
                  ImmutableList.of(
                      BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hello"),
                      BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "worldhello"),
                      BeamSqlPrimitive.of(SqlTypeName.INTEGER, 1)))
              .getValue(),
          equalTo(6));
    }

    @Test
    public void testThreeArgsNotFound() {
      assertThat(
          StringOperators.POSITION
              .apply(
                  ImmutableList.of(
                      BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hello"),
                      BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "world"),
                      BeamSqlPrimitive.of(SqlTypeName.INTEGER, 1)))
              .getValue(),
          equalTo(0));
    }
  }

  /** Test for BeamSqlOverlayExpression. */
  public static class OverlayTest {

    @Test
    public void acceptThreeArgs() {
      assertTrue(
          StringOperators.OVERLAY.accept(
              ImmutableList.of(
                  BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hello"),
                  BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hello"),
                  BeamSqlPrimitive.of(SqlTypeName.INTEGER, 1))));
    }

    @Test
    public void acceptFourArgs() {
      assertTrue(
          StringOperators.OVERLAY.accept(
              ImmutableList.of(
                  BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hello"),
                  BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hello"),
                  BeamSqlPrimitive.of(SqlTypeName.INTEGER, 1),
                  BeamSqlPrimitive.of(SqlTypeName.INTEGER, 2))));
    }

    @Test
    public void testOverlayBasic() {
      assertThat(
          StringOperators.OVERLAY
              .apply(
                  ImmutableList.of(
                      BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "w3333333rce"),
                      BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "resou"),
                      BeamSqlPrimitive.of(SqlTypeName.INTEGER, 3)))
              .getValue(),
          equalTo("w3resou3rce"));
    }

    @Test
    public void testOverlayFourArgs() {
      assertThat(
          StringOperators.OVERLAY
              .apply(
                  ImmutableList.of(
                      BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "w3333333rce"),
                      BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "resou"),
                      BeamSqlPrimitive.of(SqlTypeName.INTEGER, 3),
                      BeamSqlPrimitive.of(SqlTypeName.INTEGER, 4)))
              .getValue(),
          equalTo("w3resou33rce"));
    }

    @Test
    public void testOverlayFourArgs2() {
      assertThat(
          StringOperators.OVERLAY
              .apply(
                  ImmutableList.of(
                      BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "w3333333rce"),
                      BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "resou"),
                      BeamSqlPrimitive.of(SqlTypeName.INTEGER, 3),
                      BeamSqlPrimitive.of(SqlTypeName.INTEGER, 5)))
              .getValue(),
          equalTo("w3resou3rce"));
    }

    @Test
    public void testOverlayBigGap() {
      assertThat(
          StringOperators.OVERLAY
              .apply(
                  ImmutableList.of(
                      BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "w3333333rce"),
                      BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "resou"),
                      BeamSqlPrimitive.of(SqlTypeName.INTEGER, 3),
                      BeamSqlPrimitive.of(SqlTypeName.INTEGER, 7)))
              .getValue(),
          equalTo("w3resouce"));
    }
  }

  /** Test of BeamSqlInitCapExpression. */
  @RunWith(JUnit4.class)
  public static class InitCapTest {

    @Test
    public void testTwoWords() {
      assertThat(
          StringOperators.INIT_CAP
              .apply(ImmutableList.of(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hello world")))
              .getValue(),
          equalTo("Hello World"));
    }

    @Test
    public void testTwoWordsWonky() {
      assertThat(
          StringOperators.INIT_CAP
              .apply(ImmutableList.of(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hEllO wOrld")))
              .getValue(),
          equalTo("Hello World"));
    }

    @Test
    public void testTwoWordsSpacedOut() {
      assertThat(
          StringOperators.INIT_CAP
              .apply(ImmutableList.of(BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hello     world")))
              .getValue(),
          equalTo("Hello     World"));
    }
  }
}
