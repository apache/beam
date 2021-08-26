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
package org.apache.beam.sdk.extensions.sql.impl.udf;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.beam.sdk.extensions.sql.integrationtest.BeamSqlBuiltinFunctionsIntegrationTestBase;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for UDFs. */
@RunWith(JUnit4.class)
public class BeamSqlUdfExpressionTest extends BeamSqlBuiltinFunctionsIntegrationTestBase {

  @Test
  public void testCOSH() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("COSH(CAST(1.0 as DOUBLE))", Math.cosh(1.0))
            .addExpr("COSH(CAST(710.0 as DOUBLE))", Math.cosh(710.0))
            .addExpr("COSH(CAST(-1.0 as DOUBLE))", Math.cosh(-1.0))
            .addExprWithNullExpectedValue("COSH(CAST(NULL as DOUBLE))", TypeName.DOUBLE);

    checker.buildRunAndCheck();
  }

  @Test
  public void testSINH() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("SINH(CAST(1.0 as DOUBLE))", Math.sinh(1.0))
            .addExpr("SINH(CAST(710.0 as DOUBLE))", Math.sinh(710.0))
            .addExpr("SINH(CAST(-1.0 as DOUBLE))", Math.sinh(-1.0))
            .addExprWithNullExpectedValue("SINH(CAST(NULL as DOUBLE))", TypeName.DOUBLE);

    checker.buildRunAndCheck();
  }

  @Test
  public void testTANH() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("TANH(CAST(1.0 as DOUBLE))", Math.tanh(1.0))
            .addExpr("TANH(CAST(0.0 as DOUBLE))", Math.tanh(0.0))
            .addExpr("TANH(CAST(-1.0 as DOUBLE))", Math.tanh(-1.0))
            .addExprWithNullExpectedValue("TANH(CAST(NULL as DOUBLE))", TypeName.DOUBLE);

    checker.buildRunAndCheck();
  }

  @Test
  public void testEndsWith() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("ENDS_WITH('string1', 'g1')", true)
            .addExpr("ENDS_WITH('string2', 'g1')", false)
            .addExpr("ENDS_WITH('', '')", true)
            .addExpr("ENDS_WITH('中文', '文')", true)
            .addExpr("ENDS_WITH('中文', '中')", false);

    checker.buildRunAndCheck();
  }

  @Test
  public void testStartsWith() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("STARTS_WITH('string1', 'stri')", true)
            .addExpr("STARTS_WITH('string2', 'str1')", false)
            .addExpr("STARTS_WITH('', '')", true)
            .addExpr("STARTS_WITH('中文', '文')", false)
            .addExpr("STARTS_WITH('中文', '中')", true);

    checker.buildRunAndCheck();
  }

  @Test
  public void testLength() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("LENGTH('')", 0L)
            .addExpr("LENGTH('abcde')", 5L)
            .addExpr("LENGTH('中文')", 2L)
            .addExpr("LENGTH('\0\0')", 2L)
            .addExpr("LENGTH('абвгд')", 5L)
            .addExprWithNullExpectedValue("LENGTH(CAST(NULL as VARCHAR(0)))", TypeName.INT64)
            .addExprWithNullExpectedValue("LENGTH(CAST(NULL as VARBINARY(0)))", TypeName.INT64);

    checker.buildRunAndCheck();
  }

  @Test
  public void testReverse() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("REVERSE('')", "")
            .addExpr("REVERSE('foo')", "oof")
            .addExpr("REVERSE('中文')", "文中")
            .addExpr("REVERSE('абвгд')", "дгвба")
            .addExprWithNullExpectedValue("REVERSE(CAST(NULL as VARCHAR(0)))", TypeName.STRING)
            .addExprWithNullExpectedValue("REVERSE(CAST(NULL as VARBINARY(0)))", TypeName.STRING);

    checker.buildRunAndCheck();
  }

  @Test
  public void testFromHex() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("FROM_HEX('666f6f626172')", "foobar".getBytes(UTF_8))
            .addExpr("FROM_HEX('20')", " ".getBytes(UTF_8))
            .addExpr("FROM_HEX('616263414243')", "abcABC".getBytes(UTF_8))
            .addExpr(
                "FROM_HEX('616263414243d0b6d189d184d096d0a9d0a4')", "abcABCжщфЖЩФ".getBytes(UTF_8))
            .addExprWithNullExpectedValue("FROM_HEX(CAST(NULL as VARCHAR(0)))", TypeName.BYTES);
    checker.buildRunAndCheck();
  }

  @Test
  public void testToHex() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExprWithNullExpectedValue("TO_HEX(CAST(NULL as VARBINARY(0)))", TypeName.STRING);

    checker.buildRunAndCheck();
  }

  @Test
  public void testLeftPad() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("LPAD('abcdef', CAST(0 AS BIGINT))", "")
            .addExpr("LPAD('abcdef', CAST(0 AS BIGINT), 'defgh')", "")
            .addExpr("LPAD('abcdef', CAST(6 AS BIGINT), 'defgh')", "abcdef")
            .addExpr("LPAD('abcdef', CAST(5 AS BIGINT), 'defgh')", "abcde")
            .addExpr("LPAD('abcdef', CAST(4 AS BIGINT), 'defgh')", "abcd")
            .addExpr("LPAD('abcdef', CAST(3 AS BIGINT), 'defgh')", "abc")
            .addExpr("LPAD('abc', CAST(4 AS BIGINT), 'defg')", "dabc")
            .addExpr("LPAD('abc', CAST(5 AS BIGINT), 'defgh')", "deabc")
            .addExpr("LPAD('abc', CAST(6 AS BIGINT), 'defgh')", "defabc")
            .addExpr("LPAD('abc', CAST(7 AS BIGINT), 'defg')", "defgabc")
            .addExpr("LPAD('abcd', CAST(10 AS BIGINT), 'defg')", "defgdeabcd")
            .addExpr("LPAD('中文', CAST(10 AS BIGINT), 'жщфЖЩФ')", "жщфЖЩФжщ中文")
            .addExpr("LPAD('', CAST(5 AS BIGINT), ' ')", "     ")
            .addExpr("LPAD('', CAST(3 AS BIGINT), '-')", "---")
            .addExpr("LPAD('a', CAST(5 AS BIGINT), ' ')", "    a")
            .addExpr("LPAD('a', CAST(3 AS BIGINT), '-')", "--a")
            .addExprWithNullExpectedValue(
                "LPAD(CAST(NULL AS VARCHAR(0)), CAST(3 AS BIGINT), '-')", TypeName.STRING)
            .addExprWithNullExpectedValue("LPAD('', CAST(NULL AS BIGINT), '-')", TypeName.STRING)
            .addExprWithNullExpectedValue(
                "LPAD('', CAST(3 AS BIGINT), CAST(NULL AS VARCHAR(0)))", TypeName.STRING);

    checker.buildRunAndCheck();
  }

  @Test
  public void testRightPad() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("RPAD('abcdef', CAST(0 AS BIGINT))", "")
            .addExpr("RPAD('abcdef', CAST(0 AS BIGINT), 'defgh')", "")
            .addExpr("RPAD('abcdef', CAST(6 AS BIGINT), 'defgh')", "abcdef")
            .addExpr("RPAD('abcdef', CAST(5 AS BIGINT), 'defgh')", "abcde")
            .addExpr("RPAD('abcdef', CAST(4 AS BIGINT), 'defgh')", "abcd")
            .addExpr("RPAD('abcdef', CAST(3 AS BIGINT), 'defgh')", "abc")
            .addExpr("RPAD('abc', CAST(4 AS BIGINT), 'defg')", "abcd")
            .addExpr("RPAD('abc', CAST(5 AS BIGINT), 'defgh')", "abcde")
            .addExpr("RPAD('abc', CAST(6 AS BIGINT), 'defgh')", "abcdef")
            .addExpr("RPAD('abc', CAST(7 AS BIGINT), 'defg')", "abcdefg")
            .addExpr("RPAD('abcd', CAST(10 AS BIGINT), 'defg')", "abcddefgde")
            .addExpr("RPAD('中文', CAST(10 AS BIGINT), 'жщфЖЩФ')", "中文жщфЖЩФжщ")
            .addExpr("RPAD('', CAST(5 AS BIGINT), ' ')", "     ")
            .addExpr("RPAD('', CAST(3 AS BIGINT), '-')", "---")
            .addExpr("RPAD('a', CAST(5 AS BIGINT), ' ')", "a    ")
            .addExpr("RPAD('a', CAST(3 AS BIGINT), '-')", "a--")
            .addExprWithNullExpectedValue(
                "RPAD(CAST(NULL AS VARCHAR(0)), CAST(3 AS BIGINT), '-')", TypeName.STRING)
            .addExprWithNullExpectedValue("RPAD('', CAST(NULL AS BIGINT), '-')", TypeName.STRING)
            .addExprWithNullExpectedValue(
                "RPAD('', CAST(3 AS BIGINT), CAST(NULL AS VARCHAR(0)))", TypeName.STRING);

    checker.buildRunAndCheck();
  }

  @Test
  public void testMd5() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("MD5('foobar')", DigestUtils.md5("foobar"))
            .addExpr("MD5('中文')", DigestUtils.md5("中文"))
            .addExprWithNullExpectedValue("MD5(CAST(NULL AS VARCHAR(0)))", TypeName.BYTES);
    checker.buildRunAndCheck();
  }

  @Test
  public void testSHA1() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("SHA1('foobar')", DigestUtils.sha1("foobar"))
            .addExpr("SHA1('中文')", DigestUtils.sha1("中文"))
            .addExprWithNullExpectedValue("SHA1(CAST(NULL AS VARCHAR(0)))", TypeName.BYTES);
    checker.buildRunAndCheck();
  }

  @Test
  public void testSHA256() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("SHA256('foobar')", DigestUtils.sha256("foobar"))
            .addExpr("SHA256('中文')", DigestUtils.sha256("中文"))
            .addExprWithNullExpectedValue("SHA256(CAST(NULL AS VARCHAR(0)))", TypeName.BYTES);
    checker.buildRunAndCheck();
  }

  @Test
  public void testSHA512() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("SHA512('foobar')", DigestUtils.sha512("foobar"))
            .addExpr("SHA512('中文')", DigestUtils.sha512("中文"))
            .addExprWithNullExpectedValue("SHA512(CAST(NULL AS VARCHAR(0)))", TypeName.BYTES);
    checker.buildRunAndCheck();
  }
}
