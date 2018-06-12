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

import org.apache.beam.sdk.extensions.sql.integrationtest.BeamSqlBuiltinFunctionsIntegrationTestBase;
import org.junit.Test;

/** Integration test for string functions. */
public class BeamSqlDslStringOperatorsTest extends BeamSqlBuiltinFunctionsIntegrationTestBase {
  @Test
  public void testStringFunctions() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("'hello' || ' world'", "hello world")
            .addExpr("CHAR_LENGTH('hello')", 5)
            .addExpr("CHARACTER_LENGTH('hello')", 5)
            .addExpr("INITCAP('hello world')", "Hello World")
            .addExpr("LOWER('HELLO')", "hello")
            .addExpr("POSITION('world' IN 'helloworld')", 6)
            .addExpr("POSITION('world' IN 'helloworldworld' FROM 7)", 11)
            .addExpr("POSITION('world' IN 'hello')", 0)
            .addExpr("TRIM(' hello ')", "hello")
            .addExpr("TRIM(LEADING 'eh' FROM 'hehe__hehe')", "__hehe")
            .addExpr("TRIM(TRAILING 'eh' FROM 'hehe__hehe')", "hehe__")
            .addExpr("TRIM(BOTH 'eh' FROM 'hehe__hehe')", "__")
            .addExpr("TRIM(BOTH ' ' FROM ' hello ')", "hello")
            .addExpr("OVERLAY('w3333333rce' PLACING 'resou' FROM 3)", "w3resou3rce")
            .addExpr("OVERLAY('w3333333rce' PLACING 'resou' FROM 3 FOR 4)", "w3resou33rce")
            .addExpr("OVERLAY('w3333333rce' PLACING 'resou' FROM 3 FOR 5)", "w3resou3rce")
            .addExpr("OVERLAY('w3333333rce' PLACING 'resou' FROM 3 FOR 7)", "w3resouce")
            .addExpr("SUBSTRING('hello' FROM 2)", "ello")
            .addExpr("SUBSTRING('hello' FROM -1)", "o")
            .addExpr("SUBSTRING('hello' FROM 2 FOR 2)", "el")
            .addExpr("SUBSTRING('hello' FROM 2 FOR 100)", "ello")
            .addExpr("SUBSTRING('hello' FROM -3 for 2)", "ll")
            .addExpr("UPPER('hello')", "HELLO");

    checker.buildRunAndCheck();
  }
}
