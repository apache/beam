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
package org.apache.beam.sdk.extensions.sql.integrationtest;

import org.junit.Test;

/** Integration test for string functions. */
public class BeamSqlStringFunctionsIntegrationTest
    extends BeamSqlBuiltinFunctionsIntegrationTestBase {
  @Test
  public void testStringFunctions() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("'hello' || ' world'", "hello world")
            .addExpr("CHAR_LENGTH('hello')", 5)
            .addExpr("CHARACTER_LENGTH('hello')", 5)
            .addExpr("UPPER('hello')", "HELLO")
            .addExpr("LOWER('HELLO')", "hello")
            .addExpr("POSITION('world' IN 'helloworld')", 5)
            .addExpr("POSITION('world' IN 'helloworldworld' FROM 7)", 10)
            .addExpr("TRIM(' hello ')", "hello")
            .addExpr("TRIM(LEADING ' ' FROM ' hello ')", "hello ")
            .addExpr("TRIM(TRAILING ' ' FROM ' hello ')", " hello")
            .addExpr("TRIM(BOTH ' ' FROM ' hello ')", "hello")
            .addExpr("OVERLAY('w3333333rce' PLACING 'resou' FROM 3)", "w3resou3rce")
            .addExpr("SUBSTRING('hello' FROM 2)", "ello")
            .addExpr("SUBSTRING('hello' FROM 2 FOR 2)", "el")
            .addExpr("INITCAP('hello world')", "Hello World");

    checker.buildRunAndCheck();
  }
}
