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

/** Integration test for conditional functions. */
public class BeamSqlConditionalFunctionsIntegrationTest
    extends BeamSqlBuiltinFunctionsIntegrationTestBase {
  @Test
  public void testConditionalFunctions() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("CASE 1 WHEN 1 THEN 'hello' ELSE 'world' END", "hello")
            .addExpr(
                "CASE 2 " + "WHEN 1 THEN 'hello' " + "WHEN 3 THEN 'bond' " + "ELSE 'world' END",
                "world")
            .addExpr("CASE " + "WHEN 1 = 1 THEN 'hello' " + "ELSE 'world' END", "hello")
            .addExpr("CASE " + "WHEN 1 > 1 THEN 'hello' " + "ELSE 'world' END", "world")
            .addExpr("NULLIF(5, 4) ", 5)
            .addExpr("COALESCE(1, 5) ", 1)
            .addExpr("COALESCE(NULL, 5) ", 5);

    checker.buildRunAndCheck();
  }
}
