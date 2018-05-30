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

/** Integration test for logical functions. */
public class BeamSqlLogicalFunctionsIntegrationTest
    extends BeamSqlBuiltinFunctionsIntegrationTestBase {
  @Test
  public void testStringFunctions() throws Exception {
    ExpressionChecker checker =
        new ExpressionChecker()
            .addExpr("c_integer = 1 AND c_bigint = 1", true)
            .addExpr("c_integer = 1 OR c_bigint = 2", true)
            .addExpr("NOT c_bigint = 2", true)
            .addExpr("(NOT c_bigint = 2) AND (c_integer = 1 OR c_bigint = 3)", true)
            .addExpr("c_integer = 2 AND c_bigint = 1", false)
            .addExpr("c_integer = 2 OR c_bigint = 2", false)
            .addExpr("NOT c_bigint = 1", false)
            .addExpr("(NOT c_bigint = 2) AND (c_integer = 2 OR c_bigint = 3)", false);

    checker.buildRunAndCheck();
  }
}
