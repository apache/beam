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

package org.apache.beam.dsls.sql.integrationtest;

import org.junit.Test;

/**
 * Integration test for comparison operators.
 */
public class BeamSqlComparisonOperatorsIntegrationTest
    extends BeamSqlBuiltinFunctionsIntegrationTestBase {
  @Test
  public void testComparisionOperators() throws Exception {
    ExpressionChecker checker = new ExpressionChecker()
        .addExpr("1 = 1", true)
        .addExpr("1 = 2", false)

        .addExpr("1 IS NOT NULL", true)
        .addExpr("NULL IS NOT NULL", false)

        .addExpr("1 IS NULL", false)
        .addExpr("NULL IS NULL", true)

        .addExpr("2 >= 1", true)
        .addExpr("1 >= 1", true)
        .addExpr("1 >= 2", false)

        .addExpr("2 > 1", true)
        .addExpr("1 > 1", false)
        .addExpr("1 > 2", false)

        .addExpr("2 <= 1", false)
        .addExpr("1 <= 1", true)
        .addExpr("1 <= 2", true)

        .addExpr("2 < 1", false)
        .addExpr("1 < 1", false)
        .addExpr("1 < 2", true)

        .addExpr("1 <> 1", false)
        .addExpr("1 <> 2", true)
        ;

    checker.buildRunAndCheck();
  }

}
