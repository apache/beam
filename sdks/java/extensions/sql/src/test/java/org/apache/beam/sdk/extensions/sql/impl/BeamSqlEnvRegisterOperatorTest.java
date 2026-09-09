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
package org.apache.beam.sdk.extensions.sql.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.extensions.sql.impl.rel.BaseRelTest;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlFunction;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlIdentifier;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlKind;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlOperator;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.SqlSyntax;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.type.OperandTypes;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.type.ReturnTypes;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.beam.vendor.calcite.v1_40_0.org.apache.calcite.sql.validate.SqlNameMatchers;
import org.junit.Test;

/**
 * Tests for {@link BeamSqlEnv#registerSqlOperator(SqlOperator)} and the session-scoped operator
 * table in {@link JdbcConnection}.
 */
public class BeamSqlEnvRegisterOperatorTest extends BaseRelTest {

  /** A simple custom function for testing, not present in the default operator table. */
  private static final String CUSTOM_FUNCTION_NAME = "BEAM_TEST_CUSTOM_FUNC";

  private static SqlFunction createCustomFunction() {
    return new SqlFunction(
        CUSTOM_FUNCTION_NAME,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER,
        null,
        OperandTypes.NUMERIC,
        SqlFunctionCategory.USER_DEFINED_FUNCTION);
  }

  @Test
  public void testExtraOperatorTableStartsEmpty() {
    // A fresh env should have an empty extra operator table
    BeamSqlEnv freshEnv = BeamSqlEnv.readOnly("empty_test", new java.util.HashMap<>());
    assertTrue(
        "Extra operator table should be empty on a fresh env",
        freshEnv.connection.getExtraOperatorTable().getOperatorList().isEmpty());
  }

  @Test
  public void testRegisterSqlOperator_addsToExtraOperatorTable() {
    SqlFunction customFunc = createCustomFunction();
    env.registerSqlOperator(customFunc);

    // Verify the operator is in the extra operator table
    assertTrue(
        "Registered operator should be present in the extra operator table",
        env.connection.getExtraOperatorTable().getOperatorList().contains(customFunc));
    assertEquals(1, env.connection.getExtraOperatorTable().getOperatorList().size());
  }

  @Test
  public void testRegisterSqlOperator_makesOperatorResolvableByName() {
    SqlFunction customFunc = createCustomFunction();
    env.registerSqlOperator(customFunc);

    // Verify the operator is resolvable by name in the extra operator table
    SqlNameMatcher nameMatcher = SqlNameMatchers.withCaseSensitive(false);
    java.util.List<SqlOperator> resolvedOps = new java.util.ArrayList<>();
    env.connection
        .getExtraOperatorTable()
        .lookupOperatorOverloads(
            new SqlIdentifier(CUSTOM_FUNCTION_NAME, SqlParserPos.ZERO),
            SqlFunctionCategory.USER_DEFINED_FUNCTION,
            SqlSyntax.FUNCTION,
            resolvedOps,
            nameMatcher);

    assertFalse(
        "Custom operator should be resolvable by name after registration", resolvedOps.isEmpty());
    assertTrue(
        "The resolved operator should be the one we registered", resolvedOps.contains(customFunc));
  }
}
