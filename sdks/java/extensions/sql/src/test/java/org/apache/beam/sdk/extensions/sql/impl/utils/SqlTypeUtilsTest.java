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
package org.apache.beam.sdk.extensions.sql.impl.utils;

import static org.apache.beam.sdk.extensions.sql.impl.utils.SqlTypeUtils.findExpressionOfType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.base.Optional;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlExpression;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;

/** Tests for {@link SqlTypeUtils}. */
public class SqlTypeUtilsTest {
  private static final BigDecimal DECIMAL_THREE = new BigDecimal(3);
  private static final BigDecimal DECIMAL_FOUR = new BigDecimal(4);

  private static final List<BeamSqlExpression> EXPRESSIONS =
      Arrays.asList(
          BeamSqlPrimitive.of(SqlTypeName.INTERVAL_DAY, DECIMAL_THREE),
          BeamSqlPrimitive.of(SqlTypeName.INTERVAL_MONTH, DECIMAL_FOUR),
          BeamSqlPrimitive.of(SqlTypeName.INTEGER, 4),
          BeamSqlPrimitive.of(SqlTypeName.INTEGER, 5));

  @Test
  public void testFindExpressionOfType_success() {
    Optional<BeamSqlExpression> typeName = findExpressionOfType(EXPRESSIONS, SqlTypeName.INTEGER);

    assertTrue(typeName.isPresent());
    assertEquals(SqlTypeName.INTEGER, typeName.get().getOutputType());
  }

  @Test
  public void testFindExpressionOfType_failure() {
    Optional<BeamSqlExpression> typeName = findExpressionOfType(EXPRESSIONS, SqlTypeName.VARCHAR);

    assertFalse(typeName.isPresent());
  }

  @Test
  public void testFindExpressionOfTypes_success() {
    Optional<BeamSqlExpression> typeName = findExpressionOfType(EXPRESSIONS, SqlTypeName.INT_TYPES);

    assertTrue(typeName.isPresent());
    assertEquals(SqlTypeName.INTEGER, typeName.get().getOutputType());
  }

  @Test
  public void testFindExpressionOfTypes_failure() {
    Optional<BeamSqlExpression> typeName =
        findExpressionOfType(EXPRESSIONS, SqlTypeName.CHAR_TYPES);

    assertFalse(typeName.isPresent());
  }
}
