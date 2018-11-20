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
package org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.reinterpret;

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;

/** Unit tests for {@link IntegerReinterpretConversions}. */
public class IntegerReinterpretConversionsTest {

  private static final BeamSqlPrimitive TINYINT_PRIMITIVE_5 =
      BeamSqlPrimitive.of(SqlTypeName.TINYINT, (byte) 5);

  private static final BeamSqlPrimitive SMALLINT_PRIMITIVE_6 =
      BeamSqlPrimitive.of(SqlTypeName.SMALLINT, (short) 6);

  private static final BeamSqlPrimitive INTEGER_PRIMITIVE_8 =
      BeamSqlPrimitive.of(SqlTypeName.INTEGER, 8);

  private static final BeamSqlPrimitive BIGINT_PRIMITIVE_15 =
      BeamSqlPrimitive.of(SqlTypeName.BIGINT, 15L);

  @Test
  public void testTinyIntToBigint() {
    BeamSqlPrimitive conversionResultPrimitive =
        IntegerReinterpretConversions.INTEGER_TYPES_TO_BIGINT.convert(TINYINT_PRIMITIVE_5);

    assertEquals(SqlTypeName.BIGINT, conversionResultPrimitive.getOutputType());
    assertEquals(5L, conversionResultPrimitive.getLong());
  }

  @Test
  public void testSmallIntToBigint() {
    BeamSqlPrimitive conversionResultPrimitive =
        IntegerReinterpretConversions.INTEGER_TYPES_TO_BIGINT.convert(SMALLINT_PRIMITIVE_6);

    assertEquals(SqlTypeName.BIGINT, conversionResultPrimitive.getOutputType());
    assertEquals(6L, conversionResultPrimitive.getLong());
  }

  @Test
  public void testIntegerToBigint() {
    BeamSqlPrimitive conversionResultPrimitive =
        IntegerReinterpretConversions.INTEGER_TYPES_TO_BIGINT.convert(INTEGER_PRIMITIVE_8);

    assertEquals(SqlTypeName.BIGINT, conversionResultPrimitive.getOutputType());
    assertEquals(8L, conversionResultPrimitive.getLong());
  }

  @Test
  public void testBigintToBigint() {
    BeamSqlPrimitive conversionResultPrimitive =
        IntegerReinterpretConversions.INTEGER_TYPES_TO_BIGINT.convert(BIGINT_PRIMITIVE_15);

    assertEquals(SqlTypeName.BIGINT, conversionResultPrimitive.getOutputType());
    assertEquals(15L, conversionResultPrimitive.getLong());
  }
}
