package org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.reinterpret;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.internal.util.collections.Sets;

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

/**
 * Unit tests for {@link Reinterpreter}.
 */
public class ReinterpreterTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test public void testBuilderCreatesInstance() {
    Reinterpreter reinterpreter = newReinterpreter();
    assertNotNull(reinterpreter);
  }

  @Test public void testBuilderThrowsWithoutConverters() {
    thrown.expect(IllegalArgumentException.class);
    Reinterpreter.builder().build();
  }

  @Test public void testCanConvertBetweenSupportedTypes() {
    Reinterpreter reinterpreter = Reinterpreter.builder()
        .withConversion(mockConversion(SqlTypeName.SYMBOL, SqlTypeName.SMALLINT, SqlTypeName.DATE))
        .withConversion(mockConversion(SqlTypeName.INTEGER, SqlTypeName.FLOAT))
        .build();

    assertTrue(reinterpreter.canConvert(SqlTypeName.SMALLINT, SqlTypeName.SYMBOL));
    assertTrue(reinterpreter.canConvert(SqlTypeName.DATE, SqlTypeName.SYMBOL));
    assertTrue(reinterpreter.canConvert(SqlTypeName.FLOAT, SqlTypeName.INTEGER));
  }

  @Test public void testCannotConvertFromUnsupportedTypes() {
    Reinterpreter reinterpreter = Reinterpreter.builder()
        .withConversion(mockConversion(SqlTypeName.SYMBOL, SqlTypeName.SMALLINT, SqlTypeName.DATE))
        .withConversion(mockConversion(SqlTypeName.INTEGER, SqlTypeName.FLOAT))
        .build();

    Set<SqlTypeName> unsupportedTypes = new HashSet<>(SqlTypeName.ALL_TYPES);
    unsupportedTypes.removeAll(
          Sets.newSet(SqlTypeName.DATE, SqlTypeName.SMALLINT, SqlTypeName.FLOAT));

    for (SqlTypeName unsupportedType : unsupportedTypes) {
      assertFalse(reinterpreter.canConvert(unsupportedType, SqlTypeName.DATE));
      assertFalse(reinterpreter.canConvert(unsupportedType, SqlTypeName.INTEGER));
    }
  }

  @Test public void testCannotConvertToUnsupportedTypes() {
    Reinterpreter reinterpreter = Reinterpreter.builder()
        .withConversion(mockConversion(SqlTypeName.SYMBOL, SqlTypeName.SMALLINT, SqlTypeName.DATE))
        .withConversion(mockConversion(SqlTypeName.INTEGER, SqlTypeName.FLOAT))
        .build();

    Set<SqlTypeName> unsupportedTypes = new HashSet<>(SqlTypeName.ALL_TYPES);
    unsupportedTypes.removeAll(Sets.newSet(SqlTypeName.SYMBOL, SqlTypeName.INTEGER));

    for (SqlTypeName unsupportedType : unsupportedTypes) {
      assertFalse(reinterpreter.canConvert(SqlTypeName.SMALLINT, unsupportedType));
      assertFalse(reinterpreter.canConvert(SqlTypeName.DATE, unsupportedType));
      assertFalse(reinterpreter.canConvert(SqlTypeName.FLOAT, unsupportedType));
    }
  }

  @Test public void testConvert() {
    Date date = new Date(12345L);
    BeamSqlPrimitive stringPrimitive = BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hello");
    BeamSqlPrimitive datePrimitive = BeamSqlPrimitive.of(SqlTypeName.DATE, date);

    ReinterpretConversion mockConversion = mock(ReinterpretConversion.class);
    doReturn(Sets.newSet(SqlTypeName.VARCHAR)).when(mockConversion).from();
    doReturn(SqlTypeName.DATE).when(mockConversion).to();
    doReturn(datePrimitive).when(mockConversion).convert(same(stringPrimitive));

    Reinterpreter reinterpreter = Reinterpreter.builder().withConversion(mockConversion).build();
    BeamSqlPrimitive converted = reinterpreter.convert(SqlTypeName.DATE, stringPrimitive);

    assertSame(datePrimitive, converted);
    verify(mockConversion).convert(same(stringPrimitive));
  }

  @Test public void testConvertThrowsForUnsupportedFromType() {
    thrown.expect(UnsupportedOperationException.class);

    BeamSqlPrimitive intervalPrimitive = BeamSqlPrimitive
        .of(SqlTypeName.INTERVAL_DAY, new BigDecimal(2));

    Reinterpreter reinterpreter = newReinterpreter();
    reinterpreter.convert(SqlTypeName.DATE, intervalPrimitive);
  }

  @Test public void testConvertThrowsForUnsupportedToType() {
    thrown.expect(UnsupportedOperationException.class);

    BeamSqlPrimitive stringPrimitive = BeamSqlPrimitive.of(SqlTypeName.VARCHAR, "hello");

    Reinterpreter reinterpreter = newReinterpreter();
    reinterpreter.convert(SqlTypeName.INTERVAL_DAY, stringPrimitive);
  }

  private Reinterpreter newReinterpreter() {
    return Reinterpreter.builder()
        .withConversion(
            mockConversion(
                SqlTypeName.DATE,
                SqlTypeName.SMALLINT, SqlTypeName.VARCHAR))
        .build();
  }

  private ReinterpretConversion mockConversion(SqlTypeName convertTo, SqlTypeName ... convertFrom) {
    ReinterpretConversion conversion = mock(ReinterpretConversion.class);

    doReturn(Sets.newSet(convertFrom)).when(conversion).from();
    doReturn(convertTo).when(conversion).to();

    return conversion;
  }
}
