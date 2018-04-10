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
import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.beam.sdk.extensions.sql.impl.interpreter.operator.BeamSqlPrimitive;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;


/**
 * Unit test for {@link ReinterpretConversion}.
 */
public class ReinterpretConversionTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test public void testNewInstanceProperties() {
    Set<SqlTypeName> from = ImmutableSet.of(SqlTypeName.FLOAT, SqlTypeName.TIME);
    SqlTypeName to = SqlTypeName.BOOLEAN;
    Function<BeamSqlPrimitive, BeamSqlPrimitive> mockConversionFunction = mock(Function.class);

    ReinterpretConversion conversion = ReinterpretConversion.builder()
        .from(from)
        .to(to)
        .convert(mockConversionFunction)
        .build();

    assertEquals(from, conversion.from());
    assertEquals(to, conversion.to());
  }

  @Test public void testConvert() {
    BeamSqlPrimitive integerPrimitive = BeamSqlPrimitive.of(SqlTypeName.INTEGER, 3);
    BeamSqlPrimitive booleanPrimitive = BeamSqlPrimitive.of(SqlTypeName.BOOLEAN, true);

    Function<BeamSqlPrimitive, BeamSqlPrimitive> mockConversionFunction = mock(Function.class);
    doReturn(booleanPrimitive).when(mockConversionFunction).apply(same(integerPrimitive));

    ReinterpretConversion conversion = ReinterpretConversion.builder()
        .from(SqlTypeName.INTEGER)
        .to(SqlTypeName.BOOLEAN)
        .convert(mockConversionFunction)
        .build();

    BeamSqlPrimitive conversionResult = conversion.convert(integerPrimitive);

    assertSame(booleanPrimitive, conversionResult);
    verify(mockConversionFunction).apply(same(integerPrimitive));
  }

  @Test public void testBuilderThrowsWithoutFrom() {
    thrown.expect(IllegalArgumentException.class);
    ReinterpretConversion.builder()
        .to(SqlTypeName.BOOLEAN)
        .convert(mock(Function.class))
        .build();
  }

  @Test public void testBuilderThrowsWihtoutTo() {
    thrown.expect(IllegalArgumentException.class);
    ReinterpretConversion.builder()
        .from(SqlTypeName.BOOLEAN)
        .convert(mock(Function.class))
        .build();
  }

  @Test public void testBuilderThrowsWihtoutConversionFunction() {
    thrown.expect(IllegalArgumentException.class);
    ReinterpretConversion.builder()
        .from(SqlTypeName.BOOLEAN)
        .to(SqlTypeName.SMALLINT)
        .build();
  }

  @Test public void testConvertThrowsForUnsupportedInput() {
    thrown.expect(IllegalArgumentException.class);

    ReinterpretConversion conversion = ReinterpretConversion.builder()
        .from(SqlTypeName.DATE)
        .to(SqlTypeName.BOOLEAN)
        .convert(mock(Function.class))
        .build();

    conversion.convert(BeamSqlPrimitive.of(SqlTypeName.INTEGER, 3));
  }
}
