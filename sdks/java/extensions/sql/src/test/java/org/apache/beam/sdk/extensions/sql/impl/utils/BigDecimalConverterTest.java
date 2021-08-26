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

import static org.junit.Assert.assertNotNull;

import java.math.BigDecimal;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Unit tests for {@link BigDecimalConverter}. */
public class BigDecimalConverterTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testReturnsConverterForNumericTypes() {
    for (TypeName numericType : TypeName.NUMERIC_TYPES) {
      SerializableFunction<BigDecimal, ? extends Number> converter =
          BigDecimalConverter.forSqlType(numericType);

      assertNotNull(converter);
      assertNotNull(converter.apply(BigDecimal.TEN));
    }
  }

  @Test
  public void testThrowsForUnsupportedTypes() {
    thrown.expect(UnsupportedOperationException.class);
    BigDecimalConverter.forSqlType(TypeName.STRING);
  }
}
