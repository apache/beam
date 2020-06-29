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
package org.apache.beam.sdk.io.snowflake.test.unit.data;

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeDecimal;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeDouble;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeFloat;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeInteger;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeNumber;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeNumeric;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeReal;
import org.junit.Test;

public class SnowflakeNumericTest {
  @Test
  public void testDecimal() {
    SnowflakeDecimal decimal = SnowflakeDecimal.of(20, 1);

    assertEquals("NUMBER(20,1)", decimal.sql());
  }

  @Test
  public void testDouble() {
    SnowflakeDouble sfDouble = SnowflakeDouble.of();

    assertEquals("FLOAT", sfDouble.sql());
  }

  @Test
  public void testFloat() {
    SnowflakeFloat sfFloat = SnowflakeFloat.of();

    assertEquals("FLOAT", sfFloat.sql());
  }

  @Test
  public void testInteger() {
    SnowflakeInteger sfInteger = SnowflakeInteger.of();

    assertEquals("NUMBER(38,0)", sfInteger.sql());
  }

  @Test
  public void testNumber() {
    SnowflakeNumber snowflakeNumber = SnowflakeNumber.of();

    assertEquals("NUMBER(38,0)", snowflakeNumber.sql());
  }

  @Test
  public void testNumeric() {
    SnowflakeNumeric sfNumeric = SnowflakeNumeric.of(33, 2);

    assertEquals("NUMBER(33,2)", sfNumeric.sql());
  }

  @Test
  public void testReal() {
    SnowflakeReal sfReal = SnowflakeReal.of();

    assertEquals("FLOAT", sfReal.sql());
  }
}
