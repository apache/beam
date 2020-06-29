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

import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeBinary;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeChar;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeString;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeText;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeVarBinary;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeVarchar;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class SnowflakeVarcharTest {
  @Test
  public void testSingleVarchar() {
    SnowflakeVarchar varchar = SnowflakeVarchar.of();

    assertEquals("VARCHAR", varchar.sql());
  }

  @Test
  public void testSingleVarcharWithLimit() {
    SnowflakeVarchar varchar = SnowflakeVarchar.of(100);

    assertEquals("VARCHAR(100)", varchar.sql());
  }

  @Test
  public void testString() {
    SnowflakeString str = SnowflakeString.of();

    assertEquals("VARCHAR", str.sql());
  }

  @Test
  public void testText() {
    SnowflakeText text = SnowflakeText.of();

    assertEquals("VARCHAR", text.sql());
  }

  @Test
  public void testBinary() {
    SnowflakeBinary binary = SnowflakeBinary.of();

    assertEquals("BINARY", binary.sql());
  }

  @Test
  public void testVarBinary() {
    SnowflakeVarBinary binary = SnowflakeVarBinary.of();

    assertEquals("BINARY", binary.sql());
  }

  @Test
  public void testBinaryWithLimit() {
    SnowflakeBinary binary = SnowflakeBinary.of(100);

    assertEquals("BINARY(100)", binary.sql());
  }

  @Rule public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void testBinaryReachesLimit() {
    exceptionRule.expect(IllegalArgumentException.class);
    SnowflakeBinary.of(8388609L);
  }

  @Test
  public void testChar() {
    SnowflakeChar sfChar = SnowflakeChar.of();

    assertEquals("VARCHAR(1)", sfChar.sql());
  }
}
