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

import java.util.Arrays;
import java.util.Collection;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeDataType;
import org.apache.beam.sdk.io.snowflake.data.datetime.SnowflakeDate;
import org.apache.beam.sdk.io.snowflake.data.datetime.SnowflakeDateTime;
import org.apache.beam.sdk.io.snowflake.data.datetime.SnowflakeTime;
import org.apache.beam.sdk.io.snowflake.data.datetime.SnowflakeTimestamp;
import org.apache.beam.sdk.io.snowflake.data.datetime.SnowflakeTimestampLTZ;
import org.apache.beam.sdk.io.snowflake.data.datetime.SnowflakeTimestampNTZ;
import org.apache.beam.sdk.io.snowflake.data.datetime.SnowflakeTimestampTZ;
import org.apache.beam.sdk.io.snowflake.data.logical.SnowflakeBoolean;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeDecimal;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeDouble;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeFloat;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeInteger;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeNumber;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeNumeric;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeReal;
import org.apache.beam.sdk.io.snowflake.data.structured.SnowflakeArray;
import org.apache.beam.sdk.io.snowflake.data.structured.SnowflakeObject;
import org.apache.beam.sdk.io.snowflake.data.structured.SnowflakeVariant;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeBinary;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeChar;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeString;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeText;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeVarBinary;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeVarchar;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class SnowflakeDataTypeValidTest {
  private SnowflakeDataType snowflakeDataType;
  private String expectedResult;

  public SnowflakeDataTypeValidTest(SnowflakeDataType snowflakeDataType, String expectedResult) {
    this.snowflakeDataType = snowflakeDataType;
    this.expectedResult = expectedResult;
  }

  @Parameterized.Parameters
  public static Collection primeNumbers() {
    return Arrays.asList(
        new Object[][] {
          {SnowflakeBoolean.of(), "BOOLEAN"},
          {SnowflakeDate.of(), "DATE"},
          {SnowflakeDateTime.of(), "TIMESTAMP_NTZ"},
          {SnowflakeTime.of(), "TIME"},
          {SnowflakeDateTime.of(), "TIMESTAMP_NTZ"},
          {SnowflakeTimestamp.of(), "TIMESTAMP_NTZ"},
          {SnowflakeTimestampNTZ.of(), "TIMESTAMP_NTZ"},
          {SnowflakeTimestampLTZ.of(), "TIMESTAMP_LTZ"},
          {SnowflakeTimestampTZ.of(), "TIMESTAMP_TZ"},
          {SnowflakeDecimal.of(20, 1), "NUMBER(20,1)"},
          {SnowflakeDouble.of(), "FLOAT"},
          {SnowflakeFloat.of(), "FLOAT"},
          {SnowflakeInteger.of(), "NUMBER(38,0)"},
          {SnowflakeNumber.of(), "NUMBER(38,0)"},
          {SnowflakeNumeric.of(33, 2), "NUMBER(33,2)"},
          {SnowflakeReal.of(), "FLOAT"},
          {SnowflakeVariant.of(), "VARIANT"},
          {SnowflakeArray.of(), "ARRAY"},
          {SnowflakeObject.of(), "OBJECT"},
          {SnowflakeVarchar.of(), "VARCHAR"},
          {SnowflakeVarchar.of(100), "VARCHAR(100)"},
          {SnowflakeString.of(), "VARCHAR"},
          {SnowflakeText.of(), "VARCHAR"},
          {SnowflakeBinary.of(), "BINARY"},
          {SnowflakeVarBinary.of(), "BINARY"},
          {SnowflakeBinary.of(100), "BINARY(100)"},
          {SnowflakeChar.of(), "VARCHAR(1)"}
        });
  }

  @Test
  public void testSnowflakeDataType() {
    assertEquals(expectedResult, snowflakeDataType.sql());
  }
}
