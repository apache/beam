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

import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeBinary;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeVarchar;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class SnowflakeDataTypeExceptionTest {

  @Rule public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void testBinaryReachesLimit() {
    exceptionRule.expect(IllegalArgumentException.class);
    SnowflakeBinary.of(SnowflakeBinary.MAX_SIZE + 1);
  }

  @Test
  public void testVarcharReachesLimit() {
    exceptionRule.expect(IllegalArgumentException.class);
    SnowflakeVarchar.of(SnowflakeVarchar.MAX_LENGTH + 1);
  }
}
