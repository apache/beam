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

import org.apache.beam.sdk.io.snowflake.data.SnowflakeColumn;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeTableSchema;
import org.apache.beam.sdk.io.snowflake.data.numeric.SnowflakeDouble;
import org.apache.beam.sdk.io.snowflake.data.text.SnowflakeVarchar;
import org.junit.Test;

public class SnowflakeTableSchemaTest {
  @Test
  public void testOneColumn() {
    SnowflakeTableSchema schema =
        new SnowflakeTableSchema(SnowflakeColumn.of("id", SnowflakeVarchar.of()));

    assertEquals("id VARCHAR", schema.sql());
  }

  @Test
  public void testTwoColumns() {
    SnowflakeTableSchema schema =
        new SnowflakeTableSchema(
            SnowflakeColumn.of("id", new SnowflakeVarchar()),
            SnowflakeColumn.of("tax", new SnowflakeDouble()));

    assertEquals("id VARCHAR, tax FLOAT", schema.sql());
  }
}
