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
package org.apache.beam.sdk.io.clickhouse;

import static org.junit.Assert.assertEquals;

import java.util.Properties;
import org.apache.beam.sdk.io.clickhouse.TableSchema.ColumnType;
import org.apache.beam.sdk.util.ReleaseInfo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link ClickHouseIO} that do not require a running ClickHouse instance. */
@RunWith(JUnit4.class)
public class ClickHouseIOTest {

  private static final String BEAM_AGENT =
      String.format("Apache Beam/%s", ReleaseInfo.getReleaseInfo().getSdkVersion());

  @Test
  public void testBuildClientNameNoExistingClientName() {
    Properties properties = new Properties();
    assertEquals(BEAM_AGENT, ClickHouseIO.buildClientName(properties));
  }

  @Test
  public void testBuildClientNameWithEmptyClientName() {
    Properties properties = new Properties();
    properties.setProperty("client_name", "");
    assertEquals(BEAM_AGENT, ClickHouseIO.buildClientName(properties));
  }

  @Test
  public void testBuildClientNameWithExistingClientName() {
    Properties properties = new Properties();
    properties.setProperty("client_name", "MyApp/1.0");
    assertEquals(BEAM_AGENT + " MyApp/1.0", ClickHouseIO.buildClientName(properties));
  }

  @Test
  public void testInsertSql() {
    TableSchema tableSchema =
        TableSchema.of(
            TableSchema.Column.of("f0", ColumnType.INT64),
            TableSchema.Column.of("f1", ColumnType.INT64));

    String expected = "INSERT INTO \"test_table\" (\"f0\", \"f1\")";

    assertEquals(expected, ClickHouseIO.WriteFn.insertSql(tableSchema, "test_table"));
  }
}
