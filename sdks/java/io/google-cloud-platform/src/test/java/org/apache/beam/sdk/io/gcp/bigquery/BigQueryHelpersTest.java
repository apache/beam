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
package org.apache.beam.sdk.io.gcp.bigquery;

import static org.junit.Assert.assertEquals;

import com.google.api.client.util.Data;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ShardedKeyCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BigQueryHelpers}. */
@RunWith(JUnit4.class)
public class BigQueryHelpersTest {
  @Rule
  public transient ExpectedException thrown = ExpectedException.none();

  @Test
  public void testTableParsing() {
    TableReference ref = BigQueryHelpers
        .parseTableSpec("my-project:data_set.table_name");
    assertEquals("my-project", ref.getProjectId());
    assertEquals("data_set", ref.getDatasetId());
    assertEquals("table_name", ref.getTableId());
  }

  @Test
  public void testTableParsing_validPatterns() {
    BigQueryHelpers.parseTableSpec("a123-456:foo_bar.d");
    BigQueryHelpers.parseTableSpec("a12345:b.c");
    BigQueryHelpers.parseTableSpec("b12345.c");
  }

  @Test
  public void testTableParsing_noProjectId() {
    TableReference ref = BigQueryHelpers
        .parseTableSpec("data_set.table_name");
    assertEquals(null, ref.getProjectId());
    assertEquals("data_set", ref.getDatasetId());
    assertEquals("table_name", ref.getTableId());
  }

  @Test
  public void testTableParsingError() {
    thrown.expect(IllegalArgumentException.class);
    BigQueryHelpers.parseTableSpec("0123456:foo.bar");
  }

  @Test
  public void testTableParsingError_2() {
    thrown.expect(IllegalArgumentException.class);
    BigQueryHelpers.parseTableSpec("myproject:.bar");
  }

  @Test
  public void testTableParsingError_3() {
    thrown.expect(IllegalArgumentException.class);
    BigQueryHelpers.parseTableSpec(":a.b");
  }

  @Test
  public void testTableParsingError_slash() {
    thrown.expect(IllegalArgumentException.class);
    BigQueryHelpers.parseTableSpec("a\\b12345:c.d");
  }

  @Test
  public void testTableDecoratorStripping() {
    assertEquals("project:dataset.table",
        BigQueryHelpers.stripPartitionDecorator("project:dataset.table$20171127"));
    assertEquals("project:dataset.table",
        BigQueryHelpers.stripPartitionDecorator("project:dataset.table"));
  }

  // Test that BigQuery's special null placeholder objects can be encoded.
  @Test
  public void testCoder_nullCell() throws CoderException {
    TableRow row = new TableRow();
    row.set("temperature", Data.nullOf(Object.class));
    row.set("max_temperature", Data.nullOf(Object.class));

    byte[] bytes = CoderUtils.encodeToByteArray(TableRowJsonCoder.of(), row);

    TableRow newRow = CoderUtils.decodeFromByteArray(TableRowJsonCoder.of(), bytes);
    byte[] newBytes = CoderUtils.encodeToByteArray(TableRowJsonCoder.of(), newRow);

    Assert.assertArrayEquals(bytes, newBytes);
  }


  @Test
  public void testShardedKeyCoderIsSerializableWithWellKnownCoderType() {
    CoderProperties.coderSerializable(ShardedKeyCoder.of(GlobalWindow.Coder.INSTANCE));
  }

  @Test
  public void testTableRowInfoCoderSerializable() {
    CoderProperties.coderSerializable(TableRowInfoCoder.of());
  }

  @Test
  public void testComplexCoderSerializable() {
    CoderProperties.coderSerializable(
        WindowedValue.getFullCoder(
            KvCoder.of(
                ShardedKeyCoder.of(StringUtf8Coder.of()),
                TableRowInfoCoder.of()),
            IntervalWindow.getCoder()));
  }
}
