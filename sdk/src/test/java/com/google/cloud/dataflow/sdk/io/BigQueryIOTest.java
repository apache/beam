/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.io;

import static org.junit.Assert.assertEquals;

import com.google.api.client.util.Data;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.TableRowJsonCoder;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.CreateDisposition;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.WriteDisposition;
import com.google.cloud.dataflow.sdk.options.BigQueryOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.util.CoderUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;

/**
 * Tests for BigQueryIO.
 */
@RunWith(JUnit4.class)
public class BigQueryIOTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private void checkReadObject(
      BigQueryIO.Read.Bound bound, String project, String dataset, String table) {
    checkReadObjectWithValidate(bound, project, dataset, table, true);
  }

  private void checkReadObjectWithValidate(
      BigQueryIO.Read.Bound bound, String project, String dataset, String table, boolean validate) {
    assertEquals(project, bound.table.getProjectId());
    assertEquals(dataset, bound.table.getDatasetId());
    assertEquals(table, bound.table.getTableId());
    assertEquals(validate, bound.validate);
  }

  private void checkWriteObject(
      BigQueryIO.Write.Bound bound, String project, String dataset, String table,
      TableSchema schema, CreateDisposition createDisposition,
      WriteDisposition writeDisposition) {
    checkWriteObjectWithValidate(
        bound, project, dataset, table, schema, createDisposition, writeDisposition, true);
  }

  private void checkWriteObjectWithValidate(
      BigQueryIO.Write.Bound bound, String project, String dataset, String table,
      TableSchema schema, CreateDisposition createDisposition,
      WriteDisposition writeDisposition, boolean validate) {
    assertEquals(project, bound.table.getProjectId());
    assertEquals(dataset, bound.table.getDatasetId());
    assertEquals(table, bound.table.getTableId());
    assertEquals(schema, bound.schema);
    assertEquals(createDisposition, bound.createDisposition);
    assertEquals(writeDisposition, bound.writeDisposition);
    assertEquals(validate, bound.validate);
  }

  @Before
  public void setUp() {
    BigQueryOptions options = PipelineOptionsFactory.as(BigQueryOptions.class);
    options.setProject("defaultProject");
  }

  @Test
  public void testBuildSource() throws IOException {
    BigQueryIO.Read.Bound bound = BigQueryIO.Read.named("ReadMyTable")
        .from("foo.com:project:somedataset.sometable");
    checkReadObject(bound, "foo.com:project", "somedataset", "sometable");
  }

  @Test
  public void testBuildSourcewithoutValidation() throws IOException {
    // This test just checks that using withoutValidation will not trigger object
    // construction errors.
    BigQueryIO.Read.Bound bound = BigQueryIO.Read.named("ReadMyTable")
        .from("foo.com:project:somedataset.sometable").withoutValidation();
    checkReadObjectWithValidate(bound, "foo.com:project", "somedataset", "sometable", false);
  }

  @Test
  public void testBuildSourceWithDefaultProject() throws IOException {
    BigQueryIO.Read.Bound bound = BigQueryIO.Read.named("ReadMyTable")
        .from("somedataset.sometable");
    checkReadObject(bound, null, "somedataset", "sometable");
  }

  @Test
  public void testBuildSourceWithTableReference() throws IOException {
    TableReference table = new TableReference()
        .setProjectId("foo.com:project")
        .setDatasetId("somedataset")
        .setTableId("sometable");
    BigQueryIO.Read.Bound bound = BigQueryIO.Read.named("ReadMyTable")
        .from(table);
    checkReadObject(bound, "foo.com:project", "somedataset", "sometable");
  }

  @Test(expected = IllegalStateException.class)
  public void testBuildSourceWithoutTable() throws IOException {
    Pipeline p = TestPipeline.create();
    p.apply(BigQueryIO.Read.named("ReadMyTable"));
  }

  @Test
  public void testBuildSink() throws IOException {
    BigQueryIO.Write.Bound bound = BigQueryIO.Write.named("WriteMyTable")
        .to("foo.com:project:somedataset.sometable");
    checkWriteObject(
        bound, "foo.com:project", "somedataset", "sometable",
        null, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_EMPTY);
  }

  @Test
  public void testBuildSinkwithoutValidation() throws IOException {
    // This test just checks that using withoutValidation will not trigger object
    // construction errors.
    BigQueryIO.Write.Bound bound = BigQueryIO.Write.named("WriteMyTable")
        .to("foo.com:project:somedataset.sometable").withoutValidation();
    checkWriteObjectWithValidate(
        bound, "foo.com:project", "somedataset", "sometable",
        null, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_EMPTY, false);
  }

  @Test
  public void testBuildSinkDefaultProject() throws IOException {
    BigQueryIO.Write.Bound bound = BigQueryIO.Write.named("WriteMyTable")
        .to("somedataset.sometable");
    checkWriteObject(
        bound, null, "somedataset", "sometable",
        null, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_EMPTY);
  }

  @Test
  public void testBuildSinkWithTableReference() throws IOException {
    TableReference table = new TableReference()
        .setProjectId("foo.com:project")
        .setDatasetId("somedataset")
        .setTableId("sometable");
    BigQueryIO.Write.Bound bound = BigQueryIO.Write.named("WriteMyTable")
        .to(table);
    checkWriteObject(
        bound, "foo.com:project", "somedataset", "sometable",
        null, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_EMPTY);
  }

  @Test(expected = IllegalStateException.class)
  public void testBuildSinkWithoutTable() throws IOException {
    Pipeline p = TestPipeline.create();
    p.apply(Create.<TableRow>of()).setCoder(TableRowJsonCoder.of())
        .apply(BigQueryIO.Write.named("WriteMyTable"));
  }

  @Test
  public void testBuildSinkWithSchema() throws IOException {
    TableSchema schema = new TableSchema();
    BigQueryIO.Write.Bound bound = BigQueryIO.Write.named("WriteMyTable")
        .to("foo.com:project:somedataset.sometable").withSchema(schema);
    checkWriteObject(
        bound, "foo.com:project", "somedataset", "sometable",
        schema, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_EMPTY);
  }

  @Test
  public void testBuildSinkWithCreateDispositionNever() throws IOException {
    BigQueryIO.Write.Bound bound = BigQueryIO.Write.named("WriteMyTable")
        .to("foo.com:project:somedataset.sometable")
        .withCreateDisposition(CreateDisposition.CREATE_NEVER);
    checkWriteObject(
        bound, "foo.com:project", "somedataset", "sometable",
        null, CreateDisposition.CREATE_NEVER, WriteDisposition.WRITE_EMPTY);
  }

  @Test
  public void testBuildSinkWithCreateDispositionIfNeeded() throws IOException {
    BigQueryIO.Write.Bound bound = BigQueryIO.Write.named("WriteMyTable")
        .to("foo.com:project:somedataset.sometable")
        .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED);
    checkWriteObject(
        bound, "foo.com:project", "somedataset", "sometable",
        null, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_EMPTY);
  }

  @Test
  public void testBuildSinkWithWriteDispositionTruncate() throws IOException {
    BigQueryIO.Write.Bound bound = BigQueryIO.Write.named("WriteMyTable")
        .to("foo.com:project:somedataset.sometable")
        .withWriteDisposition(WriteDisposition.WRITE_TRUNCATE);
    checkWriteObject(
        bound, "foo.com:project", "somedataset", "sometable",
        null, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_TRUNCATE);
  }

  @Test
  public void testBuildSinkWithWriteDispositionAppend() throws IOException {
    BigQueryIO.Write.Bound bound = BigQueryIO.Write.named("WriteMyTable")
        .to("foo.com:project:somedataset.sometable")
        .withWriteDisposition(WriteDisposition.WRITE_APPEND);
    checkWriteObject(
        bound, "foo.com:project", "somedataset", "sometable",
        null, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_APPEND);
  }

  @Test
  public void testBuildSinkWithWriteDispositionEmpty() throws IOException {
    BigQueryIO.Write.Bound bound = BigQueryIO.Write.named("WriteMyTable")
        .to("foo.com:project:somedataset.sometable")
        .withWriteDisposition(WriteDisposition.WRITE_EMPTY);
    checkWriteObject(
        bound, "foo.com:project", "somedataset", "sometable",
        null, CreateDisposition.CREATE_IF_NEEDED, WriteDisposition.WRITE_EMPTY);
  }

  @Test
  public void testTableParsing() {
    TableReference ref = BigQueryIO
        .parseTableSpec("my-project:data_set.table_name");
    Assert.assertEquals("my-project", ref.getProjectId());
    Assert.assertEquals("data_set", ref.getDatasetId());
    Assert.assertEquals("table_name", ref.getTableId());
  }

  @Test
  public void testTableParsing_validPatterns() {
    BigQueryIO.parseTableSpec("a123-456:foo_bar.d");
    BigQueryIO.parseTableSpec("a12345:b.c");
    BigQueryIO.parseTableSpec("b12345.c");
  }

  @Test
  public void testTableParsing_noProjectId() {
    TableReference ref = BigQueryIO
        .parseTableSpec("data_set.table_name");
    Assert.assertEquals(null, ref.getProjectId());
    Assert.assertEquals("data_set", ref.getDatasetId());
    Assert.assertEquals("table_name", ref.getTableId());
  }

  @Test
  public void testTableParsingError() {
    thrown.expect(IllegalArgumentException.class);
    BigQueryIO.parseTableSpec("0123456:foo.bar");
  }

  @Test
  public void testTableParsingError_2() {
    thrown.expect(IllegalArgumentException.class);
    BigQueryIO.parseTableSpec("myproject:.bar");
  }

  @Test
  public void testTableParsingError_3() {
    thrown.expect(IllegalArgumentException.class);
    BigQueryIO.parseTableSpec(":a.b");
  }

  @Test
  public void testTableParsingError_slash() {
    thrown.expect(IllegalArgumentException.class);
    BigQueryIO.parseTableSpec("a\\b12345:c.d");
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
}
