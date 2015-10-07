/*
 * Copyright (C) 2015 Google Inc.
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

package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.cloud.dataflow.sdk.util.CoderUtils.makeCloudEncoding;
import static com.google.cloud.dataflow.sdk.util.Structs.addString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.dataflow.model.Source;
import com.google.cloud.dataflow.sdk.options.GcpOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.DirectModeExecutionContext;
import com.google.cloud.dataflow.sdk.util.TestCredential;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;

import org.hamcrest.core.IsInstanceOf;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for BigQueryReaderFactory.
 */
@RunWith(JUnit4.class)
public class BigQueryReaderFactoryTest {
  void runTestCreateBigQueryReaderFromTable(
      String project, String dataset, String table, CloudObject encoding) throws Exception {
    CloudObject spec = CloudObject.forClassName("BigQuerySource");
    addString(spec, "project", project);
    addString(spec, "dataset", dataset);
    addString(spec, "table", table);

    Source cloudSource = new Source();
    cloudSource.setSpec(spec);
    cloudSource.setCodec(encoding);

    GcpOptions options = PipelineOptionsFactory.create().as(GcpOptions.class);
    options.setGcpCredential(new TestCredential());

    Reader<?> reader = ReaderFactory.Registry.defaultRegistry().create(
        cloudSource,
        options,
        DirectModeExecutionContext.create(),
        null,
        null);
    assertThat(reader, new IsInstanceOf(BigQueryReader.class));
    BigQueryReader bigQueryReader = (BigQueryReader) reader;
    TableReference tableRef = bigQueryReader.getTableRef();
    assertEquals(project, tableRef.getProjectId());
    assertEquals(dataset, tableRef.getDatasetId());
    assertEquals(table, tableRef.getTableId());
  }

  void runTestCreateBigQueryReaderFromQuery(String query, CloudObject encoding) throws Exception {
    CloudObject spec = CloudObject.forClassName("BigQuerySource");
    addString(spec, "bigquery_query", query);

    Source cloudSource = new Source();
    cloudSource.setSpec(spec);
    cloudSource.setCodec(encoding);

    GcpOptions options = PipelineOptionsFactory.create().as(GcpOptions.class);
    options.setGcpCredential(new TestCredential());

    Reader<?> reader = ReaderFactory.Registry.defaultRegistry().create(
        cloudSource,
        options,
        DirectModeExecutionContext.create(),
        null,
        null);

    assertThat(reader, new IsInstanceOf(BigQueryReader.class));
    BigQueryReader bigQueryReader = (BigQueryReader) reader;
    assertEquals(query, bigQueryReader.getQuery());
  }

  @Test
  public void testCreateBigQueryReaderFromQuery() throws Exception {
    runTestCreateBigQueryReaderFromQuery("somequery", makeCloudEncoding("TableRowJsonCoder"));
  }

  @Test
  public void testCreateBigQueryReaderFromTable() throws Exception {
    runTestCreateBigQueryReaderFromTable(
        "someproject", "somedataset", "sometable", makeCloudEncoding("TableRowJsonCoder"));
  }

  @Test
  public void testCreateBigQueryReaderCoderIgnored() throws Exception {
    // BigQuery sources do not need a coder because the TableRow objects are read directly from
    // the table using the BigQuery API.
    runTestCreateBigQueryReaderFromTable(
        "someproject", "somedataset", "sometable", makeCloudEncoding("BigEndianIntegerCoder"));
  }
}
