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

import com.google.api.client.util.Base64;
import com.google.api.services.bigquery.model.JobConfigurationQuery;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.Lists;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.TableRowJsonCoder;
import org.apache.beam.sdk.options.BigQueryOptions;


/**
 * A fake implementation of BigQuery's query service..
 */
class FakeBigQueryServices implements BigQueryServices {
  private JobService jobService;
  private FakeDatasetService datasetService;

  FakeBigQueryServices withJobService(JobService jobService) {
    this.jobService = jobService;
    return this;
  }

  FakeBigQueryServices withDatasetService(FakeDatasetService datasetService) {
    this.datasetService = datasetService;
    return this;
  }

  @Override
  public JobService getJobService(BigQueryOptions bqOptions) {
    return jobService;
  }

  @Override
  public DatasetService getDatasetService(BigQueryOptions bqOptions) {
    return datasetService;
  }

  @Override
  public BigQueryJsonReader getReaderFromTable(BigQueryOptions bqOptions, TableReference tableRef) {
    try {
      List<TableRow> rows = datasetService.getAllRows(
          tableRef.getProjectId(), tableRef.getDatasetId(), tableRef.getTableId());
      return new FakeBigQueryReader(rows);
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  public BigQueryJsonReader getReaderFromQuery(
      BigQueryOptions bqOptions, String projectId, JobConfigurationQuery queryConfig) {
    try {
      List<TableRow> rows = rowsFromEncodedQuery(queryConfig.getQuery());
      return new FakeBigQueryReader(rows);
    } catch (IOException e) {
      return null;
    }
  }

  static List<TableRow> rowsFromEncodedQuery(String query) throws IOException {
    ListCoder<TableRow> listCoder = ListCoder.of(TableRowJsonCoder.of());
    ByteArrayInputStream input = new ByteArrayInputStream(Base64.decodeBase64(query));
    List<TableRow> rows = listCoder.decode(input, Context.OUTER);
    for (TableRow row : rows) {
      convertNumbers(row);
    }
    return rows;
  }

  static String encodeQuery(List<TableRow> rows) throws IOException {
    ListCoder<TableRow> listCoder = ListCoder.of(TableRowJsonCoder.of());
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    listCoder.encode(rows, output, Context.OUTER);
    return Base64.encodeBase64String(output.toByteArray());
  }

  private static class FakeBigQueryReader implements BigQueryJsonReader {
    private static final int UNSTARTED = -1;
    private static final int CLOSED = Integer.MAX_VALUE;

    private List<byte[]> serializedTableRowReturns;
    private int currIndex;

    FakeBigQueryReader(List<TableRow> tableRowReturns) throws IOException {
      this.serializedTableRowReturns = Lists.newArrayListWithExpectedSize(tableRowReturns.size());
      for (TableRow tableRow : tableRowReturns) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        TableRowJsonCoder.of().encode(tableRow, output, Context.OUTER);
        serializedTableRowReturns.add(output.toByteArray());
      }
      this.currIndex = UNSTARTED;
    }

    @Override
    public boolean start() throws IOException {
      assertEquals(UNSTARTED, currIndex);
      currIndex = 0;
      return currIndex < serializedTableRowReturns.size();
    }

    @Override
    public boolean advance() throws IOException {
      return ++currIndex < serializedTableRowReturns.size();
    }

    @Override
    public TableRow getCurrent() throws NoSuchElementException {
      if (currIndex >= serializedTableRowReturns.size()) {
        throw new NoSuchElementException();
      }

      ByteArrayInputStream input = new ByteArrayInputStream(
          serializedTableRowReturns.get(currIndex));
      try {
        return convertNumbers(TableRowJsonCoder.of().decode(input, Context.OUTER));
      } catch (IOException e) {
        return null;
      }
    }

    @Override
    public void close() throws IOException {
      currIndex = CLOSED;
    }
  }


  // Longs tend to get converted back to Integers due to JSON serialization. Convert them back.
  static TableRow convertNumbers(TableRow tableRow) {
    for (TableRow.Entry entry : tableRow.entrySet()) {
      if (entry.getValue() instanceof Integer) {
        entry.setValue(new Long((Integer) entry.getValue()));
      }
    }
    return tableRow;
  }
}
