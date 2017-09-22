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

import com.google.api.client.util.Base64;
import com.google.api.services.bigquery.model.TableRow;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import org.apache.beam.sdk.coders.Coder.Context;
import org.apache.beam.sdk.coders.ListCoder;


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
