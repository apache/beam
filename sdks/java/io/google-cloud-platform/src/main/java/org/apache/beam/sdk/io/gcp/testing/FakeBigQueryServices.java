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
package org.apache.beam.sdk.io.gcp.testing;

import com.google.api.client.util.Base64;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableRow;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices;
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder;
import org.apache.beam.sdk.util.Histogram;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

/** A fake implementation of BigQuery's query service.. */
@Internal
public class FakeBigQueryServices implements BigQueryServices {
  private JobService jobService;
  private DatasetService datasetService;
  private StorageClient storageClient;

  public FakeBigQueryServices withJobService(JobService jobService) {
    this.jobService = jobService;
    return this;
  }

  public FakeBigQueryServices withDatasetService(FakeDatasetService datasetService) {
    this.datasetService = datasetService;
    return this;
  }

  public FakeBigQueryServices withStorageClient(StorageClient storageClient) {
    this.storageClient = storageClient;
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
  public DatasetService getDatasetService(BigQueryOptions bqOptions, Histogram requestLatencies) {
    return datasetService;
  }

  @Override
  public StorageClient getStorageClient(BigQueryOptions bqOptions) {
    return storageClient;
  }

  /**
   * An implementation of {@link BigQueryServerStream} which takes a {@link List} as the {@link
   * Iterable} to simulate a server stream. {@link #FakeBigQueryServerStream} is a no-op.
   */
  public static class FakeBigQueryServerStream<T> implements BigQueryServerStream<T> {

    private final List<T> items;

    public FakeBigQueryServerStream(List<T> items) {
      this.items = items;
    }

    @Override
    public Iterator<T> iterator() {
      return items.iterator();
    }

    @Override
    public void cancel() {}
  }

  public static String encodeQueryResult(Table table) throws IOException {
    return encodeQueryResult(table, ImmutableList.of());
  }

  public static String encodeQueryResult(Table table, List<TableRow> rows) throws IOException {
    KvCoder<String, List<TableRow>> coder =
        KvCoder.of(StringUtf8Coder.of(), ListCoder.of(TableRowJsonCoder.of()));
    KV<String, List<TableRow>> kv = KV.of(BigQueryHelpers.toJsonString(table), rows);
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    coder.encode(kv, outputStream);
    return Base64.encodeBase64String(outputStream.toByteArray());
  }

  public static KV<Table, List<TableRow>> decodeQueryResult(String queryResult) throws IOException {
    KvCoder<String, List<TableRow>> coder =
        KvCoder.of(StringUtf8Coder.of(), ListCoder.of(TableRowJsonCoder.of()));
    ByteArrayInputStream inputStream = new ByteArrayInputStream(Base64.decodeBase64(queryResult));
    KV<String, List<TableRow>> kv = coder.decode(inputStream);
    Table table = BigQueryHelpers.fromJsonString(kv.getKey(), Table.class);
    List<TableRow> rows = kv.getValue();
    rows.forEach(FakeBigQueryServices::convertNumbers);
    return KV.of(table, rows);
  }

  // Longs tend to get converted back to Integers due to JSON serialization. Convert them back.
  public static TableRow convertNumbers(TableRow tableRow) {
    for (TableRow.Entry<?, Object> entry : tableRow.entrySet()) {
      if (entry.getValue() instanceof Integer) {
        entry.setValue(Long.valueOf((Integer) entry.getValue()));
      }
    }
    return tableRow;
  }
}
