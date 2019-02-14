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

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkState;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.bigquery.storage.v1beta1.ReadOptions.TableReadOptions;
import com.google.cloud.bigquery.storage.v1beta1.Storage.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadSession;
import com.google.cloud.bigquery.storage.v1beta1.Storage.Stream;
import com.google.cloud.bigquery.storage.v1beta1.TableReferenceProto;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers.TableRefToTableRefProto;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.StorageClient;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link org.apache.beam.sdk.io.Source} representing reading from a table. */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class BigQueryStorageTableSource<T> extends BoundedSource<T> {

  /**
   * The maximum number of streams which will be requested when creating a read session, regardless
   * of the desired bundle size.
   */
  private static final int MAX_SPLIT_COUNT = 10_000;

  /**
   * The minimum number of streams which will be requested when creating a read session, regardless
   * of the desired bundle size. Note that the server may still choose to return fewer than ten
   * streams based on the layout of the table.
   */
  private static final int MIN_SPLIT_COUNT = 10;

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryStorageTableSource.class);

  public static <T> BigQueryStorageTableSource<T> create(
      ValueProvider<TableReference> tableRefProvider,
      @Nullable TableReadOptions readOptions,
      SerializableFunction<SchemaAndRecord, T> parseFn,
      Coder<T> outputCoder,
      BigQueryServices bqServices) {
    return new BigQueryStorageTableSource<>(
        NestedValueProvider.of(
            checkNotNull(tableRefProvider, "tableRefProvider"), new TableRefToTableRefProto()),
        readOptions,
        parseFn,
        outputCoder,
        bqServices);
  }

  private final ValueProvider<TableReferenceProto.TableReference> tableRefProtoProvider;
  private final TableReadOptions readOptions;
  private final SerializableFunction<SchemaAndRecord, T> parseFn;
  private final Coder<T> outputCoder;
  private final BigQueryServices bqServices;
  private final AtomicReference<Long> tableSizeBytes;

  private BigQueryStorageTableSource(
      ValueProvider<TableReferenceProto.TableReference> tableRefProtoProvider,
      @Nullable TableReadOptions readOptions,
      SerializableFunction<SchemaAndRecord, T> parseFn,
      Coder<T> outputCoder,
      BigQueryServices bqServices) {
    this.tableRefProtoProvider = checkNotNull(tableRefProtoProvider, "tableRefProtoProvider");
    this.readOptions = readOptions;
    this.parseFn = checkNotNull(parseFn, "parseFn");
    this.outputCoder = checkNotNull(outputCoder, "outputCoder");
    this.bqServices = checkNotNull(bqServices, "bqServices");
    this.tableSizeBytes = new AtomicReference<>();
  }

  @Override
  public Coder<T> getOutputCoder() {
    return outputCoder;
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder.addIfNotNull(
        DisplayData.item("table", BigQueryHelpers.displayTableRefProto(tableRefProtoProvider))
            .withLabel("Table"));
  }

  private TableReferenceProto.TableReference getTargetTable(BigQueryOptions bqOptions)
      throws IOException {
    TableReferenceProto.TableReference tableReferenceProto = tableRefProtoProvider.get();
    return setDefaultProjectIfAbsent(bqOptions, tableReferenceProto);
  }

  private TableReferenceProto.TableReference setDefaultProjectIfAbsent(
      BigQueryOptions bqOptions, TableReferenceProto.TableReference tableReferenceProto) {
    if (Strings.isNullOrEmpty(tableReferenceProto.getProjectId())) {
      checkState(
          !Strings.isNullOrEmpty(bqOptions.getProject()),
          "No project ID set in %s or %s, cannot construct a complete %s",
          TableReferenceProto.TableReference.class.getSimpleName(),
          BigQueryOptions.class.getSimpleName(),
          TableReferenceProto.TableReference.class.getSimpleName());
      LOG.info(
          "Project ID not set in {}. Using default project from {}.",
          TableReferenceProto.TableReference.class.getSimpleName(),
          BigQueryOptions.class.getSimpleName());
      tableReferenceProto =
          tableReferenceProto.toBuilder().setProjectId(bqOptions.getProject()).build();
    }
    return tableReferenceProto;
  }

  private List<String> getSelectedFields() {
    if (readOptions != null && !readOptions.getSelectedFieldsList().isEmpty()) {
      return readOptions.getSelectedFieldsList();
    }
    return null;
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    if (tableSizeBytes.get() == null) {
      BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
      TableReferenceProto.TableReference tableReferenceProto =
          setDefaultProjectIfAbsent(bqOptions, tableRefProtoProvider.get());
      TableReference tableReference = BigQueryHelpers.toTableRef(tableReferenceProto);
      Table table =
          bqServices.getDatasetService(bqOptions).getTable(tableReference, getSelectedFields());
      tableSizeBytes.compareAndSet(null, table.getNumBytes());
    }
    return tableSizeBytes.get();
  }

  @Override
  public List<BigQueryStorageStreamSource<T>> split(
      long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
    BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
    TableReferenceProto.TableReference tableReferenceProto =
        setDefaultProjectIfAbsent(bqOptions, tableRefProtoProvider.get());
    TableReference tableReference = BigQueryHelpers.toTableRef(tableReferenceProto);
    Table table =
        bqServices.getDatasetService(bqOptions).getTable(tableReference, getSelectedFields());
    long tableSizeBytes = (table != null) ? table.getNumBytes() : 0;

    int streamCount = 0;
    if (desiredBundleSizeBytes > 0) {
      streamCount = (int) Math.min(tableSizeBytes / desiredBundleSizeBytes, MAX_SPLIT_COUNT);
    }

    CreateReadSessionRequest.Builder requestBuilder =
        CreateReadSessionRequest.newBuilder()
            .setParent("projects/" + bqOptions.getProject())
            .setTableReference(tableReferenceProto)
            .setRequestedStreams(Math.max(streamCount, MIN_SPLIT_COUNT));

    if (readOptions != null) {
      requestBuilder.setReadOptions(readOptions);
    }

    ReadSession readSession;
    try (StorageClient client = bqServices.getStorageClient(bqOptions)) {
      readSession = client.createReadSession(requestBuilder.build());
    }

    if (readSession.getStreamsList().isEmpty()) {
      // The underlying table is empty or has no rows which can be read.
      return ImmutableList.of();
    }

    List<BigQueryStorageStreamSource<T>> sources = Lists.newArrayList();
    for (Stream stream : readSession.getStreamsList()) {
      sources.add(
          BigQueryStorageStreamSource.create(
              readSession, stream, table.getSchema(), parseFn, outputCoder, bqServices));
    }

    return ImmutableList.copyOf(sources);
  }

  @Override
  public BoundedReader<T> createReader(PipelineOptions options) throws IOException {
    throw new UnsupportedOperationException("BigQuery table source must be split before reading");
  }
}
