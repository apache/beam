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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.storage.v1beta1.ReadOptions.TableReadOptions;
import com.google.cloud.bigquery.storage.v1beta1.Storage.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ReadSession;
import com.google.cloud.bigquery.storage.v1beta1.Storage.ShardingStrategy;
import com.google.cloud.bigquery.storage.v1beta1.Storage.Stream;
import java.io.IOException;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.StorageClient;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A base class for {@link BoundedSource} implementations which read from BigQuery using the
 * BigQuery storage API.
 */
@Experimental(Kind.SOURCE_SINK)
abstract class BigQueryStorageSourceBase<T> extends BoundedSource<T> {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryStorageSourceBase.class);

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

  protected final TableReadOptions tableReadOptions;
  protected final ValueProvider<List<String>> selectedFieldsProvider;
  protected final ValueProvider<String> rowRestrictionProvider;
  protected final SerializableFunction<SchemaAndRecord, T> parseFn;
  protected final Coder<T> outputCoder;
  protected final BigQueryServices bqServices;

  BigQueryStorageSourceBase(
      @Nullable TableReadOptions tableReadOptions,
      @Nullable ValueProvider<List<String>> selectedFieldsProvider,
      @Nullable ValueProvider<String> rowRestrictionProvider,
      SerializableFunction<SchemaAndRecord, T> parseFn,
      Coder<T> outputCoder,
      BigQueryServices bqServices) {
    checkArgument(
        tableReadOptions == null
            || (selectedFieldsProvider == null && rowRestrictionProvider == null),
        "tableReadOptions is mutually exclusive with selectedFieldsProvider and rowRestrictionProvider");
    this.tableReadOptions = tableReadOptions;
    this.selectedFieldsProvider = selectedFieldsProvider;
    this.rowRestrictionProvider = rowRestrictionProvider;
    this.parseFn = checkNotNull(parseFn, "parseFn");
    this.outputCoder = checkNotNull(outputCoder, "outputCoder");
    this.bqServices = checkNotNull(bqServices, "bqServices");
  }

  /**
   * Returns the table to read from at split time. This is currently never an anonymous table, but
   * it can be a named table which was created to hold the results of a query.
   */
  protected abstract Table getTargetTable(BigQueryOptions options) throws Exception;

  @Override
  public Coder<T> getOutputCoder() {
    return outputCoder;
  }

  @Override
  public List<BigQueryStorageStreamSource<T>> split(
      long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
    BigQueryOptions bqOptions = options.as(BigQueryOptions.class);
    Table targetTable = getTargetTable(bqOptions);
    int streamCount = 0;
    if (desiredBundleSizeBytes > 0) {
      long tableSizeBytes = (targetTable != null) ? targetTable.getNumBytes() : 0;
      streamCount = (int) Math.min(tableSizeBytes / desiredBundleSizeBytes, MAX_SPLIT_COUNT);
    }

    streamCount = Math.max(streamCount, MIN_SPLIT_COUNT);

    CreateReadSessionRequest.Builder requestBuilder =
        CreateReadSessionRequest.newBuilder()
            .setParent("projects/" + bqOptions.getProject())
            .setTableReference(BigQueryHelpers.toTableRefProto(targetTable.getTableReference()))
            .setRequestedStreams(streamCount)
            .setShardingStrategy(ShardingStrategy.BALANCED);

    if (selectedFieldsProvider != null || rowRestrictionProvider != null) {
      TableReadOptions.Builder builder = TableReadOptions.newBuilder();
      if (selectedFieldsProvider != null) {
        builder.addAllSelectedFields(selectedFieldsProvider.get());
      }
      if (rowRestrictionProvider != null) {
        builder.setRowRestriction(rowRestrictionProvider.get());
      }
      requestBuilder.setReadOptions(builder);
    } else if (tableReadOptions != null) {
      requestBuilder.setReadOptions(tableReadOptions);
    }

    ReadSession readSession;
    try (StorageClient client = bqServices.getStorageClient(bqOptions)) {
      CreateReadSessionRequest request = requestBuilder.build();
      readSession = client.createReadSession(request);
      LOG.info(
          "Sent BigQuery Storage API CreateReadSession request '{}'; received response '{}'.",
          request,
          readSession);
    }

    if (readSession.getStreamsList().isEmpty()) {
      // The underlying table is empty or all rows have been pruned.
      return ImmutableList.of();
    }

    Schema sessionSchema = new Schema.Parser().parse(readSession.getAvroSchema().getSchema());
    TableSchema trimmedSchema =
        BigQueryAvroUtils.trimBigQueryTableSchema(targetTable.getSchema(), sessionSchema);
    List<BigQueryStorageStreamSource<T>> sources = Lists.newArrayList();
    for (Stream stream : readSession.getStreamsList()) {
      sources.add(
          BigQueryStorageStreamSource.create(
              readSession, stream, trimmedSchema, parseFn, outputCoder, bqServices));
    }

    return ImmutableList.copyOf(sources);
  }

  @Override
  public BoundedReader<T> createReader(PipelineOptions options) throws IOException {
    throw new UnsupportedOperationException("BigQuery storage source must be split before reading");
  }
}
