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
package org.apache.beam.testing;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryServicesImpl.createBigQueryClientCustomErrors;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.model.*;
import com.google.api.services.bigquery.model.Table;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.cloud.hadoop.util.ChainingHttpRequestInitializer;
import com.google.common.base.MoreObjects;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.beam.runners.core.metrics.MonitoringInfoConstants;
import org.apache.beam.sdk.extensions.gcp.auth.NullCredentialInitializer;
import org.apache.beam.sdk.extensions.gcp.util.LatencyRecordingHttpRequestInitializer;
import org.apache.beam.sdk.extensions.gcp.util.RetryHttpRequestInitializer;
import org.apache.beam.sdk.extensions.gcp.util.Transport;
import org.apache.beam.sdk.io.gcp.bigquery.*;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.DatasetService;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.*;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

public class UpdateSchemaDoFn extends DoFn<KV<String, TableRow>, TableRow> {
  private static final long DEFAULT_BUFFER_MILLIS = 2 * 60 * 1000; // check every 2 min

  @TimerId("triggerUpdate")
  private final TimerSpec triggerUpdate = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

  @StateId("nextTriggerMillis")
  private final StateSpec<ValueState<Long>> nextTriggerMillis = StateSpecs.value();

  @StateId("tableFieldNames")
  private final StateSpec<SetState<String>> tableFieldNames = StateSpecs.set();

  @StateId("tableRows")
  private final StateSpec<BagState<TimestampedValue<TableRow>>> tableRows =
      StateSpecs.bag(TimestampedValue.TimestampedValueCoder.of(TableRowJsonCoder.of()));

  @StateId("newFields")
  private final StateSpec<MapState<String, String>> newFields = StateSpecs.map();

  private @MonotonicNonNull Bigquery bqClient; // needed to update the table schema
  private final BigQueryServices bqServices = new BigQueryServicesImpl();
  private @MonotonicNonNull DatasetService datasetService;
  private final long bufferMillis;
  private final Set<String> localKnownFields = new HashSet<>();

  private UpdateSchemaDoFn(long bufferMillis) {
    this.bufferMillis = bufferMillis;
  }

  public UpdateSchemaDoFn() {
    this.bufferMillis = DEFAULT_BUFFER_MILLIS;
  }

  static UpdateSchemaDoFn atInterval(Instant interval) {
    return new UpdateSchemaDoFn(interval.getMillis());
  }

  @StartBundle
  public void startBundle(PipelineOptions options) {
    if (bqClient == null) {
      bqClient = newBigQueryClient(options.as(BigQueryOptions.class)).build();
    }
  }

  @ProcessElement
  public void process(
      @Element KV<String, TableRow> element,
      PipelineOptions options,
      @Timestamp Instant timestamp,
      @AlwaysFetched @StateId("tableFieldNames") SetState<String> tableFieldNames,
      @AlwaysFetched @StateId("nextTriggerMillis") ValueState<Long> nextTriggerMillis,
      @TimerId("triggerUpdate") Timer triggerUpdate,
      @AlwaysFetched @StateId("tableRows") BagState<TimestampedValue<TableRow>> tableRows,
      @StateId("newFields") MapState<String, String> newFields,
      OutputReceiver<TableRow> out)
      throws IOException, InterruptedException {
    // TODO: if table schem has been updated and this row's fields are already included,
    //  just fast track and send it downstream and skip this processing

    if (localKnownFields.isEmpty()) {
      Iterable<String> stateFields = tableFieldNames.read();
      if (stateFields == null || !stateFields.iterator().hasNext()) {
        // State hasn't been set up yet. names from BQ
        localKnownFields.addAll(loadTableFieldNames(element.getKey(), options, tableFieldNames));
      } else {
        stateFields.forEach(localKnownFields::add);
      }
      System.out.println("current table field names: " + localKnownFields);
    }

    TableRow row = element.getValue();
    Set<String> rowFields = row.keySet();
    // fast path: if the row doesn't have any new fields, we can safely output it
    if (localKnownFields.containsAll(rowFields)) {
      out.output(row);
      return;
    }

    // slow path: store the row in state and trigger a schema update after some time
    for (Map.Entry<String, Object> entry : row.entrySet()) {
      String key = entry.getKey();
      // check local cache for cheap lookup first
      if (!localKnownFields.contains(key)) {
        String type = determineSchemaFieldType(key, entry.getValue());
        newFields.put(key, type);
        // update local cache so next row doesn't trigger state lookup again
        localKnownFields.add(key);
        System.out.println(
            "found new field: " + key + ", value: " + type + ". inferred type: " + type);
      }
    }
    tableRows.add(TimestampedValue.of(row, timestamp));

    long targetTs =
        MoreObjects.firstNonNull(
            nextTriggerMillis.read(),
            triggerUpdate.getCurrentRelativeTime().getMillis() + bufferMillis);
    triggerUpdate.set(Instant.ofEpochMilli(targetTs));
    System.out.println("next trigger at millis: " + targetTs);
  }

  @OnTimer("triggerUpdate")
  public void updateSchema(
      @Key String tableSpec,
      PipelineOptions options,
      @StateId("nextTriggerMillis") ValueState<Long> nextTriggerMillis,
      @StateId("tableRows") BagState<TimestampedValue<TableRow>> tableRows,
      @StateId("newFields") MapState<String, String> newFields,
      @AlwaysFetched @StateId("tableFieldNames") SetState<String> tableFieldNames,
      OutputReceiver<TableRow> out)
      throws IOException, InterruptedException {
    System.out.println("start update schema");
    // skip update schema operation if we don't have any new fields
    if (newFields.isEmpty().read()) {
      System.out.println("no new fields buffered");
      return;
    }

    DatasetService datasetService = getDatasetService(options);
    TableReference tableRef = checkStateNotNull(BigQueryUtils.toTableReference(tableSpec));
    TableSchema schema = checkStateNotNull(datasetService.getTable(tableRef)).getSchema();
    List<TableFieldSchema> schemaFields = schema.getFields();
    ImmutableSet.Builder<String> newNames = ImmutableSet.builder();
    for (Map.Entry<String, String> entry : newFields.entries().read()) {
      schemaFields.add(
          new TableFieldSchema()
              .setName(entry.getKey())
              .setType(entry.getValue())
              .setMode("NULLABLE"));
      newNames.add(entry.getKey());
    }

    Table tableUpdate = new Table();
    tableUpdate.setSchema(schema);
    checkStateNotNull(bqClient)
        .tables()
        .patch(tableRef.getProjectId(), tableRef.getDatasetId(), tableRef.getTableId(), tableUpdate)
        .execute();
    System.out.println("successfully updated schema to: " + schema);

    newFields.clear();
    newNames.build().forEach(tableFieldNames::add);
    localKnownFields.addAll(newNames.build());

    // flush rows to downstream write transform
    System.out.println("flushing rows downstream");
    for (TimestampedValue<TableRow> row : tableRows.read()) {
      out.outputWithTimestamp(row.getValue(), row.getTimestamp());
    }
    tableRows.clear();

    // clear the next trigger millis state.
    // the value and timer will get reset next time this DoFn sees a row with new fields
    nextTriggerMillis.clear();
  }

  // find table schema field names and load it to state
  private Set<String> loadTableFieldNames(
      String tableSpec, PipelineOptions options, SetState<String> tableFieldNames)
      throws IOException, InterruptedException {
    System.out.println("loading table field names for the first time");
    DatasetService datasetService = getDatasetService(options);
    TableReference tableRef = checkStateNotNull(BigQueryUtils.toTableReference(tableSpec));
    Table table =
        checkStateNotNull(
            datasetService.getTable(tableRef), "Got null when fetching table: %s", tableRef);
    TableSchema schema = table.getSchema();
    Set<String> currentFieldNames =
        schema.getFields().stream().map(TableFieldSchema::getName).collect(Collectors.toSet());
    currentFieldNames.forEach(tableFieldNames::add);
    localKnownFields.addAll(currentFieldNames);

    return currentFieldNames;
  }

  private static String determineSchemaFieldType(String name, Object value) {
    if (value instanceof String) {
      return "STRING";
    } else if (value instanceof Integer) {
      return "INTEGER";
    }
    // ... flesh this tree out
    else {
      throw new UnsupportedOperationException(
          String.format(
              "New field '%s' has value with unsupported type: %s.", name, value.getClass()));
    }
  }

  private DatasetService getDatasetService(PipelineOptions pipelineOptions) {
    if (datasetService == null) {
      datasetService = bqServices.getDatasetService(pipelineOptions.as(BigQueryOptions.class));
    }
    return datasetService;
  }

  /**
   * This code is copied from BigQueryServicesImpl#newBigQueryClient. Ideally we'd use Beam's BQ
   * services, but they currently don't expose a table update method.
   */
  private static Bigquery.Builder newBigQueryClient(BigQueryOptions options) {
    // Do not log 404. It clutters the output and is possibly even required by the
    // caller.
    RetryHttpRequestInitializer httpRequestInitializer =
        new RetryHttpRequestInitializer(ImmutableList.of(404));
    httpRequestInitializer.setCustomErrors(createBigQueryClientCustomErrors());
    httpRequestInitializer.setReadTimeout(options.getHTTPReadTimeout());
    httpRequestInitializer.setWriteTimeout(options.getHTTPWriteTimeout());
    ImmutableList.Builder<HttpRequestInitializer> initBuilder = ImmutableList.builder();
    Credentials credential = options.getGcpCredential();
    initBuilder.add(
        credential == null
            ? new NullCredentialInitializer()
            : new HttpCredentialsAdapter(credential));

    initBuilder.add(
        new LatencyRecordingHttpRequestInitializer(
            ImmutableMap.of(
                MonitoringInfoConstants.Labels.SERVICE, "BigQuery",
                MonitoringInfoConstants.Labels.METHOD, "BigQueryWrite")));

    initBuilder.add(httpRequestInitializer);
    HttpRequestInitializer chainInitializer =
        new ChainingHttpRequestInitializer(
            Iterables.toArray(initBuilder.build(), HttpRequestInitializer.class));
    Bigquery.Builder builder =
        new Bigquery.Builder(Transport.getTransport(), Transport.getJsonFactory(), chainInitializer)
            .setApplicationName(options.getAppName())
            .setGoogleClientRequestInitializer(options.getGoogleApiTrace());

    @Nullable String endpoint = options.getBigQueryEndpoint();
    if (!Strings.isNullOrEmpty(endpoint)) {
      builder.setRootUrl(endpoint);
    }
    return builder;
  }
}
