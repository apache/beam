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

import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.cloud.hadoop.util.ApiErrorExtractor;
import java.io.IOException;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.KV;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This DoFn is responsible for updating a BigQuery's table schema. The input is a TableSchema
 * containing only the schema delta (new fields, relaxed fields). It outputs elements for all
 * updated tables, which act as notifcations to the buffering stage that the elements can be
 * retried.
 */
public class PatchTableSchemaDoFn<DestinationT extends @NonNull Object, ElementT>
    extends DoFn<KV<DestinationT, TableSchema>, KV<DestinationT, ElementT>> {
  private final BigQueryServices bqServices;
  private final StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations;
  private TwoLevelMessageConverterCache<DestinationT, ElementT> messageConverters;
  private transient BigQueryServices.@Nullable DatasetService datasetServiceInternal = null;
  private transient BigQueryServices.@Nullable WriteStreamService writeStreamServiceInternal = null;

  private static final Logger LOG = LoggerFactory.getLogger(PatchTableSchemaDoFn.class);

  PatchTableSchemaDoFn(
      String operationName,
      BigQueryServices bqServices,
      StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations) {
    this.messageConverters = new TwoLevelMessageConverterCache<>(operationName);
    this.bqServices = bqServices;
    this.dynamicDestinations = dynamicDestinations;
  }

  private BigQueryServices.DatasetService getDatasetService(PipelineOptions pipelineOptions)
      throws IOException {
    if (datasetServiceInternal == null) {
      datasetServiceInternal =
          bqServices.getDatasetService(pipelineOptions.as(BigQueryOptions.class));
    }
    return datasetServiceInternal;
  }

  private BigQueryServices.WriteStreamService getWriteStreamService(PipelineOptions pipelineOptions)
      throws IOException {
    if (writeStreamServiceInternal == null) {
      writeStreamServiceInternal =
          bqServices.getWriteStreamService(pipelineOptions.as(BigQueryOptions.class));
    }
    return writeStreamServiceInternal;
  }

  @Teardown
  public void onTeardown() {
    try {
      if (datasetServiceInternal != null) {
        datasetServiceInternal.close();
        datasetServiceInternal = null;
      }
      if (writeStreamServiceInternal != null) {
        writeStreamServiceInternal.close();
        writeStreamServiceInternal = null;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @ProcessElement
  public void processElement(
      @Element KV<DestinationT, TableSchema> element,
      OutputReceiver<KV<DestinationT, @Nullable ElementT>> o,
      ProcessContext context,
      PipelineOptions pipelineOptions)
      throws Exception {
    dynamicDestinations.setSideInputAccessorFromProcessContext(context);
    DestinationT destination = element.getKey();
    TableSchema tableSchemaDiff = element.getValue();

    StorageApiDynamicDestinations.MessageConverter<ElementT> messageConverter =
        messageConverters.get(
            destination,
            dynamicDestinations,
            pipelineOptions,
            getDatasetService(pipelineOptions),
            getWriteStreamService(pipelineOptions));

    while (true) {
      TableSchema baseSchema = messageConverter.getTableSchema();
      TableSchema updatedSchema = UpgradeTableSchema.mergeSchemas(baseSchema, tableSchemaDiff);
      // Check first to see if the schema still needs updating.
      if (baseSchema.equals(updatedSchema)) {
        return;
      }

      BackOff backoff =
          FluentBackoff.DEFAULT
              .withInitialBackoff(Duration.standardSeconds(1))
              .withMaxBackoff(Duration.standardMinutes(1))
              .withMaxRetries(500)
              .withThrottledTimeCounter(
                  BigQuerySinkMetrics.throttledTimeCounter(
                      BigQuerySinkMetrics.RpcMethod.PATCH_TABLE))
              .backoff();

      boolean schemaOutOfDate = false;
      Exception lastException = null;
      do {
        try {
          getDatasetService(pipelineOptions)
              .patchTableSchema(
                  dynamicDestinations.getTable(destination).getTableReference(),
                  TableRowToStorageApiProto.protoSchemaToTableSchema(updatedSchema));
          // Indicate that we've patched this schema.
          o.output(KV.of(destination, null));
          return;
        } catch (IOException e) {
          ApiErrorExtractor errorExtractor = new ApiErrorExtractor();
          if (errorExtractor.preconditionNotMet(e) || errorExtractor.badRequest(e)) {
            schemaOutOfDate = true;
            break;
          } else {
            lastException = e;
          }
        }
      } while (BackOffUtils.next(Sleeper.DEFAULT, backoff));
      if (schemaOutOfDate) {
        // This could be due to an out-of-date schema.
        LOG.info("Schema out of date. Refreshing.");
        messageConverter.updateSchemaFromTable();
      } else {
        // We ran out of retries.
        throw new RuntimeException("Failed to patch table schema.", lastException);
      }
    }
  }
}
