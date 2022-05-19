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

import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * A transform to write sharded records to BigQuery using the Storage API. This transform uses the
 * default stream to write the records. Records written will show up in BigQuery immediately,
 * however exactly once is not guaranteed - duplicates may appear in the output. For exactly-once
 * writes, use {@link StorageApiWritesShardedRecords} or {@link StorageApiWriteUnshardedRecords}.
 */
@SuppressWarnings("FutureReturnValueIgnored")
public class StorageApiWriteRecordsInconsistent<DestinationT, ElementT>
    extends PTransform<PCollection<KV<DestinationT, StorageApiWritePayload>>, PCollection<Void>> {
  private final StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations;
  private final BigQueryServices bqServices;

  public StorageApiWriteRecordsInconsistent(
      StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations,
      BigQueryServices bqServices) {
    this.dynamicDestinations = dynamicDestinations;
    this.bqServices = bqServices;
  }

  @Override
  public PCollection<Void> expand(PCollection<KV<DestinationT, StorageApiWritePayload>> input) {
    String operationName = input.getName() + "/" + getName();
    BigQueryOptions bigQueryOptions = input.getPipeline().getOptions().as(BigQueryOptions.class);
    // Append records to the Storage API streams.
    input.apply(
        "Write Records",
        ParDo.of(
                new StorageApiWriteUnshardedRecords.WriteRecordsDoFn<>(
                    operationName,
                    dynamicDestinations,
                    bqServices,
                    true,
                    bigQueryOptions.getStorageApiAppendThresholdBytes(),
                    bigQueryOptions.getStorageApiAppendThresholdRecordCount(),
                    bigQueryOptions.getNumStorageWriteApiStreamAppendClients()))
            .withSideInputs(dynamicDestinations.getSideInputs()));
    return input.getPipeline().apply("voids", Create.empty(VoidCoder.of()));
  }
}
