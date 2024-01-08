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

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.storage.v1.AppendRowsRequest;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

/**
 * A transform to write sharded records to BigQuery using the Storage API. This transform uses the
 * default stream to write the records. Records written will show up in BigQuery immediately,
 * however exactly once is not guaranteed - duplicates may appear in the output. For exactly-once
 * writes, use {@link StorageApiWritesShardedRecords} or {@link StorageApiWriteUnshardedRecords}.
 */
@SuppressWarnings("FutureReturnValueIgnored")
public class StorageApiWriteRecordsInconsistent<DestinationT, ElementT>
    extends PTransform<PCollection<KV<DestinationT, StorageApiWritePayload>>, PCollectionTuple> {
  private final StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations;
  private final BigQueryServices bqServices;
  private final TupleTag<BigQueryStorageApiInsertError> failedRowsTag;
  private final @Nullable TupleTag<TableRow> successfulRowsTag;
  private final TupleTag<KV<String, String>> finalizeTag = new TupleTag<>("finalizeTag");
  private final Coder<BigQueryStorageApiInsertError> failedRowsCoder;
  private final Coder<TableRow> successfulRowsCoder;
  private final boolean autoUpdateSchema;
  private final boolean ignoreUnknownValues;
  private final BigQueryIO.Write.CreateDisposition createDisposition;
  private final @Nullable String kmsKey;
  private final boolean usesCdc;
  private final AppendRowsRequest.MissingValueInterpretation defaultMissingValueInterpretation;

  public StorageApiWriteRecordsInconsistent(
      StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations,
      BigQueryServices bqServices,
      TupleTag<BigQueryStorageApiInsertError> failedRowsTag,
      @Nullable TupleTag<TableRow> successfulRowsTag,
      Coder<BigQueryStorageApiInsertError> failedRowsCoder,
      Coder<TableRow> successfulRowsCoder,
      boolean autoUpdateSchema,
      boolean ignoreUnknownValues,
      BigQueryIO.Write.CreateDisposition createDisposition,
      @Nullable String kmsKey,
      boolean usesCdc,
      AppendRowsRequest.MissingValueInterpretation defaultMissingValueInterpretation) {
    this.dynamicDestinations = dynamicDestinations;
    this.bqServices = bqServices;
    this.failedRowsTag = failedRowsTag;
    this.failedRowsCoder = failedRowsCoder;
    this.successfulRowsCoder = successfulRowsCoder;
    this.successfulRowsTag = successfulRowsTag;
    this.autoUpdateSchema = autoUpdateSchema;
    this.ignoreUnknownValues = ignoreUnknownValues;
    this.createDisposition = createDisposition;
    this.kmsKey = kmsKey;
    this.usesCdc = usesCdc;
    this.defaultMissingValueInterpretation = defaultMissingValueInterpretation;
  }

  @Override
  public PCollectionTuple expand(PCollection<KV<DestinationT, StorageApiWritePayload>> input) {
    String operationName = input.getName() + "/" + getName();
    BigQueryOptions bigQueryOptions = input.getPipeline().getOptions().as(BigQueryOptions.class);
    // Append records to the Storage API streams.
    TupleTagList tupleTagList = TupleTagList.of(failedRowsTag);
    if (successfulRowsTag != null) {
      tupleTagList = tupleTagList.and(successfulRowsTag);
    }
    PCollectionTuple result =
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
                        bigQueryOptions.getNumStorageWriteApiStreamAppendClients(),
                        finalizeTag,
                        failedRowsTag,
                        successfulRowsTag,
                        autoUpdateSchema,
                        ignoreUnknownValues,
                        createDisposition,
                        kmsKey,
                        usesCdc,
                        defaultMissingValueInterpretation))
                .withOutputTags(finalizeTag, tupleTagList)
                .withSideInputs(dynamicDestinations.getSideInputs()));
    result.get(failedRowsTag).setCoder(failedRowsCoder);
    if (successfulRowsTag != null) {
      result.get(successfulRowsTag).setCoder(successfulRowsCoder);
    }
    return result;
  }
}
