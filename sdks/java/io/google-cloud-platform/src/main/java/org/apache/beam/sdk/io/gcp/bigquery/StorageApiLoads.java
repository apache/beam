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

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.ShardedKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;

/** This {@link PTransform} manages loads into BigQuery using the Storage API. */
public class StorageApiLoads<DestinationT, ElementT>
    extends PTransform<PCollection<KV<DestinationT, ElementT>>, WriteResult> {
  static final int MAX_BATCH_SIZE_BYTES = 2 * 1024 * 1024;
  final TupleTag<KV<DestinationT, StorageApiWritePayload>> successfulRowsTag =
      new TupleTag<>("successfulRows");
  final TupleTag<BigQueryStorageApiInsertError> failedRowsTag = new TupleTag<>("failedRows");

  private final Coder<DestinationT> destinationCoder;
  private final StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations;
  private final CreateDisposition createDisposition;
  private final String kmsKey;
  private final Duration triggeringFrequency;
  private final BigQueryServices bqServices;
  private final int numShards;
  private final boolean allowInconsistentWrites;
  private final boolean allowAutosharding;

  public StorageApiLoads(
      Coder<DestinationT> destinationCoder,
      StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations,
      CreateDisposition createDisposition,
      String kmsKey,
      Duration triggeringFrequency,
      BigQueryServices bqServices,
      int numShards,
      boolean allowInconsistentWrites,
      boolean allowAutosharding) {
    this.destinationCoder = destinationCoder;
    this.dynamicDestinations = dynamicDestinations;
    this.createDisposition = createDisposition;
    this.kmsKey = kmsKey;
    this.triggeringFrequency = triggeringFrequency;
    this.bqServices = bqServices;
    this.numShards = numShards;
    this.allowInconsistentWrites = allowInconsistentWrites;
    this.allowAutosharding = allowAutosharding;
  }

  @Override
  public WriteResult expand(PCollection<KV<DestinationT, ElementT>> input) {
    Coder<StorageApiWritePayload> payloadCoder;
    try {
      payloadCoder =
          input.getPipeline().getSchemaRegistry().getSchemaCoder(StorageApiWritePayload.class);
    } catch (NoSuchSchemaException e) {
      throw new RuntimeException(e);
    }
    Coder<KV<DestinationT, StorageApiWritePayload>> successCoder =
        KvCoder.of(destinationCoder, payloadCoder);
    if (allowInconsistentWrites) {
      return expandInconsistent(input, successCoder);
    } else {
      return triggeringFrequency != null
          ? expandTriggered(input, successCoder, payloadCoder)
          : expandUntriggered(input, successCoder);
    }
  }

  public WriteResult expandInconsistent(
      PCollection<KV<DestinationT, ElementT>> input,
      Coder<KV<DestinationT, StorageApiWritePayload>> successCoder) {
    PCollection<KV<DestinationT, ElementT>> inputInGlobalWindow =
        input.apply("rewindowIntoGlobal", Window.into(new GlobalWindows()));

    PCollectionTuple convertedRecords =
        inputInGlobalWindow
            .apply(
                "CreateTables",
                new CreateTableDestinations<>(
                    createDisposition, bqServices, dynamicDestinations, kmsKey))
            .apply(
                "Convert",
                new StorageApiConvertMessages<>(
                    dynamicDestinations,
                    bqServices,
                    failedRowsTag,
                    successfulRowsTag,
                    BigQueryStorageApiInsertErrorCoder.of(),
                    successCoder));
    convertedRecords
        .get(successfulRowsTag)
        .apply(
            "StorageApiWriteInconsistent",
            new StorageApiWriteRecordsInconsistent<>(dynamicDestinations, bqServices));
    return WriteResult.in(
        input.getPipeline(),
        null,
        null,
        null,
        null,
        null,
        failedRowsTag,
        convertedRecords.get(failedRowsTag));
  }

  public WriteResult expandTriggered(
      PCollection<KV<DestinationT, ElementT>> input,
      Coder<KV<DestinationT, StorageApiWritePayload>> successCoder,
      Coder<StorageApiWritePayload> payloadCoder) {
    // Handle triggered, low-latency loads into BigQuery.
    PCollection<KV<DestinationT, ElementT>> inputInGlobalWindow =
        input.apply("rewindowIntoGlobal", Window.into(new GlobalWindows()));
    PCollectionTuple result =
        inputInGlobalWindow
            .apply(
                "CreateTables",
                new CreateTableDestinations<>(
                    createDisposition, bqServices, dynamicDestinations, kmsKey))
            .apply(
                "Convert",
                new StorageApiConvertMessages<>(
                    dynamicDestinations,
                    bqServices,
                    failedRowsTag,
                    successfulRowsTag,
                    BigQueryStorageApiInsertErrorCoder.of(),
                    successCoder));

    PCollection<KV<ShardedKey<DestinationT>, Iterable<StorageApiWritePayload>>> groupedRecords;

    if (this.allowAutosharding) {
      groupedRecords =
          result
              .get(successfulRowsTag)
              .apply(
                  "GroupIntoBatches",
                  GroupIntoBatches.<DestinationT, StorageApiWritePayload>ofByteSize(
                          MAX_BATCH_SIZE_BYTES,
                          (StorageApiWritePayload e) -> (long) e.getPayload().length)
                      .withMaxBufferingDuration(triggeringFrequency)
                      .withShardedKey());

    } else {
      PCollection<KV<ShardedKey<DestinationT>, StorageApiWritePayload>> shardedRecords =
          createShardedKeyValuePairs(result)
              .setCoder(KvCoder.of(ShardedKey.Coder.of(destinationCoder), payloadCoder));
      groupedRecords =
          shardedRecords.apply(
              "GroupIntoBatches",
              GroupIntoBatches.<ShardedKey<DestinationT>, StorageApiWritePayload>ofByteSize(
                      MAX_BATCH_SIZE_BYTES,
                      (StorageApiWritePayload e) -> (long) e.getPayload().length)
                  .withMaxBufferingDuration(triggeringFrequency));
    }
    groupedRecords.apply(
        "StorageApiWriteSharded",
        new StorageApiWritesShardedRecords<>(
            dynamicDestinations, createDisposition, kmsKey, bqServices, destinationCoder));

    return WriteResult.in(
        input.getPipeline(),
        null,
        null,
        null,
        null,
        null,
        failedRowsTag,
        result.get(failedRowsTag));
  }

  private PCollection<KV<ShardedKey<DestinationT>, StorageApiWritePayload>>
      createShardedKeyValuePairs(PCollectionTuple pCollection) {
    return pCollection
        .get(successfulRowsTag)
        .apply(
            "AddShard",
            ParDo.of(
                new DoFn<
                    KV<DestinationT, StorageApiWritePayload>,
                    KV<ShardedKey<DestinationT>, StorageApiWritePayload>>() {
                  int shardNumber;

                  @Setup
                  public void setup() {
                    shardNumber = ThreadLocalRandom.current().nextInt(numShards);
                  }

                  @ProcessElement
                  public void processElement(
                      @Element KV<DestinationT, StorageApiWritePayload> element,
                      OutputReceiver<KV<ShardedKey<DestinationT>, StorageApiWritePayload>> o) {
                    DestinationT destination = element.getKey();
                    ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
                    buffer.putInt(++shardNumber % numShards);
                    o.output(KV.of(ShardedKey.of(destination, buffer.array()), element.getValue()));
                  }
                }));
  }

  public WriteResult expandUntriggered(
      PCollection<KV<DestinationT, ElementT>> input,
      Coder<KV<DestinationT, StorageApiWritePayload>> successCoder) {
    PCollection<KV<DestinationT, ElementT>> inputInGlobalWindow =
        input.apply(
            "rewindowIntoGlobal", Window.<KV<DestinationT, ElementT>>into(new GlobalWindows()));
    PCollectionTuple convertedRecords =
        inputInGlobalWindow
            .apply(
                "CreateTables",
                new CreateTableDestinations<>(
                    createDisposition, bqServices, dynamicDestinations, kmsKey))
            .apply(
                "Convert",
                new StorageApiConvertMessages<>(
                    dynamicDestinations,
                    bqServices,
                    failedRowsTag,
                    successfulRowsTag,
                    BigQueryStorageApiInsertErrorCoder.of(),
                    successCoder));
    convertedRecords
        .get(successfulRowsTag)
        .apply(
            "StorageApiWriteUnsharded",
            new StorageApiWriteUnshardedRecords<>(dynamicDestinations, bqServices));

    return WriteResult.in(
        input.getPipeline(),
        null,
        null,
        null,
        null,
        null,
        failedRowsTag,
        convertedRecords.get(failedRowsTag));
  }
}
