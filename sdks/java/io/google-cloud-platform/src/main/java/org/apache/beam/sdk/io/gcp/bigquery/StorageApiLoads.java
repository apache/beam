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
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterFirst;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.ShardedKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This {@link PTransform} manages loads into BigQuery using the Storage API. */
public class StorageApiLoads<DestinationT, ElementT>
    extends PTransform<PCollection<KV<DestinationT, ElementT>>, WriteResult> {
  private static final Logger LOG = LoggerFactory.getLogger(StorageApiLoads.class);
  static final int FILE_TRIGGERING_RECORD_COUNT = 100;

  private final Coder<DestinationT> destinationCoder;
  private final Coder<ElementT> elementCoder;
  private final StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations;
  private final CreateDisposition createDisposition;
  private final String kmsKey;
  private final Duration triggeringFrequency;
  private final BigQueryServices bqServices;
  private final int numShards;

  public StorageApiLoads(
      Coder<DestinationT> destinationCoder,
      Coder<ElementT> elementCoder,
      StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations,
      CreateDisposition createDisposition,
      String kmsKey,
      Duration triggeringFrequency,
      BigQueryServices bqServices,
      int numShards) {
    this.destinationCoder = destinationCoder;
    this.elementCoder = elementCoder;
    this.dynamicDestinations = dynamicDestinations;
    this.createDisposition = createDisposition;
    this.kmsKey = kmsKey;
    this.triggeringFrequency = triggeringFrequency;
    this.bqServices = bqServices;
    this.numShards = numShards;
  }

  @Override
  public WriteResult expand(PCollection<KV<DestinationT, ElementT>> input) {
    return triggeringFrequency != null ? expandTriggered(input) : expandUntriggered(input);
  }

  public WriteResult expandTriggered(PCollection<KV<DestinationT, ElementT>> input) {
    // Handle triggered, low-latency loads into BigQuery.
    PCollection<KV<DestinationT, ElementT>> inputInGlobalWindow =
        input.apply(
            "rewindowIntoGlobal",
            Window.<KV<DestinationT, ElementT>>into(new GlobalWindows())
                .triggering(
                    Repeatedly.forever(
                        AfterFirst.of(
                            AfterProcessingTime.pastFirstElementInPane()
                                .plusDelayOf(triggeringFrequency),
                            AfterPane.elementCountAtLeast(FILE_TRIGGERING_RECORD_COUNT))))
                .discardingFiredPanes());

    // First shard all the records.
    // TODO(reuvenlax): Add autosharding support so that users don't have to pick a shard count.
    PCollection<KV<ShardedKey<DestinationT>, ElementT>> shardedRecords =
        inputInGlobalWindow
            .apply(
                "AddShard",
                ParDo.of(
                    new DoFn<KV<DestinationT, ElementT>, KV<ShardedKey<DestinationT>, ElementT>>() {
                      int shardNumber;

                      @Setup
                      public void setup() {
                        shardNumber = ThreadLocalRandom.current().nextInt(numShards);
                      }

                      @ProcessElement
                      public void processElement(
                          @Element KV<DestinationT, ElementT> element,
                          OutputReceiver<KV<ShardedKey<DestinationT>, ElementT>> o) {
                        DestinationT destination = element.getKey();
                        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
                        buffer.putInt(++shardNumber % numShards);
                        o.output(
                            KV.of(ShardedKey.of(destination, buffer.array()), element.getValue()));
                      }
                    }))
            .setCoder(KvCoder.of(ShardedKey.Coder.of(destinationCoder), elementCoder));

    PCollection<KV<ShardedKey<DestinationT>, Iterable<ElementT>>> groupedRecords =
        shardedRecords.apply("GroupIntoShards", GroupByKey.create());

    groupedRecords.apply(
        "StorageApiWriteSharded",
        new StorageApiWritesShardedRecords<>(
            dynamicDestinations, createDisposition, kmsKey, bqServices, destinationCoder));

    return writeResult(input.getPipeline());
  }

  public WriteResult expandUntriggered(PCollection<KV<DestinationT, ElementT>> input) {
    PCollection<KV<DestinationT, ElementT>> inputInGlobalWindow =
        input.apply(
            "rewindowIntoGlobal", Window.<KV<DestinationT, ElementT>>into(new GlobalWindows()));
    inputInGlobalWindow.apply(
        "StorageApiWriteUnsharded",
        new StorageApiWriteUnshardedRecords<>(
            dynamicDestinations, createDisposition, kmsKey, bqServices, destinationCoder));
    return writeResult(input.getPipeline());
  }

  private WriteResult writeResult(Pipeline p) {
    // TODO(reuvenlax): Support per-record failures if schema doesn't match or if the record is too
    // large.
    PCollection<TableRow> empty =
        p.apply("CreateEmptyFailedInserts", Create.empty(TypeDescriptor.of(TableRow.class)));
    return WriteResult.in(p, new TupleTag<>("failedInserts"), empty);
  }
}
