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

import static org.apache.beam.sdk.transforms.errorhandling.BadRecordRouter.BAD_RECORD_TAG;

import com.google.cloud.bigquery.storage.v1.TableSchema;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.BadRecordRouter;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.ShardedKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.UnsignedInteger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * A transform that converts messages to protocol buffers in preparation for writing to BigQuery.
 */
public class StorageApiConvertMessages<DestinationT, ElementT>
    extends PTransform<PCollection<KV<DestinationT, ElementT>>, PCollectionTuple> {
  private final StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations;
  private final BigQueryServices bqServices;
  private final TupleTag<BigQueryStorageApiInsertError> failedWritesTag;
  private final TupleTag<KV<DestinationT, StorageApiWritePayload>> successfulWritesTag;
  private final TupleTag<KV<DestinationT, TableSchema>> patchTableSchemaTag;
  private final TupleTag<KV<DestinationT, ElementT>> elementsWaitingForSchemaTag;
  private final Coder<BigQueryStorageApiInsertError> errorCoder;
  private final Coder<KV<DestinationT, StorageApiWritePayload>> successCoder;
  private final Coder<ElementT> elementCoder;
  private final Coder<DestinationT> destinationCoder;

  private final @Nullable SerializableFunction<ElementT, RowMutationInformation> rowMutationFn;
  private final BadRecordRouter badRecordRouter;
  private final boolean hasSchemaUpdateOptions;

  public StorageApiConvertMessages(
      StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations,
      BigQueryServices bqServices,
      TupleTag<BigQueryStorageApiInsertError> failedWritesTag,
      TupleTag<KV<DestinationT, StorageApiWritePayload>> successfulWritesTag,
      Coder<BigQueryStorageApiInsertError> errorCoder,
      Coder<KV<DestinationT, StorageApiWritePayload>> successCoder,
      Coder<ElementT> elementCoder,
      Coder<DestinationT> destinationCoder,
      @Nullable SerializableFunction<ElementT, RowMutationInformation> rowMutationFn,
      BadRecordRouter badRecordRouter,
      boolean hasSchemaUpdateOptions) {
    this.dynamicDestinations = dynamicDestinations;
    this.bqServices = bqServices;
    this.failedWritesTag = failedWritesTag;
    this.successfulWritesTag = successfulWritesTag;
    this.patchTableSchemaTag = new TupleTag<>("PatchTableSchema");
    this.elementsWaitingForSchemaTag = new TupleTag<>("elementHolder");
    this.errorCoder = errorCoder;
    this.successCoder = successCoder;
    this.elementCoder = elementCoder;
    this.destinationCoder = destinationCoder;
    this.rowMutationFn = rowMutationFn;
    this.badRecordRouter = badRecordRouter;
    this.hasSchemaUpdateOptions = hasSchemaUpdateOptions;
  }

  @Override
  public PCollectionTuple expand(PCollection<KV<DestinationT, ElementT>> input) {
    String operationName = input.getName() + "/" + getName();
    // This code currently assumes that the input is in the global window.
    Preconditions.checkState(input.getWindowingStrategy().getWindowFn() instanceof GlobalWindows);

    @SuppressWarnings({
      "nullness" // TODO(https://github.com/apache/beam/issues/20497)
    })
    ConvertMessagesDoFn<DestinationT, ElementT> convertMessagesDoFn =
        new ConvertMessagesDoFn<>(
            dynamicDestinations,
            bqServices,
            operationName,
            failedWritesTag,
            successfulWritesTag,
            patchTableSchemaTag,
            elementsWaitingForSchemaTag,
            rowMutationFn,
            badRecordRouter,
            input.getCoder(),
            hasSchemaUpdateOptions);

    PCollectionTuple result =
        input.apply(
            "Convert to message",
            ParDo.of(convertMessagesDoFn)
                .withOutputTags(
                    successfulWritesTag,
                    TupleTagList.of(
                        ImmutableList.of(
                            failedWritesTag,
                            BAD_RECORD_TAG,
                            patchTableSchemaTag,
                            elementsWaitingForSchemaTag)))
                .withSideInputs(dynamicDestinations.getSideInputs()));
    result.get(successfulWritesTag).setCoder(successCoder);
    result.get(failedWritesTag).setCoder(errorCoder);
    result.get(BAD_RECORD_TAG).setCoder(BadRecord.getCoder(input.getPipeline()));
    result
        .get(patchTableSchemaTag)
        .setCoder(KvCoder.of(destinationCoder, ProtoCoder.of(TableSchema.class)));
    result.get(elementsWaitingForSchemaTag).setCoder(KvCoder.of(destinationCoder, elementCoder));
    if (!hasSchemaUpdateOptions) {
      // Don't expand the update graph if it's not needed.
      return result;
    } else {
      final int numShards =
          input
              .getPipeline()
              .getOptions()
              .as(BigQueryOptions.class)
              .getSchemaUpgradeBufferingShards();

      // Throttle the stream to the patch-table function so that only a single update per table per
      // two seconds gets processed (to match quotas). The combiner merges incremental schemas, so
      // we
      // won't miss any updates.
      PCollection<KV<ShardedKey<DestinationT>, ElementT>> tablesPatched =
          result
              .get(patchTableSchemaTag)
              .apply(
                  "rewindow",
                  Window.<KV<DestinationT, TableSchema>>configure()
                      .triggering(
                          Repeatedly.forever(
                              AfterProcessingTime.pastFirstElementInPane()
                                  .plusDelayOf(Duration.standardSeconds(2))))
                      .discardingFiredPanes())
              .apply("merge schemas", Combine.fewKeys(new MergeSchemaCombineFn()))
              .setCoder(KvCoder.of(destinationCoder, ProtoCoder.of(TableSchema.class)))
              .apply(
                  "Patch table schema",
                  ParDo.of(
                      new PatchTableSchemaDoFn<>(operationName, bqServices, dynamicDestinations)))
              .setCoder(KvCoder.of(destinationCoder, NullableCoder.of(elementCoder)))
              // We need to make sure that all shards of the buffering transform are notified.
              .apply(
                  "fanout to all shards",
                  FlatMapElements.via(
                      new SimpleFunction<
                          KV<DestinationT, ElementT>,
                          Iterable<KV<ShardedKey<DestinationT>, ElementT>>>() {
                        @Override
                        public Iterable<KV<ShardedKey<DestinationT>, ElementT>> apply(
                            KV<DestinationT, ElementT> elem) {
                          return IntStream.range(0, numShards)
                              .mapToObj(
                                  i ->
                                      KV.of(
                                          StorageApiConvertMessages.AssignShardFn.getShardedKey(
                                              elem.getKey(), i, numShards),
                                          elem.getValue()))
                              .collect(Collectors.toList());
                        }
                      }))
              .setCoder(
                  KvCoder.of(ShardedKey.Coder.of(destinationCoder), NullableCoder.of(elementCoder)))
              .apply(
                  Window.<KV<ShardedKey<DestinationT>, ElementT>>configure()
                      .triggering(DefaultTrigger.of()));

      // Any elements that are waiting for a schema update are sent to this stateful DoFn to be
      // buffered.
      // Note: we currently do not provide the DynamicDestinations object access to the side input
      // in
      // this path.
      // This is because side inputs are not currently available from timer callbacks. Since side
      // inputs are generally
      // used for getSchema and in this case we read the schema from the table, this is unlikely to
      // be
      // a problem.
      PCollection<KV<ShardedKey<DestinationT>, ElementT>> shardedWaitingElements =
          result
              .get(elementsWaitingForSchemaTag)
              // TODO: Consider using GroupIntoBatchs.withShardingKey to get auto sharding here
              // instead of fixed sharding.
              .apply("assignShard", ParDo.of(new AssignShardFn<>(numShards)))
              .setCoder(
                  KvCoder.of(
                      ShardedKey.Coder.of(destinationCoder), NullableCoder.of(elementCoder)));

      PCollectionList<KV<ShardedKey<DestinationT>, ElementT>> waitingElementsList =
          PCollectionList.of(shardedWaitingElements).and(tablesPatched);
      PCollectionTuple retryResult =
          waitingElementsList
              .apply("Buffered flatten", Flatten.pCollections())
              .apply(
                  "bufferElements",
                  ParDo.of(new SchemaUpdateHoldingFn<>(elementCoder, convertMessagesDoFn))
                      .withOutputTags(
                          successfulWritesTag,
                          TupleTagList.of(ImmutableList.of(failedWritesTag, BAD_RECORD_TAG))));
      retryResult.get(successfulWritesTag).setCoder(successCoder);
      retryResult.get(failedWritesTag).setCoder(errorCoder);
      retryResult.get(BAD_RECORD_TAG).setCoder(BadRecord.getCoder(input.getPipeline()));

      // Flatten successes and failures from both the regular transform and the retry transform.
      PCollection<KV<DestinationT, StorageApiWritePayload>> allSuccesses =
          PCollectionList.of(result.get(successfulWritesTag))
              .and(retryResult.get(successfulWritesTag))
              .apply("flattenSuccesses", Flatten.pCollections());
      PCollection<BigQueryStorageApiInsertError> allFailures =
          PCollectionList.of(result.get(failedWritesTag))
              .and(retryResult.get(failedWritesTag))
              .apply("flattenFailures", Flatten.pCollections());
      PCollection<BadRecord> allBadRecords =
          PCollectionList.of(result.get(BAD_RECORD_TAG))
              .and(retryResult.get(BAD_RECORD_TAG))
              .apply("flattenBadRecords", Flatten.pCollections());
      return PCollectionTuple.of(successfulWritesTag, allSuccesses)
          .and(failedWritesTag, allFailures)
          .and(BAD_RECORD_TAG, allBadRecords);
    }
  }

  static class AssignShardFn<K, V> extends DoFn<KV<K, V>, KV<ShardedKey<K>, V>> {
    private int shard;
    private final int numBuckets;

    public AssignShardFn(int numBuckets) {
      this.numBuckets = numBuckets;
    }

    @Setup
    public void setup() {
      shard = ThreadLocalRandom.current().nextInt();
    }

    @ProcessElement
    public void processElement(@Element KV<K, V> element, OutputReceiver<KV<ShardedKey<K>, V>> r) {
      ++shard;
      r.output(KV.of(getShardedKey(element.getKey(), shard, numBuckets), element.getValue()));
    }

    static <K extends @NonNull Object> ShardedKey<K> getShardedKey(
        K key, int shard, int numBuckets) {
      UnsignedInteger unsignedNumBuckets = UnsignedInteger.fromIntBits(numBuckets);
      ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
      buffer.putInt(UnsignedInteger.fromIntBits(shard).mod(unsignedNumBuckets).intValue());
      return ShardedKey.of(key, buffer.array());
    }
  }
}
