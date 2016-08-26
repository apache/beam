/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.runners.inprocess;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.dataflow.sdk.io.Write;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Values;
import com.google.cloud.dataflow.sdk.transforms.windowing.DefaultTrigger;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.POutput;
import com.google.common.annotations.VisibleForTesting;
import org.joda.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A {@link PTransformOverrideFactory} that overrides {@link Write} {@link PTransform PTransforms}
 * with an unspecified number of shards with a write with a specified number of shards. The number
 * of shards is the log base 10 of the number of input records, with up to 2 additional shards.
 */
class WriteWithShardingFactory implements PTransformOverrideFactory {
  static final int MAX_RANDOM_EXTRA_SHARDS = 3;

  @Override
  public <InputT extends PInput, OutputT extends POutput> PTransform<InputT, OutputT> override(
      PTransform<InputT, OutputT> transform) {
    if (transform instanceof Write.Bound) {
      Write.Bound<InputT> that = (Write.Bound<InputT>) transform;
      if (that.getNumShards() == 0) {
        return (PTransform<InputT, OutputT>) new DynamicallyReshardedWrite<InputT>(that);
      }
    }
    return transform;
  }

  private static class DynamicallyReshardedWrite <T> extends PTransform<PCollection<T>, PDone> {
    private final transient Write.Bound<T> original;

    private DynamicallyReshardedWrite(Write.Bound<T> original) {
      this.original = original;
    }

    @Override
    public PDone apply(PCollection<T> input) {
      PCollection<T> records = input.apply("RewindowInputs",
          Window.<T>into(new GlobalWindows()).triggering(DefaultTrigger.of())
              .withAllowedLateness(Duration.ZERO)
              .discardingFiredPanes());
      final PCollectionView<Long> numRecords = records.apply(Count.<T>globally().asSingletonView());
      PCollection<T> resharded =
          records
              .apply(
                  "ApplySharding",
                  ParDo.withSideInputs(numRecords)
                      .of(
                          new KeyBasedOnCountFn<T>(
                              numRecords,
                              ThreadLocalRandom.current().nextInt(MAX_RANDOM_EXTRA_SHARDS))))
              .apply("GroupIntoShards", GroupByKey.<Integer, T>create())
              .apply("DropShardingKeys", Values.<Iterable<T>>create())
              .apply("FlattenShardIterables", Flatten.<T>iterables());
      // This is an inverted application to apply the expansion of the original Write PTransform
      // without adding a new Write Transform Node, which would be overwritten the same way, leading
      // to an infinite recursion. We cannot modify the number of shards, because that is determined
      // at runtime.
      return original.apply(resharded);
    }
  }

  @VisibleForTesting
  static class KeyBasedOnCountFn<T> extends DoFn<T, KV<Integer, T>> {
    @VisibleForTesting
    static final int MIN_SHARDS_FOR_LOG = 3;

    private final PCollectionView<Long> numRecords;
    private final int randomExtraShards;
    private int currentShard;
    private int maxShards;

    KeyBasedOnCountFn(PCollectionView<Long> numRecords, int extraShards) {
      this.numRecords = numRecords;
      this.randomExtraShards = extraShards;
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
      if (maxShards == 0L) {
        maxShards = calculateShards(c.sideInput(numRecords));
        currentShard = ThreadLocalRandom.current().nextInt(maxShards);
      }
      int shard = currentShard;
      currentShard = (currentShard + 1) % maxShards;
      c.output(KV.of(shard, c.element()));
    }

    private int calculateShards(long totalRecords) {
      checkArgument(
          totalRecords > 0,
          "KeyBasedOnCountFn cannot be invoked on an element if there are no elements");
      if (totalRecords < MIN_SHARDS_FOR_LOG + randomExtraShards) {
        return (int) totalRecords;
      }
      // 100mil records before >7 output files
      int floorLogRecs = Double.valueOf(Math.log10(totalRecords)).intValue();
      int shards = Math.max(floorLogRecs, MIN_SHARDS_FOR_LOG) + randomExtraShards;
      return shards;
    }
  }
}
