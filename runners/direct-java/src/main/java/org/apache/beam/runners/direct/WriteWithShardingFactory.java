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

package org.apache.beam.runners.direct;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Write;
import org.apache.beam.sdk.io.Write.Bound;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TaggedPValue;
import org.joda.time.Duration;

/**
 * A {@link PTransformOverrideFactory} that overrides {@link Write} {@link PTransform PTransforms}
 * with an unspecified number of shards with a write with a specified number of shards. The number
 * of shards is the log base 10 of the number of input records, with up to 2 additional shards.
 */
class WriteWithShardingFactory<InputT>
    implements org.apache.beam.sdk.runners.PTransformOverrideFactory<
        PCollection<InputT>, PDone, Write.Bound<InputT>> {
  static final int MAX_RANDOM_EXTRA_SHARDS = 3;

  @Override
  public PTransform<PCollection<InputT>, PDone> getReplacementTransform(
      Bound<InputT> transform) {
    if (transform.getNumShards() == 0) {
      return new DynamicallyReshardedWrite<>(transform);
    }
    return transform;
  }

  @Override
  public PCollection<InputT> getInput(
      List<TaggedPValue> inputs, Pipeline p) {
    return (PCollection<InputT>) Iterables.getOnlyElement(inputs).getValue();
  }

  @Override
  public Map<PValue, ReplacementOutput> mapOutputs(
      List<TaggedPValue> outputs, PDone newOutput) {
    return Collections.emptyMap();
  }

  private static class DynamicallyReshardedWrite<T> extends PTransform<PCollection<T>, PDone> {
    private final transient Write.Bound<T> original;

    private DynamicallyReshardedWrite(Bound<T> original) {
      this.original = original;
    }

    @Override
    public PDone expand(PCollection<T> input) {
      checkArgument(IsBounded.BOUNDED == input.isBounded(),
          "%s can only be applied to a Bounded PCollection",
          getClass().getSimpleName());
      PCollection<T> records = input.apply("RewindowInputs",
          Window.<T>into(new GlobalWindows()).triggering(DefaultTrigger.of())
              .withAllowedLateness(Duration.ZERO)
              .discardingFiredPanes());
      final PCollectionView<Long> numRecords = records
          .apply("CountRecords", Count.<T>globally().asSingletonView());
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
      return original.expand(resharded);
    }
  }

  @VisibleForTesting
  static class KeyBasedOnCountFn<T> extends DoFn<T, KV<Integer, T>> {
    @VisibleForTesting
    static final int MIN_SHARDS_FOR_LOG = 3;

    private final PCollectionView<Long> numRecords;
    private final int randomExtraShards;
    private int currentShard;
    private int maxShards = 0;

    KeyBasedOnCountFn(PCollectionView<Long> numRecords, int extraShards) {
      this.numRecords = numRecords;
      this.randomExtraShards = extraShards;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
      if (maxShards == 0) {
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
