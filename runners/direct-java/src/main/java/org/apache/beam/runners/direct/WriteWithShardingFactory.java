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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.runners.core.construction.PTransformReplacements;
import org.apache.beam.runners.core.construction.WriteFilesTranslation;
import org.apache.beam.sdk.io.WriteFiles;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.runners.PTransformOverrideFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * A {@link PTransformOverrideFactory} that overrides {@link WriteFiles} {@link PTransform
 * PTransforms} with an unspecified number of shards with a write with a specified number of shards.
 * The number of shards is the log base 10 of the number of input records, with up to 2 additional
 * shards.
 */
class WriteWithShardingFactory<InputT>
    implements PTransformOverrideFactory<
        PCollection<InputT>, PDone, PTransform<PCollection<InputT>, PDone>> {
  static final int MAX_RANDOM_EXTRA_SHARDS = 3;
  @VisibleForTesting static final int MIN_SHARDS_FOR_LOG = 3;

  @Override
  public PTransformReplacement<PCollection<InputT>, PDone> getReplacementTransform(
      AppliedPTransform<PCollection<InputT>, PDone, PTransform<PCollection<InputT>, PDone>>
          transform) {
    try {
      WriteFiles<InputT, ?, ?> replacement = WriteFiles.to(
          WriteFilesTranslation.getSink(transform),
          WriteFilesTranslation.getFormatFunction(transform));
      if (WriteFilesTranslation.isWindowedWrites(transform)) {
        replacement = replacement.withWindowedWrites();
      }
      return PTransformReplacement.of(
          PTransformReplacements.getSingletonMainInput(transform),
          replacement.withSharding(new LogElementShardsWithDrift<InputT>()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Map<PValue, ReplacementOutput> mapOutputs(
      Map<TupleTag<?>, PValue> outputs, PDone newOutput) {
    return Collections.emptyMap();
  }

  private static class LogElementShardsWithDrift<T>
      extends PTransform<PCollection<T>, PCollectionView<Integer>> {

    @Override
    public PCollectionView<Integer> expand(PCollection<T> records) {
      return records
          .apply(Window.<T>into(new GlobalWindows()))
          .apply("CountRecords", Count.<T>globally())
          .apply("GenerateShardCount", ParDo.of(new CalculateShardsFn()))
          .apply(View.<Integer>asSingleton());
    }
  }

  @VisibleForTesting
  static class CalculateShardsFn extends DoFn<Long, Integer> {
    private final Supplier<Integer> extraShardsSupplier;

    public CalculateShardsFn() {
      this(new BoundedRandomIntSupplier(MAX_RANDOM_EXTRA_SHARDS));
    }

    /**
     * Construct a {@link CalculateShardsFn} that always uses a constant number of specified extra
     * shards.
     */
    @VisibleForTesting
    CalculateShardsFn(int constantExtraShards) {
      this(Suppliers.ofInstance(constantExtraShards));
    }

    private CalculateShardsFn(Supplier<Integer> extraShardsSupplier) {
      this.extraShardsSupplier = extraShardsSupplier;
    }

    @ProcessElement
    public void process(ProcessContext ctxt) {
      ctxt.output(calculateShards(ctxt.element()));
    }

    private int calculateShards(long totalRecords) {
      if (totalRecords == 0) {
        // WriteFiles out at least one shard, even if there is no input.
        return 1;
      }
      // Windows get their own number of random extra shards. This is stored in a side input, so
      // writers use a consistent number of keys.
      int extraShards = extraShardsSupplier.get();
      if (totalRecords < MIN_SHARDS_FOR_LOG + extraShards) {
        return (int) totalRecords;
      }
      // 100mil records before >7 output files
      int floorLogRecs = Double.valueOf(Math.log10(totalRecords)).intValue();
      return Math.max(floorLogRecs, MIN_SHARDS_FOR_LOG) + extraShards;
    }
  }

  private static class BoundedRandomIntSupplier implements Supplier<Integer>, Serializable {
    private final int upperBound;

    private BoundedRandomIntSupplier(int upperBound) {
      this.upperBound = upperBound;
    }

    @Override
    public Integer get() {
      return ThreadLocalRandom.current().nextInt(0, upperBound);
    }
  }
}
