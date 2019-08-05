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
package org.apache.beam.sdk.extensions.smb;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.BucketedInput;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

/**
 * Sorted-bucket files are {@code PCollection<V>}s written with {@link SortedBucketSink} that can be
 * efficiently merged without shuffling with {@link SortedBucketSource}. When writing, values are
 * grouped by key into buckets, sorted by key within a bucket, and written to files. When reading,
 * key-values in matching buckets are read in a merge-sort style, reducing shuffle.
 */
public class SortedBucketIO {

  static final int DEFAULT_NUM_BUCKETS = 128;
  static final int DEFAULT_NUM_SHARDS = 1;
  static final HashType DEFAULT_HASH_TYPE = HashType.MURMUR3_128;
  static final int DEFAULT_SORTER_MEMORY_MB = 128;

  /** Co-groups sorted-bucket sources with the same sort key. */
  public static <FinalKeyT> CoGbkBuilder<FinalKeyT> read(Class<FinalKeyT> finalKeyClass) {
    return new CoGbkBuilder<>(finalKeyClass);
  }

  /** Builder for sorted-bucket {@link CoGbk}. */
  public static class CoGbkBuilder<K> {
    private final Class<K> finalKeyClass;

    private CoGbkBuilder(Class<K> finalKeyClass) {
      this.finalKeyClass = finalKeyClass;
    }

    /** Returns a new {@link CoGbk} with the given first sorted-bucket source in {@link Read}. */
    public CoGbk<K> of(Read<?> read) {
      return new CoGbk<>(finalKeyClass, Collections.singletonList(read));
    }
  }

  /**
   * A {@link PTransform} for co-grouping sorted-bucket sources using {@link SortedBucketSource}.
   */
  public static class CoGbk<K> extends PTransform<PBegin, PCollection<KV<K, CoGbkResult>>> {
    private final Class<K> keyClass;
    private final List<Read<?>> reads;

    private CoGbk(Class<K> keyClass, List<Read<?>> reads) {
      this.keyClass = keyClass;
      this.reads = reads;
    }

    /**
     * Returns a new {@link CoGbk} that is the same as this, appended with the given sorted-bucket
     * source in {@link Read}.
     */
    public CoGbk<K> and(Read<?> read) {
      ImmutableList<Read<?>> newReads =
          ImmutableList.<Read<?>>builder().addAll(reads).add(read).build();
      return new CoGbk<>(keyClass, newReads);
    }

    @Override
    public PCollection<KV<K, CoGbkResult>> expand(PBegin input) {
      List<BucketedInput<?, ?>> bucketedInputs =
          reads.stream().map(Read::toBucketedInput).collect(Collectors.toList());
      return input.apply(new SortedBucketSource<>(keyClass, bucketedInputs));
    }
  }

  /** Represents a single sorted-bucket source written using {@link SortedBucketSink}. */
  public abstract static class Read<V> {
    protected abstract BucketedInput<?, V> toBucketedInput();
  }
}
