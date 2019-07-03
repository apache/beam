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

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.BucketedInput;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.ToFinalResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;

/**
 * Sorted-bucket files are {@code PCollection<V>}s written with {@link SortedBucketSink} that can be
 * efficiently merged without shuffling with {@link SortedBucketSource}. When writing, values are
 * grouped by key into buckets, sorted by key within a bucket, and written to files. When reading,
 * key-values in matching buckets are read in a merge-sort style, reducing shuffle.
 */
public class SortedBucketIO {

  /** Builder for a {@link SortedBucketSource} for a given key class. */
  public static <K> ReadBuilder<K, ?, ?> read(Class<K> keyClass) {
    return new ReadBuilder<>(keyClass);
  }

  /** Transforms a two-way {@link CoGbkResult} into a typed {@link KV}. */
  static class CoGbkResult2<V1, V2> extends ToFinalResult<KV<Iterable<V1>, Iterable<V2>>> {
    private final TupleTag<V1> lhsTupleTag;
    private final TupleTag<V2> rhsTupleTag;
    private final Coder<V1> lhsCoder; // TODO: can we get these from Coder registry?
    private final Coder<V2> rhsCoder;

    CoGbkResult2(
        TupleTag<V1> lhsTupleTag,
        TupleTag<V2> rhsTupleTag,
        Coder<V1> lhsCoder,
        Coder<V2> rhsCoder) {
      this.lhsTupleTag = lhsTupleTag;
      this.rhsTupleTag = rhsTupleTag;
      this.lhsCoder = lhsCoder;
      this.rhsCoder = rhsCoder;
    }

    @Override
    public KV<Iterable<V1>, Iterable<V2>> apply(CoGbkResult input) {
      return KV.of(input.getAll(lhsTupleTag), input.getAll(rhsTupleTag));
    }

    @Override
    public Coder<KV<Iterable<V1>, Iterable<V2>>> resultCoder() {
      return KvCoder.of(IterableCoder.of(lhsCoder), IterableCoder.of(rhsCoder));
    }
  }

  /**
   * Builder for a typed two-way sorted-bucket source.
   *
   * @param <K> the type of the keys
   * @param <V1> the type of the left-hand side values
   * @param <V2> the type of the right-hand side values
   */
  public static class ReadBuilder<K, V1, V2> {
    private Class<K> keyClass;
    private JoinSource<K, V1> lhs;
    private JoinSource<K, V2> rhs;

    /**
     * Abstracts a typed source in a sorted-bucket read.
     *
     * @param <K> the type of the keys
     * @param <V> the type of the values
     */
    public static class JoinSource<K, V> {
      private final BucketedInput<K, V> bucketedInput;
      private final Coder<V> valueCoder;

      public JoinSource(BucketedInput<K, V> bucketedInput, Coder<V> valueCoder) {
        this.bucketedInput = bucketedInput;
        this.valueCoder = valueCoder;
      }
    }

    private ReadBuilder(Class<K> keyClass) {
      this.keyClass = keyClass;
    }

    private ReadBuilder(Class<K> keyClass, JoinSource<K, V1> lhs) {
      this(keyClass);
      this.lhs = lhs;
    }

    public <V> ReadBuilder<K, V, ?> of(JoinSource<K, V> lhs) {
      final ReadBuilder<K, V, ?> builderCopy = new ReadBuilder<>(keyClass);

      builderCopy.lhs = lhs;
      return builderCopy;
    }

    public <W> ReadBuilder<K, V1, W> and(JoinSource<K, W> rhs) {
      final ReadBuilder<K, V1, W> builderCopy = new ReadBuilder<>(keyClass, lhs);

      builderCopy.rhs = rhs;
      return builderCopy;
    }

    public SortedBucketSource<K, KV<Iterable<V1>, Iterable<V2>>> build() {
      return new SortedBucketSource<>(
          ImmutableList.of(lhs.bucketedInput, rhs.bucketedInput),
          keyClass,
          new CoGbkResult2<>(
              lhs.bucketedInput.tupleTag, rhs.bucketedInput.tupleTag,
              lhs.valueCoder, rhs.valueCoder));
    }
  }

  public static <K, V> SortedBucketSink<K, V> write(
      BucketMetadata<K, V> bucketingMetadata,
      ResourceId outputDirectory,
      String filenameSuffix,
      ResourceId tempDirectory,
      FileOperations<V> fileOperations) {
    return new SortedBucketSink<>(
        bucketingMetadata,
        new SMBFilenamePolicy(outputDirectory, filenameSuffix),
        fileOperations::createWriter,
        tempDirectory);
  }

  public static <K, V> SortedBucketSink<K, V> write(
      BucketMetadata<K, V> bucketingMetadata,
      ResourceId outputDirectory,
      String filenameSuffix,
      ResourceId tempDirectory,
      FileOperations<V> fileOperations,
      boolean allowNullKeys) {
    return new SortedBucketSink<>(
        bucketingMetadata,
        new SMBFilenamePolicy(outputDirectory, filenameSuffix),
        fileOperations::createWriter,
        tempDirectory,
        allowNullKeys);
  }
}
