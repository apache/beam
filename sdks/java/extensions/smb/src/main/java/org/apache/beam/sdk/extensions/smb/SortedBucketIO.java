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

import java.io.Serializable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.extensions.smb.SMBCoGbkResult.ToFinalResult;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.BucketedInputs.BucketedInput;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;

/** Abstractions for SMB sink/source creation. */
public class SortedBucketIO {

  private static final String LEFT_TUPLE_TAG_ID = "left";
  private static final String RIGHT_TUPLE_TAG_ID = "right";

  // Joins

  /**
   * Pre-built transform for an SortedBucketSource transform with two bucketed inputs.
   *
   * @param <V1>
   * @param <V2>
   */
  static class TwoSourceJoinResult<V1, V2> extends ToFinalResult<KV<Iterable<V1>, Iterable<V2>>> {
    private final Coder<V1> leftCoder; // @Todo: can we get these from Coder registry?
    private final Coder<V2> rightCoder;

    TwoSourceJoinResult(Coder<V1> leftCoder, Coder<V2> rightCoder) {
      this.leftCoder = leftCoder;
      this.rightCoder = rightCoder;
    }

    @Override
    public KV<Iterable<V1>, Iterable<V2>> apply(SMBCoGbkResult input) {
      return KV.of(
          input.getValuesForTag(new TupleTag<>(LEFT_TUPLE_TAG_ID)),
          input.getValuesForTag(new TupleTag<>(RIGHT_TUPLE_TAG_ID)));
    }

    @Override
    public Coder<KV<Iterable<V1>, Iterable<V2>>> resultCoder() {
      return KvCoder.of(
          NullableCoder.of(IterableCoder.of(leftCoder)),
          NullableCoder.of(IterableCoder.of(rightCoder)));
    }
  }

  /**
   * Implements a typed SortedBucketSource for 2 sources.
   *
   * @param <KeyT>
   * @param <V1>
   * @param <V2>
   */
  public static class SortedBucketSourceJoinBuilder<KeyT, V1, V2> implements Serializable {
    private Class<KeyT> keyClass;

    private BucketedInput<KeyT, V1> leftSource;
    private Coder<V1> leftCoder;

    private BucketedInput<KeyT, V2> rightSource;
    private Coder<V2> rightCoder;

    private SortedBucketSourceJoinBuilder(Class<KeyT> keyClass) {
      this.keyClass = keyClass;
    }

    private SortedBucketSourceJoinBuilder(
        Class<KeyT> keyClass, BucketedInput<KeyT, V1> leftSource, Coder<V1> leftCoder) {
      this(keyClass);
      this.leftCoder = leftCoder;
      this.leftSource = leftSource;
    }

    public static <KeyT> SortedBucketSourceJoinBuilder<KeyT, ?, ?> withFinalKeyType(
        Class<KeyT> keyClass) {
      return new SortedBucketSourceJoinBuilder<>(keyClass);
    }

    public <ValueT> SortedBucketSourceJoinBuilder<KeyT, ValueT, ?> of(
        ResourceId filenamePrefix,
        String filenameSuffix,
        FileOperations<ValueT> fileOperations,
        Coder<ValueT> coder) {
      final SortedBucketSourceJoinBuilder<KeyT, ValueT, ?> builderCopy =
          new SortedBucketSourceJoinBuilder<>(keyClass);

      builderCopy.leftSource =
          new BucketedInput<>(
              new TupleTag<>(LEFT_TUPLE_TAG_ID),
              new SMBFilenamePolicy(filenamePrefix, filenameSuffix).forDestination(),
              fileOperations.createReader());
      builderCopy.leftCoder = coder;

      return builderCopy;
    }

    public <ValueT> SortedBucketSourceJoinBuilder<KeyT, V1, ValueT> and(
        ResourceId filenamePrefix,
        String filenameSuffix,
        FileOperations<ValueT> fileOperations,
        Coder<ValueT> coder) {
      final SortedBucketSourceJoinBuilder<KeyT, V1, ValueT> builderCopy =
          new SortedBucketSourceJoinBuilder<KeyT, V1, ValueT>(keyClass, leftSource, leftCoder);

      builderCopy.rightSource =
          new BucketedInput<>(
              new TupleTag<>(RIGHT_TUPLE_TAG_ID),
              new SMBFilenamePolicy(filenamePrefix, filenameSuffix).forDestination(),
              fileOperations.createReader());
      builderCopy.rightCoder = coder;
      return builderCopy;
    }

    public SortedBucketSource<KeyT, KV<Iterable<V1>, Iterable<V2>>> build() {
      return new SortedBucketSource<>(
          new SortedBucketIO.TwoSourceJoinResult<>(leftCoder, rightCoder),
          ImmutableList.of(leftSource, rightSource),
          keyClass);
    }
  }

  // Sinks

  public static <KeyT, ValueT> SortedBucketSink<KeyT, ValueT> sink(
      BucketMetadata<KeyT, ValueT> bucketingMetadata,
      ResourceId outputDirectory,
      String filenameSuffix,
      ResourceId tempDirectory,
      FileOperations<ValueT> fileOperations) {
    return new SortedBucketSink<>(
        bucketingMetadata,
        new SMBFilenamePolicy(outputDirectory, filenameSuffix),
        fileOperations::createWriter,
        tempDirectory);
  }
}
