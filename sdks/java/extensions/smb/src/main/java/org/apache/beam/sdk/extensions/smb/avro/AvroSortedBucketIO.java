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
package org.apache.beam.sdk.extensions.smb.avro;

import java.io.Serializable;
import org.apache.avro.Schema;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.extensions.smb.BucketMetadata;
import org.apache.beam.sdk.extensions.smb.SMBFilenamePolicy;
import org.apache.beam.sdk.extensions.smb.SMBJoinResult;
import org.apache.beam.sdk.extensions.smb.SMBJoinResult.ToResult;
import org.apache.beam.sdk.extensions.smb.SortedBucketFile;
import org.apache.beam.sdk.extensions.smb.SortedBucketFile.Reader;
import org.apache.beam.sdk.extensions.smb.SortedBucketFile.Writer;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.KeyedBucketSources.KeyedBucketSource;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;

/**
 * Abstracts SMB sources and sinks for Avro-typed values.
 *
 * <p>Todo - use AutoValue builders
 */
public class AvroSortedBucketIO {

  public static <SortingKeyT, ValueT> SortedBucketSink<SortingKeyT, ValueT> sink(
      BucketMetadata<SortingKeyT, ValueT> bucketingMetadata,
      ResourceId outputDirectory,
      ResourceId tempDirectory,
      Class<ValueT> recordClass,
      Schema schema) {
    return new SortedBucketSink<>(
        bucketingMetadata,
        new SMBFilenamePolicy(outputDirectory, "avro"),
        new AvroWriterSupplier<>(recordClass, schema),
        tempDirectory);
  }

  static class AvroWriterSupplier<ValueT> implements Supplier<Writer<ValueT>>, Serializable {
    SortedBucketFile<ValueT> sortedBucketFile;

    AvroWriterSupplier(Class<ValueT> recordClass, Schema schema) {
      this.sortedBucketFile = new AvroSortedBucketFile<>(recordClass, schema);
    }

    @Override
    public Writer<ValueT> get() {
      return sortedBucketFile.createWriter();
    }
  }

  /**
   * Implements a typed SortedBucketSource for 2 sources.
   *
   * @param <KeyT>
   * @param <V1>
   * @param <V2>
   */
  public static class SortedBucketSourceJoinBuilder<KeyT extends Comparable<KeyT>, V1, V2>
      implements Serializable {
    private KeyedBucketSource<KeyT, V1> leftSource;
    private Coder<V1> leftCoder;
    private KeyedBucketSource<KeyT, V2> rightSource;
    private Coder<V2> rightCoder;
    private Coder<KeyT> keyCoder;

    SortedBucketSourceJoinBuilder(Coder<KeyT> keyCoder, Coder<V1> leftCoder, Coder<V2> rightCoder) {
      this.keyCoder = keyCoder;
      this.leftCoder = leftCoder;
      this.rightCoder = rightCoder;
    }

    public static <K extends Comparable<K>, V1, V2> SortedBucketSourceJoinBuilder<K, V1, V2> of(
        Coder<K> keyCoder, Coder<V1> leftCoder, Coder<V2> rightCoder) {
      return new SortedBucketSourceJoinBuilder<>(keyCoder, leftCoder, rightCoder);
    }

    public SortedBucketSourceJoinBuilder<KeyT, V1, V2> of(
        ResourceId filenamePrefix, Reader<V1> reader) {
      this.leftSource =
          new KeyedBucketSource<KeyT, V1>(
              new TupleTag<>("left"),
              new SMBFilenamePolicy(filenamePrefix, "avro").forDestination(),
              reader);

      return this;
    }

    public SortedBucketSourceJoinBuilder<KeyT, V1, V2> and(
        ResourceId filenamePrefix, Reader<V2> reader) {
      this.rightSource =
          new KeyedBucketSource<KeyT, V2>(
              new TupleTag<>("right"),
              new SMBFilenamePolicy(filenamePrefix, "avro").forDestination(),
              reader);

      return this;
    }

    public SortedBucketSource<KeyT, KV<Iterable<V1>, Iterable<V2>>> build() {
      return new SortedBucketSource<>(
          keyCoder,
          new ToResult<KV<Iterable<V1>, Iterable<V2>>>() {
            @Override
            public KV<Iterable<V1>, Iterable<V2>> apply(SMBJoinResult input) {
              return KV.of(
                  input.getValuesForTag(new TupleTag<V1>("left")),
                  input.getValuesForTag(new TupleTag<V2>("right")));
            }

            @Override
            public Coder<KV<Iterable<V1>, Iterable<V2>>> resultCoder() {
              return KvCoder.of(
                  NullableCoder.of(IterableCoder.of(leftCoder)),
                  NullableCoder.of(IterableCoder.of(rightCoder)));
            }
          },
          ImmutableList.of(leftSource, rightSource));
    }
  }
}
