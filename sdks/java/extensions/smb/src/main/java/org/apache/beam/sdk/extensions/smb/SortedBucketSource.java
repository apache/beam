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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.BucketMetadataCoder;
import org.apache.beam.sdk.extensions.smb.SMBFilenamePolicy.FileAssignment;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.fs.ResourceIdCoder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGbkResultSchema;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterators;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.UnmodifiableIterator;
import org.apache.beam.vendor.guava.v20_0.com.google.common.primitives.UnsignedBytes;

/**
 * {@code SortedBucketSource<FinalKeyT, FinalResultT>} takes multiple sorted-bucket sources written
 * with {@link SortedBucketSink}, reads key-values in matching buckets in a merge-sort style, and
 * expands resulting value groups, similar to that of a {@link
 * org.apache.beam.sdk.transforms.join.CoGbkResult}.
 *
 * @param <FinalKeyT> the type of the result keys, sources can have different key types as long as
 *     they can all be decoded as this type
 * @param <FinalResultT> the type of the expanded result, after expanding resulting value groups.
 */
public class SortedBucketSource<FinalKeyT, FinalResultT>
    extends PTransform<PBegin, PCollection<KV<FinalKeyT, FinalResultT>>> {

  private static final Comparator<byte[]> bytesComparator =
      UnsignedBytes.lexicographicalComparator();

  private final transient List<BucketedInput<?, ?>> sources;
  private final Class<FinalKeyT> finalKeyClass;
  private final ToFinalResult<FinalResultT> toFinalResult;

  public SortedBucketSource(
      List<BucketedInput<?, ?>> sources,
      Class<FinalKeyT> finalKeyClass,
      ToFinalResult<FinalResultT> toFinalResult) {
    this.sources = sources;
    this.finalKeyClass = finalKeyClass;
    this.toFinalResult = toFinalResult;
  }

  @Override
  public final PCollection<KV<FinalKeyT, FinalResultT>> expand(PBegin begin) {
    Preconditions.checkState(sources.size() > 1, "Must have more than one source");

    BucketMetadata<?, ?> first = null;
    Coder<FinalKeyT> finalKeyCoder = null;

    int leastNumBuckets = Integer.MAX_VALUE;
    // Check metadata of each source
    for (BucketedInput<?, ?> source : sources) {
      final BucketMetadata<?, ?> current = source.getMetadata();
      if (first == null) {
        first = current;
      } else {
        Preconditions.checkState(
            first.isCompatibleWith(current),
            "Source %s is incompatible with source %s",
            sources.get(0),
            source);
      }

      leastNumBuckets = Math.min(current.getNumBuckets(), leastNumBuckets);

      if (current.getKeyClass() == finalKeyClass && finalKeyCoder == null) {
        try {
          @SuppressWarnings("unchecked")
          final Coder<FinalKeyT> coder = (Coder<FinalKeyT>) current.getKeyCoder();
          finalKeyCoder = coder;
        } catch (CannotProvideCoderException e) {
          throw new RuntimeException("Could not provide coder for key class " + finalKeyClass, e);
        } catch (NonDeterministicException e) {
          throw new RuntimeException("Non-deterministic coder for key class " + finalKeyClass, e);
        }
      }
    }

    Preconditions.checkNotNull(
        finalKeyCoder, "Could not infer coder for key class %s", finalKeyClass);

    // Expand sources into one key-value pair per bucket
    @SuppressWarnings("unchecked")
    final PCollection<KV<Integer, List<BucketedInput<?, ?>>>> bucketedInputs =
        (PCollection<KV<Integer, List<BucketedInput<?, ?>>>>)
            new BucketedInputs(begin.getPipeline(), leastNumBuckets, sources)
                .expand()
                .get(new TupleTag<>("BucketedSources"));

    @SuppressWarnings("deprecation")
    final Reshuffle.ViaRandomKey<KV<Integer, List<BucketedInput<?, ?>>>> reshuffle =
        Reshuffle.viaRandomKey();
    return bucketedInputs
        .apply("ReshuffleKeys", reshuffle)
        .apply(
            "MergeBuckets",
            ParDo.of(new MergeBuckets<>(leastNumBuckets, finalKeyCoder, toFinalResult)))
        .setCoder(KvCoder.of(finalKeyCoder, toFinalResult.resultCoder()));
  }

  /** Merge key-value groups in matching buckets. */
  static class MergeBuckets<FinalKeyT, FinalResultT>
      extends DoFn<KV<Integer, List<BucketedInput<?, ?>>>, KV<FinalKeyT, FinalResultT>> {

    private static final Comparator<Map.Entry<TupleTag, KV<byte[], Iterator<?>>>> keyComparator =
        (o1, o2) -> bytesComparator.compare(o1.getValue().getKey(), o2.getValue().getKey());

    private final Integer leastNumBuckets;
    private final Coder<FinalKeyT> keyCoder;
    private final ToFinalResult<FinalResultT> toResult;

    MergeBuckets(
        int leastNumBuckets, Coder<FinalKeyT> keyCoder, ToFinalResult<FinalResultT> toResult) {
      this.leastNumBuckets = leastNumBuckets;
      this.toResult = toResult;
      this.keyCoder = keyCoder;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      final int bucketId = c.element().getKey();
      final List<BucketedInput<?, ?>> sources = c.element().getValue();
      final int numSources = sources.size();

      // Initialize iterators and tuple tags for sources
      final KeyGroupIterator[] iterators =
          sources.stream()
              .map(i -> i.createIterator(bucketId, leastNumBuckets))
              .toArray(KeyGroupIterator[]::new);
      final List<TupleTag<?>> tupleTags =
          sources.stream().map(i -> i.tupleTag).collect(Collectors.toList());
      final CoGbkResultSchema schema = CoGbkResultSchema.of(tupleTags);

      final Map<TupleTag, KV<byte[], Iterator<?>>> nextKeyGroups = new HashMap<>();

      while (true) {
        int completedSources = 0;
        // Advance key-value groups from each source
        for (int i = 0; i < numSources; i++) {
          final KeyGroupIterator it = iterators[i];
          if (nextKeyGroups.containsKey(tupleTags.get(i))) {
            continue;
          }
          if (it.hasNext()) {
            @SuppressWarnings("unchecked")
            final KV<byte[], Iterator<?>> next = it.next();
            nextKeyGroups.put(tupleTags.get(i), next);
          } else {
            completedSources++;
          }
        }

        if (nextKeyGroups.isEmpty()) {
          break;
        }

        // Find next key-value groups
        final Map.Entry<TupleTag, KV<byte[], Iterator<?>>> minKeyEntry =
            nextKeyGroups.entrySet().stream().min(keyComparator).orElse(null);

        final Iterator<Map.Entry<TupleTag, KV<byte[], Iterator<?>>>> nextKeyGroupsIt =
            nextKeyGroups.entrySet().iterator();
        final List<Iterable<?>> valueMap = new ArrayList<>();
        for (int i = 0; i < schema.size(); i++) {
          valueMap.add(new ArrayList<>());
        }

        while (nextKeyGroupsIt.hasNext()) {
          final Map.Entry<TupleTag, KV<byte[], Iterator<?>>> entry = nextKeyGroupsIt.next();
          if (keyComparator.compare(entry, minKeyEntry) == 0) {
            int index = schema.getIndex(entry.getKey());
            @SuppressWarnings("unchecked")
            final List<Object> values = (List<Object>) valueMap.get(index);
            // TODO: this exhausts everything from the "lazy" iterator and can be expensive.
            // To fix we have to make the underlying Reader range aware so that it's safe to
            // re-iterate or stop without exhausting remaining elements in the value group.
            entry.getValue().getValue().forEachRemaining(values::add);
            nextKeyGroupsIt.remove();
          }
        }

        // Output next key-value group
        final ByteArrayInputStream groupKeyBytes =
            new ByteArrayInputStream(minKeyEntry.getValue().getKey());
        final FinalKeyT groupKey;
        try {
          groupKey = keyCoder.decode(groupKeyBytes);
        } catch (Exception e) {
          throw new RuntimeException("Could not decode key bytes for group", e);
        }
        c.output(KV.of(groupKey, toResult.apply(new CoGbkResult(schema, valueMap))));

        if (completedSources == numSources) {
          break;
        }
      }
    }
  }

  /**
   * Abstracts a potentially heterogeneous list of sorted-bucket sources of {@link BucketedInput}s,
   * similar to {@link org.apache.beam.sdk.transforms.join.CoGroupByKey}.
   */
  static class BucketedInputs implements PInput {
    private transient Pipeline pipeline;
    private List<BucketedInput<?, ?>> sources;
    private Integer numBuckets;

    private BucketedInputs(
        Pipeline pipeline, Integer numBuckets, List<BucketedInput<?, ?>> sources) {
      this.pipeline = pipeline;
      this.numBuckets = numBuckets;
      this.sources = sources;
    }

    @Override
    public Pipeline getPipeline() {
      return pipeline;
    }

    @Override
    public Map<TupleTag<?>, PValue> expand() {
      final List<KV<Integer, List<BucketedInput<?, ?>>>> sources = new ArrayList<>();
      for (int i = 0; i < numBuckets; i++) {
        try {
          sources.add(KV.of(i, this.sources));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      @SuppressWarnings("unchecked")
      final KvCoder<Integer, List<BucketedInput<?, ?>>> coder =
          KvCoder.of(VarIntCoder.of(), ListCoder.of(new BucketedInput.BucketedInputCoder()));

      return Collections.singletonMap(
          new TupleTag<>("BucketedSources"),
          pipeline.apply("CreateBucketedSources", Create.of(sources).withCoder(coder)));
    }
  }

  /**
   * Abstracts a sorted-bucket input to {@link SortedBucketSource} written by {@link
   * SortedBucketSink}.
   *
   * @param <K> the type of the keys that values in a bucket are sorted with
   * @param <V> the type of the values in a bucket
   */
  public static class BucketedInput<K, V> {
    final TupleTag<V> tupleTag;
    final ResourceId filenamePrefix;
    final String filenameSuffix;
    final FileOperations<V> fileOperations;

    transient FileAssignment fileAssignment;
    transient BucketMetadata<K, V> metadata;

    public BucketedInput(
        TupleTag<V> tupleTag,
        ResourceId filenamePrefix,
        String filenameSuffix,
        FileOperations<V> fileOperations) {
      this.tupleTag = tupleTag;
      this.filenamePrefix = filenamePrefix;
      this.filenameSuffix = filenameSuffix;
      this.fileOperations = fileOperations;

      this.fileAssignment = new FileAssignment(filenamePrefix, filenameSuffix);
    }

    BucketedInput(
        TupleTag<V> tupleTag,
        ResourceId filenamePrefix,
        String filenameSuffix,
        FileOperations<V> fileOperations,
        BucketMetadata<K, V> metadata) {
      this(tupleTag, filenamePrefix, filenameSuffix, fileOperations);
      this.metadata = metadata;
    }

    BucketMetadata<K, V> getMetadata() {
      if (metadata != null) {
        return metadata;
      } else {
        try {
          metadata =
              BucketMetadata.from(
                  Channels.newInputStream(FileSystems.open(fileAssignment.forMetadata())));
          return metadata;
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

    KeyGroupIterator<byte[], V> createIterator(int bucketId, int leastNumBuckets) {
      // Create one iterator per shard
      final BucketMetadata<K, V> metadata = getMetadata();

      final int numBucketsInSource = metadata.getNumBuckets();
      final int numShards = metadata.getNumShards();

      final List<Iterator<V>> iterators = new ArrayList<>();

      // Since all BucketedInputs have a bucket count that's a power of two, we can infer
      // which buckets should be merged together for the join.
      for (int i = bucketId; i < numBucketsInSource; i += leastNumBuckets) {
        for (int j = 0; j < numShards; j++) {
          final ResourceId file = fileAssignment.forBucket(BucketShardId.of(i, j), metadata);
          try {
            iterators.add(fileOperations.iterator(file));
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      }

      // Merge-sort key-values from shards
      final UnmodifiableIterator<V> iterator =
          Iterators.mergeSorted(
              iterators,
              (o1, o2) ->
                  bytesComparator.compare(
                      getMetadata().getKeyBytes(o1), getMetadata().getKeyBytes(o2)));
      return new KeyGroupIterator<>(iterator, getMetadata()::getKeyBytes, bytesComparator);
    }

    static class BucketedInputCoder<K, V> extends AtomicCoder<BucketedInput<K, V>> {
      private static SerializableCoder<TupleTag> tupleTagCoder =
          SerializableCoder.of(TupleTag.class);
      private static ResourceIdCoder resourceIdCoder = ResourceIdCoder.of();
      private static StringUtf8Coder stringCoder = StringUtf8Coder.of();
      private static SerializableCoder<FileOperations> fileOpCoder =
          SerializableCoder.of(FileOperations.class);
      private BucketMetadataCoder<K, V> metadataCoder = new BucketMetadataCoder<>();

      @Override
      public void encode(BucketedInput<K, V> value, OutputStream outStream) throws IOException {
        tupleTagCoder.encode(value.tupleTag, outStream);
        resourceIdCoder.encode(value.filenamePrefix, outStream);
        stringCoder.encode(value.filenameSuffix, outStream);
        fileOpCoder.encode(value.fileOperations, outStream);
        metadataCoder.encode(value.getMetadata(), outStream);
      }

      @Override
      public BucketedInput<K, V> decode(InputStream inStream) throws IOException {
        @SuppressWarnings("unchecked")
        TupleTag<V> tupleTag = (TupleTag<V>) tupleTagCoder.decode(inStream);
        ResourceId filenamePrefix = resourceIdCoder.decode(inStream);
        String filenameSuffix = stringCoder.decode(inStream);
        @SuppressWarnings("unchecked")
        FileOperations<V> fileOperations = fileOpCoder.decode(inStream);
        BucketMetadata<K, V> metadata = metadataCoder.decode(inStream);
        return new BucketedInput<>(
            tupleTag, filenamePrefix, filenameSuffix, fileOperations, metadata);
      }
    }

    @Override
    public String toString() {
      return String.format(
          "BucketedInput[tupleTag=%s, filenamePrefix=%s, metadata=%s]",
          tupleTag.getId(), filenamePrefix, getMetadata());
    }
  }

  /**
   * Function to expand a {@link CoGbkResult} into desired a result type, e.g. cartesian product for
   * joins.
   */
  public abstract static class ToFinalResult<FinalResultT>
      implements SerializableFunction<CoGbkResult, FinalResultT> {
    public abstract Coder<FinalResultT> resultCoder();
  }
}
