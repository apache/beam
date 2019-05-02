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

import com.google.common.primitives.UnsignedBytes;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.smb.BucketSourceIterator.BucketSourceIteratorCoder;
import org.apache.beam.sdk.extensions.smb.SMBCoGbkResult.ToResult;
import org.apache.beam.sdk.extensions.smb.SMBFilenamePolicy.FileAssignment;
import org.apache.beam.sdk.extensions.smb.FileOperations.Reader;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.KeyedBucketSources.KeyedBucketSource;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions;

/**
 * Reads in an arbitrary number of heterogeneous typed Sources stored with the same bucketing
 * scheme, and co-groups on a common key.
 *
 * @param <KeyT>
 * @param <ResultT>
 */
public class SortedBucketSource<KeyT, ResultT>
    extends PTransform<PBegin, PCollection<KV<KeyT, ResultT>>> {
  private final SMBCoGbkResult.ToResult<ResultT> toResult;
  private final List<KeyedBucketSource<?, ?>> sources;
  private final Class<KeyT> resultKeyClass;

  public SortedBucketSource(
      SMBCoGbkResult.ToResult<ResultT> toResult,
      List<KeyedBucketSource<?, ?>> sources,
      Class<KeyT> resultKeyClass) {
    this.sources = sources;
    this.toResult = toResult;
    this.resultKeyClass = resultKeyClass;
  }

  @Override
  public final PCollection<KV<KeyT, ResultT>> expand(PBegin begin) {

    // @TODO: Support asymmetric, but still compatible, bucket sizes in reader.
    Preconditions.checkState(sources.size() > 1, "Must have more than one Source");

    BucketMetadata<?, ?> first = null;
    for (KeyedBucketSource<?, ?> source : sources) {
      BucketMetadata<?, ?> current = source.readMetadata();
      if (first == null) {
        first = current;
      } else {
        Preconditions.checkState(first.compatibleWith(current));
      }
    }

    Preconditions.checkState(first.getSortingKeyClass() == resultKeyClass);

    final int numBuckets = first.getNumBuckets();
    Coder<KeyT> resultKeyCoder;
    try {
      resultKeyCoder = (Coder<KeyT>) first.getSortingKeyCoder();
    } catch (CannotProvideCoderException e) {
      throw new RuntimeException("Could not find a coder for key type", e);
    }

    final PCollection<KV<Integer, List<BucketSourceIterator<KeyT>>>> openedReaders =
        (PCollection<KV<Integer, List<BucketSourceIterator<KeyT>>>>)
            new KeyedBucketSources(begin.getPipeline(), numBuckets, sources)
                .expand()
                .get(new TupleTag<>("readers"));

    return openedReaders
        .apply("Force each key to a separate core", Reshuffle.viaRandomKey())
        .apply(
            "Perform co-group operation per key",
            ParDo.of(new MergeBuckets<>(resultKeyCoder, toResult)))
        .setCoder(KvCoder.of(resultKeyCoder, toResult.resultCoder()));
  }

  /** @param <KeyT> */
  static class MergeBuckets<KeyT, ResultT>
      extends DoFn<KV<Integer, List<BucketSourceIterator<KeyT>>>, KV<KeyT, ResultT>> {

    static class KeyGroupBytesComparator
        implements Comparator<Map.Entry<TupleTag, KV<byte[], Iterator<?>>>>, Serializable {
      @Override
      public int compare(
          Map.Entry<TupleTag, KV<byte[], Iterator<?>>> o1,
          Map.Entry<TupleTag, KV<byte[], Iterator<?>>> o2) {
        return UnsignedBytes.lexicographicalComparator()
            .compare(o1.getValue().getKey(), o1.getValue().getKey());
      }
    }

    private final SMBCoGbkResult.ToResult<ResultT> toResult;
    private final Coder<KeyT> keyCoder;
    private final Comparator<Map.Entry<TupleTag, KV<byte[], Iterator<?>>>> keyComparator;

    MergeBuckets(Coder<KeyT> keyCoder, ToResult<ResultT> toResult) {
      this.toResult = toResult;
      this.keyCoder = keyCoder;
      this.keyComparator = new KeyGroupBytesComparator();
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      List<BucketSourceIterator<?>> readers = new ArrayList<>(c.element().getValue());

      readers.forEach(BucketSourceIterator::initializeReader);

      Map<TupleTag, KV<byte[], Iterator<?>>> nextKeyGroups = new HashMap<>();

      while (true) {
        Iterator<BucketSourceIterator<?>> readersIt = readers.iterator();

        while (readersIt.hasNext()) {
          final BucketSourceIterator<?> reader = readersIt.next();

          if (!reader.hasNextKeyGroup()) {
            readersIt.remove();
          } else {
            nextKeyGroups.put(reader.getTupleTag(), reader.nextKeyGroup());
          }
        }

        if (nextKeyGroups.isEmpty()) {
          break;
        }

        final Map.Entry<TupleTag, KV<byte[], Iterator<?>>> minKeyEntry =
            nextKeyGroups.entrySet().stream().min(keyComparator).orElse(null);

        final Iterator<Map.Entry<TupleTag, KV<byte[], Iterator<?>>>> nextKeyGroupsIt =
            nextKeyGroups.entrySet().iterator();

        final Map<TupleTag, Iterable<?>> valueMap = new HashMap<>();

        while (nextKeyGroupsIt.hasNext()) {
          final List<Object> values = new ArrayList<>();
          Map.Entry<TupleTag, KV<byte[], Iterator<?>>> entry = nextKeyGroupsIt.next();

          if (keyComparator.compare(entry, minKeyEntry) == 0) {
            entry.getValue().getValue().forEachRemaining(values::add);

            valueMap.put(entry.getKey(), values);
            nextKeyGroupsIt.remove();
          }
        }

        final ByteArrayInputStream groupKeyBytes =
            new ByteArrayInputStream(minKeyEntry.getValue().getKey());
        KeyT groupKey;
        try {
          groupKey = keyCoder.decode(groupKeyBytes);
        } catch (Exception e) {
          throw new RuntimeException("Couldn't decode key bytes: {}", e);
        }

        c.output(KV.of(groupKey, toResult.apply(new SMBCoGbkResult(valueMap))));

        if (readers.isEmpty()) {
          break;
        }
      }
    }
  }

  /**
   * Maintains type information about a possibly heterogeneous list of sources by wrapping each one
   * in a KeyedBucketSource object with TupleTag. Heavily copied from CoGroupByKey implementation.
   */
  public static class KeyedBucketSources
      implements Serializable, org.apache.beam.sdk.values.PInput {
    private transient Pipeline pipeline;
    private List<KeyedBucketSource<?, ?>> sources;
    private Integer numBuckets;

    KeyedBucketSources(Pipeline pipeline, Integer numBuckets) {
      this(pipeline, numBuckets, new ArrayList<>());
    }

    KeyedBucketSources(
        Pipeline pipeline, Integer numBuckets, List<KeyedBucketSource<?, ?>> sources) {
      this.pipeline = pipeline;
      this.numBuckets = numBuckets;
      this.sources = sources;
    }

    static <K, InputT> KeyedBucketSources of(
        Pipeline pipeline, int numBuckets, KeyedBucketSource<K, InputT> source) {
      return new KeyedBucketSources(pipeline, numBuckets).and(source);
    }

    <K, V> KeyedBucketSources and(KeyedBucketSource<K, V> source) {
      List<KeyedBucketSource<?, ?>> newKeyedCollections = copyAddLast(sources, source);

      return new KeyedBucketSources(pipeline, numBuckets, newKeyedCollections);
    }

    @Override
    public Pipeline getPipeline() {
      return pipeline;
    }

    @Override
    public Map<TupleTag<?>, PValue> expand() {
      final Map<TupleTag<?>, PValue> map = new HashMap<>();

      final List<KV<Integer, List<BucketSourceIterator>>> readers = new ArrayList<>();
      for (int i = 0; i < numBuckets; i++) {
        try {
          readers.add(KV.of(i, createReaders(i)));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      final Coder<KV<Integer, List<BucketSourceIterator>>> coder =
          KvCoder.of(VarIntCoder.of(), ListCoder.of(new BucketSourceIteratorCoder()));

      map.put(
          new TupleTag<>("readers"),
          pipeline.apply("Create bucket readers", Create.of(readers).withCoder(coder)));

      return map;
    }

    private static List<KeyedBucketSource<?, ?>> copyAddLast(
        List<KeyedBucketSource<?, ?>> keyedCollections, KeyedBucketSource<?, ?> taggedCollection) {
      final List<KeyedBucketSource<?, ?>> copy = new ArrayList<>(keyedCollections);
      copy.add(taggedCollection);
      return copy;
    }

    /**
     * Represents a single source with values V.
     *
     * @param <K>
     * @param <V>
     */
    public static class KeyedBucketSource<K, V> implements Serializable {
      final TupleTag<V> tupleTag;
      final FileAssignment fileAssignment;
      final Reader<V> reader;
      transient BucketMetadata<K, Object> metadata;

      public KeyedBucketSource(
          TupleTag<V> tupleTag, FileAssignment fileAssignment, Reader<V> reader) {
        this.tupleTag = tupleTag;
        this.fileAssignment = fileAssignment;
        this.reader = reader;
        this.metadata = null;
      }

      BucketMetadata<K, Object> readMetadata() {
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
    }

    private List<BucketSourceIterator> createReaders(Integer bucket) {
      final List<BucketSourceIterator> readers = new ArrayList<>();

      for (KeyedBucketSource<?, ?> source : sources) {
        final BucketMetadata<?, Object> metadata = source.readMetadata();
        final Reader<?> reader = source.reader;

        ResourceId resourceId = source.fileAssignment.forBucket(bucket, metadata.getNumBuckets());

        if (resourceId != null) {
          readers.add(new BucketSourceIterator<>(reader, resourceId, source.tupleTag, metadata));
        }
      }
      return readers;
    }
  }
}
