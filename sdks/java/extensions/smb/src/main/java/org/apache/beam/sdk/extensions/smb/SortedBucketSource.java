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
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.smb.BucketSourceIterator.BucketSourceIteratorCoder;
import org.apache.beam.sdk.extensions.smb.SMBCoGbkResult.ToResult;
import org.apache.beam.sdk.extensions.smb.SMBFilenamePolicy.FileAssignment;
import org.apache.beam.sdk.extensions.smb.SortedBucketFile.Reader;
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
  private final Coder<KeyT> keyCoder;
  private final SMBCoGbkResult.ToResult<ResultT> toResult;
  private final List<KeyedBucketSource<KeyT, ?>> sources;
  private final Comparator<KeyT> keyComparator;

  public SortedBucketSource(
      Coder<KeyT> keyCoder,
      SMBCoGbkResult.ToResult<ResultT> toResult,
      List<KeyedBucketSource<KeyT, ?>> sources,
      Comparator<KeyT> keyComparator) {
    this.keyCoder = keyCoder;
    this.sources = sources;
    this.toResult = toResult;
    this.keyComparator = keyComparator;
  }

  @Override
  public final PCollection<KV<KeyT, ResultT>> expand(PBegin begin) {

    // @TODO: Support asymmetric, but still compatible, bucket sizes in reader.

    BucketMetadata<KeyT, ?> first = null;
    for (KeyedBucketSource<KeyT, ?> source : sources) {
      BucketMetadata<KeyT, ?> current = source.readMetadata();
      if (first == null) {
        first = current;
      } else {
        Preconditions.checkState(first.compatibleWith(current));
      }
    }

    final int numBuckets = first.getNumBuckets();

    final PCollection<KV<Integer, List<BucketSourceIterator<KeyT>>>> openedReaders =
        (PCollection<KV<Integer, List<BucketSourceIterator<KeyT>>>>)
            new KeyedBucketSources<>(begin.getPipeline(), numBuckets, sources)
                .expand()
                .get(new TupleTag<>("readers"));

    return openedReaders
        .apply("Force each key to a separate core", Reshuffle.viaRandomKey())
        .apply(
            "Perform co-group operation per key",
            ParDo.of(new MergeBuckets<>(toResult, keyComparator)))
        .setCoder(KvCoder.of(keyCoder, toResult.resultCoder()));
  }

  /** @param <KeyT> */
  static class MergeBuckets<KeyT, ResultT>
      extends DoFn<KV<Integer, List<BucketSourceIterator<KeyT>>>, KV<KeyT, ResultT>> {
    private final SMBCoGbkResult.ToResult<ResultT> toResult;
    private final Comparator<KeyT> keyComparator;

    MergeBuckets(ToResult<ResultT> toResult, Comparator<KeyT> keyComparator) {
      this.toResult = toResult;
      this.keyComparator = keyComparator;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      List<BucketSourceIterator<KeyT>> readers = new ArrayList<>(c.element().getValue());

      readers.forEach(BucketSourceIterator::initializeReader);

      KeyT keyForGroup;

      Map<TupleTag, KV<KeyT, Iterator<?>>> nextKeyGroups = new HashMap<>();

      while (true) {
        Iterator<BucketSourceIterator<KeyT>> readersIt = readers.iterator();

        while (readersIt.hasNext()) {
          final BucketSourceIterator<KeyT> reader = readersIt.next();

          if (!reader.hasNextKeyGroup()) {
            readersIt.remove();
          } else {
            nextKeyGroups.put(reader.getTupleTag(), reader.nextKeyGroup());
          }
        }

        if (nextKeyGroups.isEmpty()) {
          break;
        }

        final KeyT minKey =
            nextKeyGroups.entrySet().stream()
                .min(
                    (e1, e2) ->
                        keyComparator.compare(e1.getValue().getKey(), e2.getValue().getKey()))
                .map(entry -> entry.getValue().getKey())
                .orElse(null);

        keyForGroup = minKey;

        final Iterator<Map.Entry<TupleTag, KV<KeyT, Iterator<?>>>> nextKeyGroupsIt =
            nextKeyGroups.entrySet().iterator();

        final Map<TupleTag, Iterable<?>> valueMap = new HashMap<>();

        while (nextKeyGroupsIt.hasNext()) {
          final List<Object> values = new ArrayList<>();
          Map.Entry<TupleTag, KV<KeyT, Iterator<?>>> entry = nextKeyGroupsIt.next();

          if (keyComparator.compare(entry.getValue().getKey(), minKey) == 0) {
            entry.getValue().getValue().forEachRemaining(values::add);

            valueMap.put(entry.getKey(), values);
            nextKeyGroupsIt.remove();
          }
        }

        c.output(KV.of(keyForGroup, toResult.apply(new SMBCoGbkResult(valueMap))));

        if (readers.isEmpty()) {
          break;
        }
      }
    }
  }

  /**
   * Maintains type information about a possibly heterogeneous list of sources by wrapping each one
   * in a KeyedBucketSource object with TupleTag. Heavily copied from CoGroupByKey implementation.
   *
   * @param <K>
   */
  public static class KeyedBucketSources<K>
      implements Serializable, org.apache.beam.sdk.values.PInput {
    private transient Pipeline pipeline;
    private List<KeyedBucketSource<K, ?>> sources;
    private Integer numBuckets;

    KeyedBucketSources(Pipeline pipeline, Integer numBuckets) {
      this(pipeline, numBuckets, new ArrayList<>());
    }

    KeyedBucketSources(
        Pipeline pipeline, Integer numBuckets, List<KeyedBucketSource<K, ?>> sources) {
      this.pipeline = pipeline;
      this.numBuckets = numBuckets;
      this.sources = sources;
    }

    static <K, InputT> KeyedBucketSources<K> of(
        Pipeline pipeline, int numBuckets, KeyedBucketSource<K, InputT> source) {
      return new KeyedBucketSources<K>(pipeline, numBuckets).and(source);
    }

    <V> KeyedBucketSources<K> and(KeyedBucketSource<K, V> source) {
      List<KeyedBucketSource<K, ?>> newKeyedCollections = copyAddLast(sources, source);

      return new KeyedBucketSources<>(pipeline, numBuckets, newKeyedCollections);
    }

    @Override
    public Pipeline getPipeline() {
      return pipeline;
    }

    @Override
    public Map<TupleTag<?>, PValue> expand() {
      final Map<TupleTag<?>, PValue> map = new HashMap<>();

      final List<KV<Integer, List<BucketSourceIterator<K>>>> readers = new ArrayList<>();
      for (int i = 0; i < numBuckets; i++) {
        try {
          readers.add(KV.of(i, createReaders(i)));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      final Coder<KV<Integer, List<BucketSourceIterator<K>>>> coder =
          KvCoder.of(VarIntCoder.of(), ListCoder.of(new BucketSourceIteratorCoder<K>()));

      map.put(
          new TupleTag<K>("readers"),
          pipeline.apply("Create bucket readers", Create.of(readers).withCoder(coder)));

      return map;
    }

    private static <K> List<KeyedBucketSource<K, ?>> copyAddLast(
        List<KeyedBucketSource<K, ?>> keyedCollections, KeyedBucketSource<K, ?> taggedCollection) {
      final List<KeyedBucketSource<K, ?>> copy = new ArrayList<>(keyedCollections);
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

    private List<BucketSourceIterator<K>> createReaders(Integer bucket) {
      final List<BucketSourceIterator<K>> readers = new ArrayList<>();

      for (KeyedBucketSource<K, ?> source : sources) {
        final BucketMetadata<K, Object> metadata = source.readMetadata();
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
