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
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.BucketMetadataCoder;
import org.apache.beam.sdk.extensions.smb.FileOperations.Reader;
import org.apache.beam.sdk.extensions.smb.SMBCoGbkResult.ToFinalResult;
import org.apache.beam.sdk.extensions.smb.SMBFilenamePolicy.FileAssignment;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.BucketedInputs.BucketedInput;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.BucketedInputs.BucketedInput.BucketedInputCoder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.fs.ResourceIdCoder;
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
import org.apache.beam.vendor.guava.v20_0.com.google.common.primitives.UnsignedBytes;

/**
 * Reads in an arbitrary number of heterogeneous typed Sources stored with the same bucketing
 * scheme, and co-groups on a common key.
 *
 * @param <FinalKeyT>
 * @param <FinalResultT>
 */
public class SortedBucketSource<FinalKeyT, FinalResultT>
    extends PTransform<PBegin, PCollection<KV<FinalKeyT, FinalResultT>>> {

  private final transient List<BucketedInput<?, ?>> sources;
  private final Class<FinalKeyT> resultKeyClass;
  private final ToFinalResult<FinalResultT> toFinalResult;

  public SortedBucketSource(
      List<BucketedInput<?, ?>> sources,
      Class<FinalKeyT> resultKeyClass,
      ToFinalResult<FinalResultT> toFinalResult) {
    this.sources = sources;
    this.resultKeyClass = resultKeyClass;
    this.toFinalResult = toFinalResult;
  }

  @SuppressWarnings({"deprecation", "unchecked"})
  @Override
  public final PCollection<KV<FinalKeyT, FinalResultT>> expand(PBegin begin) {
    Preconditions.checkState(sources.size() > 1, "Must have more than one Source");

    BucketMetadata<?, ?> first = null;
    Coder<FinalKeyT> resultKeyCoder = null;

    for (BucketedInput<?, ?> source : sources) {
      final BucketMetadata<?, ?> current = source.getMetadata();
      if (first == null) {
        first = current;
      } else {
        Preconditions.checkState(first.compatibleWith(current));
      }
      if (current.getKeyClass() == resultKeyClass && resultKeyCoder == null) {
        try {
          resultKeyCoder = (Coder<FinalKeyT>) current.getKeyCoder();
        } catch (CannotProvideCoderException e) {
          throw new RuntimeException("Could not provide a coder for key type", e);
        }
      }
    }

    Preconditions.checkNotNull(resultKeyCoder, "Couldn't infer a matching Coder for FinalKeyT");

    // @TODO: Support asymmetric, but still compatible, bucket sizes in reader.
    final int numBuckets = first.getNumBuckets();

    final PCollection<KV<Integer, List<BucketedInput<?, ?>>>> bucketedInputs =
        (PCollection<KV<Integer, List<BucketedInput<?, ?>>>>)
            new BucketedInputs(begin.getPipeline(), numBuckets, sources)
                .expand()
                .get(new TupleTag<>("BucketedSources"));

    return bucketedInputs
        .apply("ReshuffleKeys", Reshuffle.viaRandomKey())
        .apply("MergeBuckets", ParDo.of(new MergeBuckets<>(resultKeyCoder, toFinalResult)))
        .setCoder(KvCoder.of(resultKeyCoder, toFinalResult.resultCoder()));
  }

  /** @param <FinalKeyT> */
  static class MergeBuckets<FinalKeyT, FinalResultT>
      extends DoFn<KV<Integer, List<BucketedInput<?, ?>>>, KV<FinalKeyT, FinalResultT>> {

    private static final Comparator<Map.Entry<TupleTag, KV<byte[], Iterator<?>>>> keyComparator =
        (l, r) ->
            UnsignedBytes.lexicographicalComparator()
                .compare(l.getValue().getKey(), r.getValue().getKey());

    private final Coder<FinalKeyT> keyCoder;
    private final ToFinalResult<FinalResultT> toResult;

    MergeBuckets(Coder<FinalKeyT> keyCoder, ToFinalResult<FinalResultT> toResult) {
      this.toResult = toResult;
      this.keyCoder = keyCoder;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      int bucket = c.element().getKey();
      final List<BucketedInput<?, ?>> sources = new ArrayList<>(c.element().getValue());

      List<BucketedInputIterator<?, ?>> readers =
          sources.stream().map(s -> s.createIterator(bucket)).collect(Collectors.toList());

      final Map<TupleTag, KV<byte[], Iterator<?>>> nextKeyGroups = new HashMap<>();

      while (true) {
        Iterator<BucketedInputIterator<?, ?>> readersIt = readers.iterator();

        while (readersIt.hasNext()) {
          final BucketedInputIterator reader = readersIt.next();

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
        final FinalKeyT groupKey;
        try {
          groupKey = keyCoder.decode(groupKeyBytes);
        } catch (Exception e) {
          throw new RuntimeException("Couldn't decode key bytes for group: {}", e);
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
   * in a BucketedInput object with TupleTag. Heavily copied from CoGroupByKey implementation.
   */
  public static class BucketedInputs implements org.apache.beam.sdk.values.PInput {
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

    @SuppressWarnings("unchecked")
    @Override
    public Map<TupleTag<?>, PValue> expand() {
      final Map<TupleTag<?>, PValue> map = new HashMap<>();

      final List<KV<Integer, List<BucketedInput<?, ?>>>> bucketedSources = new ArrayList<>();
      for (int i = 0; i < numBuckets; i++) {
        try {
          bucketedSources.add(KV.of(i, sources));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      final KvCoder<Integer, List<BucketedInput<?, ?>>> coder =
          KvCoder.of(VarIntCoder.of(), ListCoder.of(new BucketedInputCoder()));

      map.put(
          new TupleTag<>("BucketedSources"),
          pipeline.apply("CreateBucketedSources", Create.of(bucketedSources).withCoder(coder)));

      return map;
    }

    /**
     * Represents a single source with values V.
     *
     * @param <K>
     * @param <V>
     */
    public static class BucketedInput<K, V> {
      final TupleTag<V> tupleTag;
      final ResourceId filenamePrefix;
      final String filenameSuffix;
      final Reader<V> reader;

      transient FileAssignment fileAssignment;
      transient BucketMetadata<K, V> metadata;

      public BucketedInput(
          TupleTag<V> tupleTag,
          ResourceId filenamePrefix,
          String filenameSuffix,
          Reader<V> reader) {
        this.tupleTag = tupleTag;
        this.filenamePrefix = filenamePrefix;
        this.filenameSuffix = filenameSuffix;
        this.reader = reader;

        this.fileAssignment = new FileAssignment(filenamePrefix, filenameSuffix);
      }

      BucketedInput(
          TupleTag<V> tupleTag,
          ResourceId filenamePrefix,
          String filenameSuffix,
          Reader<V> reader,
          BucketMetadata<K, V> metadata) {
        this(tupleTag, filenamePrefix, filenameSuffix, reader);
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

      BucketedInputIterator<K, V> createIterator(int bucket) {
        int numBuckets = getMetadata().getNumBuckets();
        ResourceId file = fileAssignment.forBucket(bucket, numBuckets);
        return new BucketedInputIterator<>(reader, file, tupleTag, getMetadata());
      }

      static class BucketedInputCoder<K, V> extends AtomicCoder<BucketedInput<K, V>> {

        private static SerializableCoder<TupleTag> tupleTagCoder =
            SerializableCoder.of(TupleTag.class);
        private static ResourceIdCoder resourceIdCoder = ResourceIdCoder.of();
        private static StringUtf8Coder stringCoder = StringUtf8Coder.of();
        private static SerializableCoder<Reader> readerCoder = SerializableCoder.of(Reader.class);
        private BucketMetadataCoder<K, V> metadataCoder = new BucketMetadataCoder<>();

        @Override
        public void encode(BucketedInput<K, V> value, OutputStream outStream)
            throws CoderException, IOException {
          tupleTagCoder.encode(value.tupleTag, outStream);
          resourceIdCoder.encode(value.filenamePrefix, outStream);
          stringCoder.encode(value.filenameSuffix, outStream);
          readerCoder.encode(value.reader, outStream);
          metadataCoder.encode(value.getMetadata(), outStream);
        }

        @SuppressWarnings("unchecked")
        @Override
        public BucketedInput<K, V> decode(InputStream inStream) throws CoderException, IOException {
          TupleTag<V> tupleTag = (TupleTag<V>) tupleTagCoder.decode(inStream);
          ResourceId filenamePrefix = resourceIdCoder.decode(inStream);
          String filenameSuffix = stringCoder.decode(inStream);
          Reader<V> reader = readerCoder.decode(inStream);
          BucketMetadata<K, V> metadata = metadataCoder.decode(inStream);
          return new BucketedInput<>(tupleTag, filenamePrefix, filenameSuffix, reader, metadata);
        }
      }
    }
  }
}
