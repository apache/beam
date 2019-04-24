package org.apache.beam.sdk.extensions.smb;

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.util.ArrayList;
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
import org.apache.beam.sdk.extensions.smb.SMBFilenamePolicy.FileAssignment;
import org.apache.beam.sdk.extensions.smb.SortedBucketFile.Reader;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.KeyedBucketSources;
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.KeyedBucketSources.KeyedBucketSource;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions;

public class SortedBucketSource<KeyT> extends PTransform<
    KeyedBucketSources<KeyT>, PCollection<KV<KeyT, SMBJoinResult>>> {

  private final Coder<KeyT> keyCoder;

  SortedBucketSource(Coder<KeyT> keyCoder) {
    this.keyCoder = keyCoder;
  }

  @Override
  public PCollection<KV<KeyT, SMBJoinResult>> expand(KeyedBucketSources<KeyT> sources) {

    // validate that all sources are compatible
    // Verify: what's the call site of this? does it need to be put in a dofn?
    // @Todo metadata is being fetched twice per source
    BucketMetadata<KeyT, ?> first = null;
    for (KeyedBucketSource<KeyT, ?> source : sources.getSources()) {
      BucketMetadata<KeyT, ?> current = source.readMetadata();
      if (first == null) {
        first = current;
      } else {
        Preconditions.checkState(first.compatibleWith(current));
      }
    }

    final PCollection<KV<Integer, List<BucketSourceIterator<KeyT>>>> openedReaders =
        (PCollection<KV<Integer, List<BucketSourceIterator<KeyT>>>>) sources
            .expand().get(new TupleTag<>("readers"));

    return openedReaders
        .apply(Reshuffle.viaRandomKey())
        .apply(ParDo.of(new MergeBuckets<>()))
        .setCoder(KvCoder.of(keyCoder, new SMBJoinResult.SMBJoinResultCoder()));
  }

  /**
   *
   * @param <KeyT>
   */
  static class MergeBuckets<KeyT> extends
      DoFn<KV<Integer, List<BucketSourceIterator<KeyT>>>, KV<KeyT, SMBJoinResult>> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      List<BucketSourceIterator<KeyT>> readers = c.element().getValue();
      readers.forEach(BucketSourceIterator::initialize);

      Map<TupleTag, Iterator<?>> valueMap = new HashMap<>();
      KeyT keyForGroup = null;

      // @Todo Actuall implement this, keeping track of min key seen etc...
      Iterator<BucketSourceIterator<KeyT>> iterators = readers.iterator();

      while (iterators.hasNext()) {
        BucketSourceIterator<KeyT> bucketIt = iterators.next();
        TupleTag tupleTag = bucketIt.getTupleTag();
        KV<KeyT, Iterator<?>> next = bucketIt.nextKeyGroup();

        if (next == null) {
          iterators.remove();
          continue;
        }

        if (keyForGroup == null) {
          keyForGroup = next.getKey();
        }

        valueMap.put(tupleTag, next.getValue());
      };

      c.output(KV.of(keyForGroup, new SMBJoinResult(valueMap)));
      valueMap = new HashMap<>();

      readers.forEach(r -> {
        try {
          r.finish();
        } catch (Exception e) {
          throw new RuntimeException("Closing reader failed: {}", e);
        }
      });

    }
  }

  /**
   * Maintains type information about a possibly heterogeneous list of sources
   * by wrapping each one in a KeyedBucketSource object with TupleTag.
   * Heavily copied from CoGroupByKey implementation.
   * @param <K>
   */
  static class KeyedBucketSources<K> implements PInput, Serializable {
    private transient Pipeline pipeline;
    private transient List<KeyedBucketSource<K, ?>> sources;
    private Integer numBuckets;

    KeyedBucketSources(Pipeline pipeline, Integer numBuckets) {
      this(pipeline, numBuckets, new ArrayList<>());
    }
    KeyedBucketSources(
        Pipeline pipeline,
        Integer numBuckets,
        List<KeyedBucketSource<K, ?>> sources) {
      this.pipeline = pipeline;
      this.numBuckets = numBuckets;
      this.sources = sources;
    }

    List<KeyedBucketSource<K, ?>> getSources() {
      return sources;
    }

    public static <K, InputT> KeyedBucketSources<K> of(
        Pipeline pipeline, int numBuckets, KeyedBucketSource<K, InputT> source) {
      return new KeyedBucketSources<K>(pipeline, numBuckets).and(source);
    }

    public <V> KeyedBucketSources<K> and(KeyedBucketSource<K, V> source) {
      List<KeyedBucketSource<K, ?>> newKeyedCollections = copyAddLast(sources, source);

      return new KeyedBucketSources<K>(pipeline, numBuckets, newKeyedCollections);
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

      final Map<String, Coder<Reader>> coders = new HashMap<>();

      for (KeyedBucketSource source : sources) {
        coders.put(source.tupleTag.getId(), source.reader.coder());
      }

      final Coder<KV<Integer, List<BucketSourceIterator<K>>>> coder = KvCoder.of(
          VarIntCoder.of(),
          ListCoder.of(new BucketSourceIteratorCoder<K>(coders)));

      map.put(
          new TupleTag<K>("readers"),
          pipeline.apply("Create bucket readers", Create.of(readers).withCoder(coder))
      );

      return map;
    }

    private static <K> List<KeyedBucketSource<K, ?>> copyAddLast(
        List<KeyedBucketSource<K, ?>> keyedCollections,
        KeyedBucketSource<K, ?> taggedCollection) {
      final List<KeyedBucketSource<K, ?>> retval = new ArrayList<>(keyedCollections);
      retval.add(taggedCollection);
      return retval;
    }

    static class KeyedBucketSource<K, V> implements Serializable {
      final TupleTag<V> tupleTag;
      final FileAssignment fileAssignment;
      final Reader<V> reader;
      transient BucketMetadata<K, Object> metadata;

      public KeyedBucketSource(TupleTag<V> tupleTag, FileAssignment fileAssignment,
          Reader<V> reader) {
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
            metadata = BucketMetadata
                .from(Channels.newInputStream(FileSystems.open(fileAssignment.forMetadata())));
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

        ResourceId resourceId = source.fileAssignment.forBucketShard(bucket, metadata.getNumBuckets(), 1, 1);
        readers.add(new BucketSourceIterator<K>(reader, resourceId, source.tupleTag, metadata));
      }
      return readers;
    }
  }
}
