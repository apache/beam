package org.apache.beam.sdk.extensions.smb;

import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SortedBucketSource<KeyT, ResultT>
    extends PTransform<PBegin, PCollection<KV<KeyT, ResultT>>> {

  // @Todo: PTransform has dummy ser/de methods, this will not survive closure
  private List<BucketSource<KeyT, Object>> sources;

  @Override
  public PCollection<KV<KeyT, ResultT>> expand(PBegin input) {
    input
        // wrap List in Optional so the item stays together
        .apply(Create.of(Optional.of(sources)))
        // read all metadata once to prepare buckets
        .apply(ParDo.of(new PrepareBuckets<>()))
        // force each (bucketId -> List<BucketSource>) pair to a different core
        // @Todo: is this guaranteed? Also Reshuffle is deprecated
        .apply(Reshuffle.viaRandomKey());
        // @Todo: merge join transform
    return null;
  }

  private static class PrepareBuckets<KeyT> extends DoFn<
      Optional<List<BucketSource<KeyT, Object>>>,
      KV<Integer, List<BucketSource<KeyT, Object>>>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      List<BucketSource<KeyT, Object>> sources = c.element().get();

      // validate that all sources are compatible
      BucketMetadata<KeyT, Object> first = null;
      for (BucketSource<KeyT, Object> source : sources) {
        BucketMetadata<KeyT, Object> current = source.readMetadata();
        if (first == null) {
          first = current;
        } else {
          Preconditions.checkState(first.compatibleWith(current));;
        }
      }

      // emit the same source list once for each bucket
      for (int i = 0; i < first.getNumBuckets(); i++) {
        c.output(KV.of(i, sources));
      }
    }
  }

  private static class MergeBuckets<KeyT, ResultT>
      extends DoFn<KV<Integer, List<BucketSource<KeyT, Object>>>, KV<KeyT, ResultT>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      KV<Integer, List<BucketSource<KeyT, Object>>> input = c.element();
      int bucketId = input.getKey();
      List<BucketSource<KeyT, Object>> sources = input.getValue();
      List<BucketSourceReader<KeyT, Object>> readers = sources.stream()
          .map(s -> s.getReader(bucketId))
          .collect(Collectors.toList());
      // @Todo: align keys from reader::nextKeyGroup and merge
    }
  }

  ////////////////////////////////////////
  // Helper classes
  ////////////////////////////////////////

  // @Todo: implement
  // @Todo: this goes in a PCollection, needs Coder, or is SerializableCoder OK?
  // acts like TupleTag<T> in GBK, helps us to regain types of join results
  // also abstracts away file operation and key/value iteration
  public static abstract class BucketSource<KeyT, ValueT> implements Serializable {

    // encapsulate file path, Avro schema, etc.
    private String filePrefix;
    private String fileSuffix;
    private SortedBucketFile<ValueT> file;

    public BucketMetadata<KeyT, ValueT> readMetadata() {
      try {
        ResourceId id = FileSystems.matchSingleFileSpec(filePrefix + "metadata.json").resourceId();
        return BucketMetadata.from(Channels.newInputStream(FileSystems.open(id)));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    // extract the next key and all values as a lazy iterable
    public BucketSourceReader<KeyT, ValueT> getReader(int bucketId) {
      String spec = String.format("%s-bucket-%05d.%s", filePrefix, bucketId, fileSuffix);
      try {
        ResourceId resourceId = FileSystems.matchSingleFileSpec(spec).resourceId();
        SortedBucketFile.Reader<ValueT> reader = file.createReader();
        reader.prepareRead(FileSystems.open(resourceId));
        return new BucketSourceReader<>(reader, readMetadata()::extractSortingKey);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static class BucketSourceReader<KeyT, ValueT> {
    private final SortedBucketFile.Reader<ValueT> reader;
    private final Function<ValueT, KeyT> keyFn;
    private KV<KeyT, ValueT> nextKv;

    public BucketSourceReader(SortedBucketFile.Reader<ValueT> reader,
                              Function<ValueT, KeyT> keyFn)
        throws Exception {
      this.reader = reader;
      this.keyFn = keyFn;

      ValueT value = reader.read();
      if (value == null) {
        nextKv = null;
      } else {
        nextKv = KV.of(keyFn.apply(value), value);
      }
    }

    // group next continuous values of the same key in an iterator
    public KV<KeyT, Iterator<ValueT>> nextKeyGroup() {
      if (nextKv == null) {
        return null;
      } else {
        Iterator<ValueT> iterator = new Iterator<ValueT>() {
          private KeyT key = nextKv.getKey();
          private ValueT value = nextKv.getValue();

          @Override
          public boolean hasNext() {
            return value != null;
          }

          @Override
          public ValueT next() {
            try {
              ValueT result = value;
              ValueT v = reader.read();
              if (v == null) {
                // end of file, reset outer
                value = null;
                nextKv = null;
              } else {
                KeyT k = keyFn.apply(v);
                if (key.equals(k)) {
                  // same key, update next value
                  value = v;
                } else {
                  // end of group, advance outer
                  value = null;
                  nextKv = KV.of(k, v);
                }
              }
              return result;
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }
        };

        return KV.of(nextKv.getKey(), iterator);
      }
    }
  }

  public static class Result {
  }
}
