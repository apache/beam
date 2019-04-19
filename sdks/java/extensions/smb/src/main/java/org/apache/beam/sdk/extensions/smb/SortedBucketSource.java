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
import java.util.List;
import java.util.Optional;

public class SortedBucketSource<KeyT>
        extends PTransform<PBegin, PCollection<KV<KeyT, SortedBucketSource.Result>>> {

  // @Todo: PTransform has dummy ser/de methods, this will not survive closure
  private List<BucketSource<KeyT, Object>> sources;

  @Override
  public PCollection<KV<KeyT, Result>> expand(PBegin input) {
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
          Preconditions.checkState(first.getNumBuckets() == current.getNumBuckets());
          Preconditions.checkState(first.getSortingKeyClass().equals(current.getSortingKeyClass()));
          Preconditions.checkState(first.getHashType() == current.getHashType());
        }
      }

      // emit the same source list once for each bucket
      for (int i = 0; i < first.getNumBuckets(); i++) {
        c.output(KV.of(i, sources));
      }
    }
  }

  ////////////////////////////////////////
  // Helper classes
  ////////////////////////////////////////

  // @Todo: implement
  // @Todo: this goes in a PCollection, needs Coder, or is SerializableCoder OK?
  // acts like TupleTag<T> in GBK, helps us to regain types of join results
  // also abstracts away file operation and key/value iteration
  public static abstract class BucketSource<SortingKeyT, ValueT> implements Serializable {

    // encapsulate file path, Avro schema, etc.
    private String filePrefix;
    private SortedBucketFile<ValueT> file;

    // extract the next key and all values as a lazy iterable
    public abstract KV<SortingKeyT, Iterable<ValueT>> nextKeyValues();

    public BucketMetadata<SortingKeyT, ValueT> readMetadata() {
      try {
        ResourceId id = FileSystems.matchSingleFileSpec(filePrefix + "metadata.json").resourceId();
        return BucketMetadata.from(Channels.newInputStream(FileSystems.open(id)));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static abstract class Result {
  }
}
