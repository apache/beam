package org.apache.beam.sdk.extensions.smb;

import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.sorter.BufferedExternalSorter;
import org.apache.beam.sdk.extensions.sorter.SortValues;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink.OutputFileHints;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/**
 * Writes a PCollection representing sorted, bucketized data to files, where the # of files is
 * equal to the # of buckets, assuming the contents of each bucket fit on a single worker.
 *
 * This must be implemented for different file-based IO types i.e. Avro.
 */
public abstract class SortedBucketSink<SortingKeyT, ValueT> extends PTransform<PCollection<ValueT>, PDone> {

  private final BucketingMetadata<SortingKeyT, ValueT> bucketingMetadata;

  // @todo: custom SMB FilenamePolicy impl with bucketed shard name template?
  private final FilenamePolicy filenamePolicy;
  private final OutputFileHints outputFileHints;

  public SortedBucketSink(
      BucketingMetadata<SortingKeyT, ValueT> bucketingMetadata,
      FilenamePolicy filenamePolicy,
      OutputFileHints outputFileHints) {
    this.bucketingMetadata = bucketingMetadata;
    this.filenamePolicy = filenamePolicy;
    this.outputFileHints = outputFileHints;
  }

  abstract WriteFn<ValueT> createWriteFn();

  // @Todo: This assumes all buckets will fit in memory and write to a single file...
  // @Todo: improved sorting
  @Override
  public PDone expand(PCollection<ValueT> input) {
    return input
        .apply("Assign buckets", MapElements.via(new SimpleFunction<ValueT, KV<Integer, KV<SortingKeyT, ValueT>>>() {
          @Override
          public KV<Integer, KV<SortingKeyT, ValueT>> apply(ValueT record) {
            return KV.of(
                bucketingMetadata.assignBucket(record),
                KV.of(bucketingMetadata.extractSortingKey(record), record)
            );
          }
        }))
        .setCoder(
            KvCoder.of(
                VarIntCoder.of(),
                KvCoder.of(this.bucketingMetadata.getSortingKeyCoder(), input.getCoder())
            )
        )
        // @Todo: Hopefully the group by + sort steps fuse? Verify this?
        .apply("Group per bucket", GroupByKey.create())
        .apply("Sort values in bucket", SortValues.create(BufferedExternalSorter.options()))
        .apply("Write bucket data", new WriteOperation(createWriteFn()));
  }

  // @Todo: Retry policy, atomicity guarantees, sharding per bucket, etc...
  public class WriteOperation extends PTransform<PCollection<KV<Integer,
      Iterable<KV<SortingKeyT, ValueT>>>>, PDone> { // @Todo: Do we want some kind of SortedBucket wrapper class?
    private final WriteFn<ValueT> writeFn;

    WriteOperation(WriteFn<ValueT> writeFn) {
      this.writeFn = writeFn;
    }

    @Override
    public PDone expand(PCollection<KV<Integer, Iterable<KV<SortingKeyT, ValueT>>>> input) {
      return input
          .apply("Write all buckets", ParDo.of(writeFn))
          .apply("Write SMB metadata file", PTransform.compose((PCollection<Void> in) -> {
            WritableByteChannel writableByteChannel = null;
            try {
              writableByteChannel = FileSystems.create(
                  bucketingMetadata.getMetadataResource(),
                  MimeTypes.BINARY // @todo: allow customizable mime type? depends on metadata impl
              );

              bucketingMetadata
                  .getCoder()
                  .encode(bucketingMetadata, Channels.newOutputStream(writableByteChannel));
            } catch (IOException e) {
              throw new RuntimeException(String.format("Failed to write SMB metadata: %s", e));
            } finally {
              try {
                if (writableByteChannel != null) {
                  writableByteChannel.close();
                }
              } catch (Exception e) {
                throw new RuntimeException(
                    String.format("Failed to close writableByteChannel: %s", e));
              }
            }

            // @Todo maybe return TupleTags for buckets
            return PDone.in(input.getPipeline());
          }));
    }
  }

  abstract class WriteFn<V> extends DoFn<KV<Integer, Iterable<KV<SortingKeyT, V>>>, Void> {
    abstract void openWriter(WritableByteChannel channel) throws Exception;
    abstract void writeValue(V value) throws Exception;
    abstract void closeWriter() throws Exception;

    @ProcessElement
    public final Void processElement(ProcessContext context) throws Exception {
      final Iterable<KV<SortingKeyT, V>> values = context.element().getValue();
      final Integer bucket = context.element().getKey();

      if (values == null) {
        throw new IOException(
            String.format("Write failed: bucket %s is empty", context.element().getKey()));
      }

      openWriter(FileSystems.create(
          filenamePolicy.unwindowedFilename(bucket, bucketingMetadata.getNumBuckets(), outputFileHints),
          outputFileHints.getMimeType())
      );

      try {
        values.forEach(kv -> {
          try {
            writeValue(kv.getValue());
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to write element %s: %s", kv.getValue(), e));
          }
        });
      } finally {
        closeWriter();
      }

      // @Todo: this could return the bucketing key if we want to make some assertions later on
      //        successfully written buckets?
      return null;
    }
  }
}
