
package org.apache.beam.sdk.extensions.smb.avro;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.smb.BucketMetadata;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink.WriteResult;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.LocalResources;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.fs.ResourceIdCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

// Just an example usage...
public class AvroSortedBucketSinkTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public final TemporaryFolder outputFolder = new TemporaryFolder();
  @Rule public final TemporaryFolder tmpFolder = new TemporaryFolder();

  private static final AvroBucketMetadata<Integer> METADATA = TestUtils
      .tryCreateMetadata(1, HashType.MURMUR3_32);

  private static final GenericRecord user1 = TestUtils.createUserRecord("d", 50);
  private static final GenericRecord user2 = TestUtils.createUserRecord("e", 75);
  private static final GenericRecord user3 = TestUtils.createUserRecord("f", 25);

  @Test
  public void testSink() throws Exception {
    SortedBucketSink<Integer, GenericRecord> sink = AvroSortedBucketIO.sink(
        METADATA, LocalResources.fromFile(outputFolder.getRoot(), true),
        LocalResources.fromFile(tmpFolder.getRoot(), true), null, TestUtils.schema
    );

    final PCollection<GenericRecord> users = pipeline
        .apply(Create.of(Lists.newArrayList(user1, user2, user3)).withCoder(TestUtils.userCoder));

    WriteResult writeResult = users.apply("test sink", sink);

    PCollection<ResourceId> writtenMetadata =
        (PCollection<ResourceId>) writeResult.expand().get(new TupleTag<>("SMBMetadataWritten"));

    PAssert.that(writtenMetadata).satisfies(m -> {
      final ResourceId metadataFile = m.iterator().next();
      try {
        final BucketMetadata<Integer, Object> readMetadata = BucketMetadata
            .from(Channels.newInputStream(FileSystems.open(metadataFile)));

        Assert.assertTrue(readMetadata.compatibleWith(METADATA));
      } catch (IOException e) {
        Assert.fail(String.format("Failed to read written metadata file: %s", e));
      }

      return null;
    });

    PCollection<KV<Integer, ResourceId>> writtenBuckets =
        (PCollection<KV<Integer, ResourceId>>) writeResult
            .expand().get(new TupleTag<>("SortedBucketsWritten"));

    PAssert.that(writtenBuckets
        .setCoder(KvCoder.of(VarIntCoder.of(), ResourceIdCoder.of())))
        .satisfies(b -> {
      final KV<Integer, ResourceId> bucketFile = b.iterator().next();
      Assert.assertTrue(0 == bucketFile.getKey());

      try {
        final ReadableByteChannel channel = FileSystems.open(bucketFile.getValue());
        final DataFileStream<GenericRecord> reader = new DataFileStream<>(
            Channels.newInputStream(channel), new GenericDatumReader<>(TestUtils.schema));

        Assert.assertEquals(user3, reader.next());
        Assert.assertEquals(user1, reader.next());
        Assert.assertEquals(user2, reader.next());

        Assert.assertFalse(reader.hasNext());
        reader.close();
      } catch (IOException e) {
        Assert.fail(String.format("Failed to read written bucket file: %s", e));
      }
      return null;
    });

    pipeline.run();
  }
}