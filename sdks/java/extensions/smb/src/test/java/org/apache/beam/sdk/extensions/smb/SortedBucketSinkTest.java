
package org.apache.beam.sdk.extensions.smb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;
import org.apache.beam.sdk.extensions.smb.SortedBucketFile.Writer;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink.WriteResult;
import org.apache.beam.sdk.extensions.smb.avro.AvroBucketMetadata;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.LocalResources;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.fs.ResourceIdCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Supplier;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

// Just an example usage...
public class SortedBucketSinkTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public final TemporaryFolder outputFolder = new TemporaryFolder();
  @Rule public final TemporaryFolder tmpFolder = new TemporaryFolder();

  private static final AvroBucketMetadata<Integer> METADATA = TestUtils
      .tryCreateMetadata(1, HashType.MURMUR3_32);

  private static class SerializableWriterSupplier implements Serializable, Supplier<Writer<GenericRecord>> {
    private SerializableWriterSupplier() {
    }

    @Override
    public Writer<GenericRecord> get() {
      return new Writer<GenericRecord>() {
        private AvroCoder coder = AvroCoder.of(TestUtils.schema);
        private OutputStream channel;

        @Override
        public String getMimeType() {
          return MimeTypes.BINARY;
        }

        @Override
        public void prepareWrite(WritableByteChannel channel) throws Exception {
          this.channel = Channels.newOutputStream(channel);
        }

        @Override
        public void write(GenericRecord value) throws Exception {
          coder.encode(value, channel);
        }

        @Override
        public void finishWrite() throws Exception {
          channel.close();
        }
      };
    }
  }

  class TestSortedBucketSinkImpl extends SortedBucketSink<Integer, GenericRecord> {
    TestSortedBucketSinkImpl() {
      super(METADATA,
          new SMBFilenamePolicy(
              LocalResources.fromFile(outputFolder.getRoot(), true),
              ".avro",
              LocalResources.fromFile(tmpFolder.getRoot(), true)),
          new SerializableWriterSupplier());
    }
  }

  @Test
  public void testWriteMetadata() throws  Exception {
    final ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.writeValue(new File("src/test/resources/source1", "metadata.json"),
        TestUtils.tryCreateMetadata(10, HashType.MURMUR3_32));
  }

  static final GenericRecord user1 = TestUtils.createUserRecord("a", 50);
  static final GenericRecord user2 = TestUtils.createUserRecord("b", 75);
  static final GenericRecord user3 = TestUtils.createUserRecord("c", 25);

  @Test
  public void testSink() throws Exception {
    TestSortedBucketSinkImpl sink = new TestSortedBucketSinkImpl();
    final PCollection<GenericRecord> users = pipeline
        .apply(Create.of(Lists.newArrayList(user1, user2, user3)).withCoder(TestUtils.userCoder));

    WriteResult writeResult = users.apply("test sink", sink);

    PCollection<ResourceId> writtenMetadata =
        (PCollection<ResourceId>) writeResult.expand().get(new TupleTag<>("SMBMetadataWritten"));

    PAssert.that(writtenMetadata).satisfies(m -> {
      final ResourceId metadataFile = m.iterator().next();
      final ObjectMapper objectMapper = new ObjectMapper();
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
        final InputStream is = Channels.newInputStream(channel);

        Assert.assertEquals(user3, TestUtils.userCoder.decode(is));
        Assert.assertEquals(user1, TestUtils.userCoder.decode(is));
        Assert.assertEquals(user2, TestUtils.userCoder.decode(is));
        channel.close();
      } catch (IOException e) {
        Assert.fail(String.format("Failed to read written bucket file: %s", e));
      }
      return null;
    });

    pipeline.run();
  }
}