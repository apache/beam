
package org.apache.beam.sdk.extensions.smb;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.extensions.smb.SortedBucketSink.WriteResult;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.LocalResources;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.util.MimeTypes;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

// Just an example usage...
public class SortedBucketSinkTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public final TemporaryFolder outputFolder = new TemporaryFolder();
  @Rule public final TemporaryFolder tmpFolder = new TemporaryFolder();

  static class UserBucketingMetadata extends BucketMetadata<Integer, KV<String, Integer>> {
    static UserBucketingMetadata getInstance() {
        try {
          return new UserBucketingMetadata();
        } catch (Exception e) {
          throw new RuntimeException();
        }
    }

    private UserBucketingMetadata() throws CannotProvideCoderException {
      super(2, Integer.class, HashType.MURMUR3_32);
    }

    @Override
    public Integer extractSortingKey(KV<String, Integer> value) {
      return value.getValue(); // Sort by age
    }
  }

  private static final UserBucketingMetadata METADATA = UserBucketingMetadata.getInstance();

  class TestSortedBucketSinkImpl extends SortedBucketSink<Integer, KV<String, Integer>> {
    TestSortedBucketSinkImpl() {
      super(METADATA,
          new SMBFilenamePolicy(
              LocalResources.fromFile(outputFolder.getRoot(), true),
              ".txt",
              LocalResources.fromFile(tmpFolder.getRoot(), true),
              METADATA.getNumBuckets()),
          (Void v) -> new SortedBucketFile.Writer<KV<String, Integer>>() {
            private StringUtf8Coder coder = StringUtf8Coder.of();
            private OutputStream channel;

            @Override
            public String getMimeType() {
              return MimeTypes.TEXT;
            }

            @Override
            public void prepareWrite(WritableByteChannel channel) throws Exception {
              this.channel = Channels.newOutputStream(channel);
            }

            @Override
            public void write(KV<String, Integer> value) throws Exception {
              coder.encode(value.getKey() + value.getValue().toString(), channel);
            }

            @Override
            public void finishWrite() throws Exception {
              channel.close();
            }
          });
    }
  }

  @Test
  public void testSink() throws Exception{
    TestSortedBucketSinkImpl sink = new TestSortedBucketSinkImpl();

    final PCollection<KV<String, Integer>> users = pipeline
        .apply(Create.of(ImmutableList.of(KV.of("foo", 50), KV.of("foo", 25))))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()));

    WriteResult writeResult = users.apply("test sink", sink);

    PCollection<ResourceId> writtenMetadata =
        (PCollection<ResourceId>) writeResult.expand().get(new TupleTag<>("SMBMetadataWritten"));

    PAssert.that(writtenMetadata).satisfies(m -> {
      final ResourceId metadataFile = m.iterator().next();
      final ObjectMapper objectMapper = new ObjectMapper();
      try {
        final UserBucketingMetadata readMetadata = objectMapper
            .readValue(Channels.newInputStream(FileSystems.open(metadataFile)),
            UserBucketingMetadata.class);

        Assert.assertEquals(readMetadata, METADATA);
      } catch (IOException e) {
        Assert.fail(String.format("Failed to read written metadata file: %s", e));
      }

      return null;
    });

    PCollection<KV<Integer, ResourceId>> writtenBuckets =
        (PCollection<KV<Integer, ResourceId>>) writeResult.expand().get(new TupleTag<>("SortedBucketsWritten"));

    PAssert.that(writtenBuckets).satisfies(b -> {
      final KV<Integer, ResourceId> bucketFile = b.iterator().next();

      Assert.assertTrue(1 == bucketFile.getKey());

      try {
        final InputStream is = Channels.newInputStream(FileSystems.open(bucketFile.getValue()));
        Assert.assertEquals("foo25", StringUtf8Coder.of().decode(is));
        Assert.assertEquals("foo50", StringUtf8Coder.of().decode(is));
      } catch (IOException e) {
        Assert.fail(String.format("Failed to read written bucket file: %s", e));
      }
      return null;
    });

    pipeline.run();
  }
}