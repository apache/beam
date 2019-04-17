package org.apache.beam.sdk.extensions.smb;

import java.nio.channels.WritableByteChannel;
import java.nio.file.FileSystems;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.DefaultFilenamePolicy;
import org.apache.beam.sdk.io.DefaultFilenamePolicy.Params;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink.OutputFileHints;
import org.apache.beam.sdk.io.LocalResources;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.junit.Test;

// Just an example usage...
public class SortedBucketSinkTest {

  class UserBean {
    String name;
    int age;
  }

  class UserBeanBucketingMetadata extends BucketingMetadata<String, UserBean> {

    @Override
    public Coder<BucketingMetadata> getCoder() {
      return null;
    }

    @Override
    public Integer assignBucket(UserBean user) {
      return user.age % getNumBuckets();
    }

    @Override
    public String extractSortingKey(UserBean user) {
      return user.name;
    }

    @Override
    public Coder<String> getSortingKeyCoder() {
      return StringUtf8Coder.of();
    }

    @Override
    public Integer getNumBuckets() {
      return 10;
    }

    @Override
    public ResourceId getMetadataResource() {
      return LocalResources.fromPath(
          FileSystems.getDefault().getPath("tmp", "metadata.json"),
          false
      );
    }
  }

  class TestSortedBucketSinkImpl extends SortedBucketSink<String, UserBean> {

    public TestSortedBucketSinkImpl(
        BucketingMetadata<String, UserBean> bucketingMetadata,
        FilenamePolicy filenamePolicy,
        OutputFileHints outputFileHints,
        ValueProvider<ResourceId> tempDirectoryProvider) {
      super(bucketingMetadata, filenamePolicy, outputFileHints, tempDirectoryProvider);
    }

    @Override
    Writer<UserBean> createWriter() {
      return new Writer<UserBean>() {
        @Override
        void open(WritableByteChannel channel) throws Exception {

        }

        @Override
        void append(UserBean value) throws Exception {

        }

        @Override
        void close() throws Exception {

        }
      };
    }
  }

  @Test
  public void testSink() throws Exception {

    TestSortedBucketSinkImpl sink = new TestSortedBucketSinkImpl(
        new UserBeanBucketingMetadata(),
        DefaultFilenamePolicy.fromParams(new Params()),
        new OutputFileHints() {
          @Override
          public String getMimeType() {
            return "application/json";
          }

          @Override
          public String getSuggestedFilenameSuffix() {
            return ".json";
          }
        },
        StaticValueProvider.of(null)
    );

    // @Todo...
  }
}