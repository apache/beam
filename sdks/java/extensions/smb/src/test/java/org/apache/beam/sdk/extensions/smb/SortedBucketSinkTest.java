package org.apache.beam.sdk.extensions.smb;

import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.io.DefaultFilenamePolicy;
import org.apache.beam.sdk.io.DefaultFilenamePolicy.Params;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink.OutputFileHints;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.junit.Test;

import java.nio.channels.WritableByteChannel;

// Just an example usage...
public class SortedBucketSinkTest {

  class UserBean {
    String name;
    int age;
  }

  class UserBeanBucketMetadata extends BucketMetadata<String, UserBean> {

    UserBeanBucketMetadata() throws CannotProvideCoderException {
      super(10, String.class, HashType.MURMUR3_32);
    }

    @Override
    public String extractSortingKey(UserBean user) {
      return user.name;
    }
  }

  class TestSortedBucketSinkImpl extends SortedBucketSink<String, UserBean> {

    public TestSortedBucketSinkImpl(
        BucketMetadata<String, UserBean> bucketingMetadata,
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
        new UserBeanBucketMetadata(),
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