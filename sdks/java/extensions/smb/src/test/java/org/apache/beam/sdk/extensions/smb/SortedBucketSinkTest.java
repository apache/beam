package org.apache.beam.sdk.extensions.smb;

import java.io.PrintWriter;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.FileSystems;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.DefaultFilenamePolicy;
import org.apache.beam.sdk.io.DefaultFilenamePolicy.Params;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink.OutputFileHints;
import org.apache.beam.sdk.io.LocalResources;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.junit.Test;

// Just an example usage...
public class SortedBucketSinkTest {

  class UserBean {
    String name;
    int age;
  }

  class UserBeanBucketingMetadata extends BucketingMetadata<String, UserBean> {

    @Override
    public Coder<BucketingMetadata<String, UserBean>> getCoder() {
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
        OutputFileHints outputFileHints) {
      super(bucketingMetadata, filenamePolicy, outputFileHints);
    }

    @Override
    WriteFn<UserBean> createWriteFn() {
      return new WriteFn<UserBean>() {
        PrintWriter printWriter;

        @Override
        void openWriter(WritableByteChannel channel) throws Exception {
          printWriter = new PrintWriter(Channels.newOutputStream(channel));
        }

        @Override
        void writeValue(UserBean value) throws Exception {
          printWriter.append(value.toString());
        }

        @Override
        void closeWriter() throws Exception {
          printWriter.close();
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
          @Nullable
          @Override
          public String getMimeType() {
            return "application/json";
          }

          @Nullable
          @Override
          public String getSuggestedFilenameSuffix() {
            return ".json";
          }
        }
    );

     // @Todo...
  }
}
