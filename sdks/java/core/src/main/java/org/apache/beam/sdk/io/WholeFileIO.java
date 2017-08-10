package org.apache.beam.sdk.io;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions.RESOLVE_DIRECTORY;
import static org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions.RESOLVE_FILE;
import static org.apache.beam.sdk.util.MimeTypes.BINARY;

import com.google.auto.value.AutoValue;
import com.google.protobuf.ByteString;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.PDone;

/**
 * WholeFileIO.
 */
public class WholeFileIO {

  public static Read read() {
    return new AutoValue_WholeFileIO_Read.Builder().build();
  }

  public static Write write() {
    return new AutoValue_WholeFileIO_Write.Builder().build();
  }

  /**
   * Implements read().
   */
  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<KV<String, byte[]>>> {
    @Nullable
    abstract ValueProvider<String> getFilePattern();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setFilePattern(ValueProvider<String> filePattern);

      abstract Read build();
    }

    public Read from(String filePattern) {
      checkNotNull(filePattern, "FilePattern cannot be empty.");
      return from(ValueProvider.StaticValueProvider.of(filePattern));
    }

    public Read from(ValueProvider<String> filePattern) {
      checkNotNull(filePattern, "FilePattern cannot be empty.");
      return toBuilder().setFilePattern(filePattern).build();
    }

    @Override
    public PCollection<KV<String, byte[]>> expand(PBegin input) {
      checkNotNull(
          getFilePattern(),
          "Need to set the filePattern of a WholeFileIO.Read transform."
      );

      String filePattern = getFilePattern().get();

      PCollection<String> filePatternPCollection = input.apply(Create.of(filePattern));

      PCollection<ResourceId> resourceIds = filePatternPCollection.apply(
          ParDo.of(
              new DoFn<String, ResourceId>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                  String filePattern = c.element();
                  try {
                    List<MatchResult> matchResults = FileSystems.match(
                        Collections.singletonList(filePattern));
                    for (MatchResult matchResult : matchResults) {
                      List<MatchResult.Metadata> metadataList = matchResult.metadata();
                      for (MatchResult.Metadata metadata : metadataList) {
                        ResourceId resourceId = metadata.resourceId();
                        c.output(resourceId);
                      }
                    }
                  } catch (IOException e) {
                    e.printStackTrace();
                  }
                }
              }
          )
      );

      PCollection<KV<String, byte[]>> files = resourceIds.apply(
          ParDo.of(
              new DoFn<ResourceId, KV<String, byte[]>>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                  ResourceId resourceId = c.element();

                  try {
                    ReadableByteChannel channel = FileSystems.open(resourceId);
                    ByteBuffer byteBuffer = ByteBuffer.allocate(8192);
                    ByteString byteString = ByteString.EMPTY;

                    while (channel.read(byteBuffer) != -1) {
                      byteBuffer.flip();
                      byteString = byteString.concat(ByteString.copyFrom(byteBuffer));
                      byteBuffer.clear();
                    }

                    KV<String, byte[]> kv = KV.of(
                        resourceId.getFilename(),
                        byteString.toByteArray()
                    );

                    c.output(kv);
                  } catch (IOException e) {
                    e.printStackTrace();
                  }
                }
              }
          )
      );

      return files;
    }
  }

  /**
   * Implements write().
   */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<KV<String, byte[]>>, PDone> {

    @Nullable abstract ValueProvider<ResourceId> getOutputDir();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setOutputDir(ValueProvider<ResourceId> outputDir);

      abstract Write build();
    }

    public Write to(String outputDir) {
      return to(FileBasedSink.convertToFileResourceIfPossible(outputDir));
    }

    public Write to(ResourceId outputDir) {
      return toResource(ValueProvider.StaticValueProvider.of(outputDir));
    }

    public Write toResource(ValueProvider<ResourceId> outputDir) {
      return toBuilder().setOutputDir(outputDir).build();
    }

    @Override
    public PDone expand(PCollection<KV<String, byte[]>> input) {
      checkNotNull(
          getOutputDir(),
          "Need to set the output directory of a WholeFileIO.Write transform."
      );

      ResourceId outputDir = getOutputDir().get();
      if (!outputDir.isDirectory()) {
        outputDir = outputDir.getCurrentDirectory()
                             .resolve(outputDir.getFilename(), RESOLVE_DIRECTORY);
      }
      final PCollectionView<ResourceId> outputDirView = input.getPipeline()
          .apply(Create.of(outputDir))
          .apply(View.<ResourceId>asSingleton());

      input.apply(
          ParDo.of(
              new DoFn<KV<String, byte[]>, Void>() {
                @ProcessElement
                public void processElement(ProcessContext c) {
                  KV<String, byte[]> kv = c.element();

                  ResourceId outputDir = c.sideInput(outputDirView);
                  String filename = kv.getKey();
                  ResourceId outputFile = outputDir.resolve(filename, RESOLVE_FILE);

                  byte[] bytes = kv.getValue();
                  try {
                    WritableByteChannel channel = FileSystems.create(outputFile, BINARY);
                    OutputStream os = Channels.newOutputStream(channel);
                    os.write(bytes);
                    os.flush();
                    os.close();
                  } catch (IOException e) {
                    e.printStackTrace();
                  }
                }
              }
          ).withSideInputs(outputDirView)
      );

      return PDone.in(input.getPipeline());
    }
  }

  /** Disable construction of utility class. */
  private WholeFileIO() {}
}
