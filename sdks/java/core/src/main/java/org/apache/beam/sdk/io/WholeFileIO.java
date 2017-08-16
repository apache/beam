/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions.RESOLVE_FILE;
import static org.apache.beam.sdk.util.MimeTypes.BINARY;

import com.google.auto.value.AutoValue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MoveOptions;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.util.StreamUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link PTransform}s for reading and writing files as {@link KV} pairs of filename {@link String}s
 * and byte arrays.
 *
 * <p>To read a {@link PCollection} of one or more files as {@link KV}s, use
 * {@code WholeFileIO.read()} to instantiate a transform and use
 * {@link WholeFileIO.Read#from(String)} to specify the path of the file(s) to be read.</p>
 *
 * <p>Method {@link #read} returns a {@link PCollection} of {@code  KV<String, byte[]>}s,
 * each corresponding to one file's filename and contents.</p>
 *
 * <p>The filepatterns are expanded only once.
 *
 * <p>Example 1: reading a file or filepattern (or file glob).
 *
 * <pre>{@code
 * Pipeline p = ...;
 *
 * // A Read of a local file (only runs locally):
 * PCollection<KV<String, byte[]>> oneFile = p.apply(
 *                                              WholeFileIO.read().from("/local/path/to/file.txt"));
 *
 * // A Read of local files in a directory (only runs locally):
 * PCollection<KV<String, byte[]>> manyFiles = p.apply(
 *                                               WholeFileIO.read().from("/local/path/to/files/*"));
 *
 * // A Read of local files in nested directories (only runs locally):
 * PCollection<KV<String, byte[]>> manyFiles = p.apply(
 *                                        WholeFileIO.read().from("/local/path/to/nested/dirs/**"));
 * // ^ The KV's String corresponding to filename retains only the last term of the file path
 * //   (i.e. it retains the filename and ignores intermediate directory names)
 * }</pre>
 *
 * <p>To write the byte array of a {@link PCollection} of {@code KV<String, byte[]>} to an output
 * directory with the KV's String as filename, use {@code WholeFileIO.write()} with
 * {@link WholeFileIO.Write#to(String)} to specify the output directory of the files to write.
 *
 * <p>For example:
 *
 * <pre>{@code
 * // A simple Write to a local file (only runs locally):
 * PCollection<KV<String, byte[]>> files = ...;
 * lines.apply(WholeFileIO.write().to("/path/to/output/dir/"));
 * }</pre>
 *
 * <p>Any existing files with the same names as generated output files will be overwritten.
 * Similarly, if multiple KV's in the incoming {@link PCollection} have the same String (i.e.
 * filename), then duplicates will be overwritten by the other such named elements of the
 * {@link PCollection}. In other words, only one {@link KV} of a certain filename will write out
 * successfully.
 */
public class WholeFileIO {

  /**
   * A {@link PTransform} that reads from one or more files and returns a bounded
   * {@link PCollection} containing one {@link KV} element for each input file.
   */
  public static Read read() {
    return new AutoValue_WholeFileIO_Read.Builder().build();
  }

  // TODO: Add a readAll() like TextIO.

  /**
   * A {@link PTransform} that takes a {@link PCollection} of {@link KV {@code KV<String, byte[]>}}
   * and writes each {@code byte[]} to the corresponding filename (i.e. the {@link String} of the
   * {@link KV}).
   */
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

      PCollection<String> filePatternPCollection = input.apply(
                                        Create.ofProvider(getFilePattern(), StringUtf8Coder.of()));

      PCollection<MatchResult.Metadata> matchResultMetaData = filePatternPCollection.apply(
                                                                              Match.filepatterns());

      PCollection<KV<String, byte[]>> files = matchResultMetaData.apply(
          ParDo.of(
              new DoFn<MatchResult.Metadata, KV<String, byte[]>>() {
                @ProcessElement
                public void processElement(ProcessContext c) throws IOException {
                  MatchResult.Metadata metadata = c.element();
                  ResourceId resourceId = metadata.resourceId();

                  try (
                      InputStream inStream = Channels.newInputStream(FileSystems.open(resourceId))
                  ) {
                    c.output(KV.of(resourceId.getFilename(), StreamUtils.getBytes(inStream)));
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
    private static final Logger LOG = LoggerFactory.getLogger(Write.class);

    @Nullable abstract ValueProvider<ResourceId> getOutputDir();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setOutputDir(ValueProvider<ResourceId> outputDir);

      abstract Write build();
    }

    public Write to(String outputDir) {
      return to(FileSystems.matchNewResource(outputDir, true));
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

      input.apply(
          ParDo.of(
              new DoFn<KV<String, byte[]>, Void>() {
                ValueProvider<ResourceId> tmpDir;

                @Setup
                public void setup() {
                  tmpDir = ValueProvider.NestedValueProvider.of(
                      getOutputDir(),
                      new TemporaryDirectoryBuilder()
                  );
                }

                @ProcessElement
                public void processElement(ProcessContext c) throws IOException {
                  KV<String, byte[]> kv = c.element();

                  String filename = kv.getKey();
                  ResourceId tmpFile = tmpDir.get().resolve(filename, RESOLVE_FILE);

                  byte[] bytes = kv.getValue();
                  try (
                      OutputStream outStream =
                          Channels.newOutputStream(FileSystems.create(tmpFile, BINARY))
                  ) {
                    outStream.write(bytes);
                    outStream.flush();
                  } catch (IOException e) {
                    LOG.error(
                        "Failed to write to temporary file [{}] for [{}].",
                        tmpFile,
                        getOutputDir().get().resolve(filename, RESOLVE_FILE)
                    );
                    FileSystems.delete(
                        Collections.singletonList(tmpFile),
                        MoveOptions.StandardMoveOptions.IGNORE_MISSING_FILES
                    );
                    throw e;
                  }
                }

                @Teardown
                public void teardown() throws IOException {
                  try {
                    FileSystems.rename(
                        Collections.singletonList(tmpDir.get()),
                        Collections.singletonList(getOutputDir().get())
                    );
                  } catch (IOException e) {
                    LOG.error(
                        "Failed to rename temporary directory [{}] to [{}].",
                        tmpDir.get(), getOutputDir().get()
                    );
                    throw e;
                  }
                }
              }
          )
      );

      return PDone.in(input.getPipeline());
    }

    private static class TemporaryDirectoryBuilder
        implements SerializableFunction<ResourceId, ResourceId> {
      private static final AtomicLong TEMP_COUNT = new AtomicLong(0);
      private static final DateTimeFormatter TEMPDIR_TIMESTAMP =
          DateTimeFormat.forPattern("yyyy-MM-DD_HH-mm-ss");
      // The intent of the code is to have a consistent value of tempDirectory across
      // all workers, which wouldn't happen if now() was called inline.
      private final String timestamp = Instant.now().toString(TEMPDIR_TIMESTAMP);
      // Multiple different sinks may be used in the same output directory; use tempId to create a
      // separate temp directory for each.
      private final Long tempId = TEMP_COUNT.getAndIncrement();

      @Override
      public ResourceId apply(ResourceId tempDirectory) {
        // Temp directory has a timestamp and a unique ID
        String tempDirName = String.format(".temp-beam-%s-%s", timestamp, tempId);
        return tempDirectory
            .getCurrentDirectory()
            .resolve(tempDirName, ResolveOptions.StandardResolveOptions.RESOLVE_DIRECTORY);
      }
    }
  }

  /** Disable construction of utility class. */
  private WholeFileIO() {}
}
