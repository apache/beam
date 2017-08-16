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

import javax.annotation.Nullable;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.StreamUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

// TODO: Write comment on what this is and how to use it.
// TODO: Mention that if multiple files have the same filename at write, only one will survive.
/**
 * WholeFileIO.
 */
public class WholeFileIO {

  public static Read read() {
    return new AutoValue_WholeFileIO_Read.Builder().build();
  }

  // TODO: Add a readAll() like TextIO.

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
                @ProcessElement
                public void processElement(ProcessContext c) throws IOException {
                  KV<String, byte[]> kv = c.element();

                  ResourceId outputDir = getOutputDir().get();
                  String filename = kv.getKey();
                  // TODO: Write to tmp files. Once tmp file write finished, rename to filename.
                  // TODO: ^ Alternative (faster): setup() create tmp dir, processElement()
                  //    write each file to tmp dir, teardown() rename tmp dir to outputDir
                  // (Or, instead of setup() and teardown(), use startBundle() and finishBundle()
                  //    except that you mv all files inside tmp dir to inside the outputDir
                  ResourceId outputFile = outputDir.resolve(filename, RESOLVE_FILE);

                  byte[] bytes = kv.getValue();
                  try (
                      OutputStream outStream =
                          Channels.newOutputStream(FileSystems.create(outputFile, BINARY))
                  ) {
                    outStream.write(bytes);
                    outStream.flush();
                  }
                }
              }
          )
      );

      return PDone.in(input.getPipeline());
    }
  }

  /** Disable construction of utility class. */
  private WholeFileIO() {}
}
