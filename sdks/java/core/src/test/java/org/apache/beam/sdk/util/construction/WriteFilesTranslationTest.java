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
package org.apache.beam.sdk.util.construction;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.Objects;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.io.DynamicFileDestinations;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink.OutputFileHints;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.WriteFiles;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.transforms.resourcehints.ResourceHints;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PValues;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/** Tests for {@link WriteFilesTranslation}. */
@RunWith(Parameterized.class)
public class WriteFilesTranslationTest {
  @Parameters(name = "{index}: {0}")
  public static Iterable<WriteFiles<Object, Void, Object>> data() {
    return ImmutableList.of(
        WriteFiles.to(new DummySink()),
        WriteFiles.to(new DummySink()).withWindowedWrites(),
        WriteFiles.to(new DummySink()).withNumShards(17),
        WriteFiles.to(new DummySink()).withWindowedWrites().withNumShards(42),
        WriteFiles.to(new DummySink()).withAutoSharding());
  }

  @Parameter(0)
  public WriteFiles<String, Void, String> writeFiles;

  public static TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Test
  public void testEncodedProto() throws Exception {
    SdkComponents components = SdkComponents.create();
    components.registerEnvironment(Environments.createDockerEnvironment("java"));
    RunnerApi.WriteFilesPayload payload =
        WriteFilesTranslation.payloadForWriteFiles(writeFiles, components);

    assertThat(
        payload.getRunnerDeterminedSharding(),
        equalTo(
            writeFiles.getNumShardsProvider() == null && writeFiles.getComputeNumShards() == null));

    assertThat(payload.getWindowedWrites(), equalTo(writeFiles.getWindowedWrites()));

    assertThat(
        (FileBasedSink<String, Void, String>)
            WriteFilesTranslation.sinkFromProto(payload.getSink()),
        equalTo(writeFiles.getSink()));
  }

  @Test
  public void testExtractionDirectFromTransform() throws Exception {
    PCollection<String> input = p.apply(Create.of("hello"));
    WriteFilesResult<Void> output = input.apply(writeFiles);

    AppliedPTransform<PCollection<String>, WriteFilesResult<Void>, WriteFiles<String, Void, String>>
        appliedPTransform =
            AppliedPTransform.of(
                "foo",
                PValues.expandInput(input),
                PValues.expandOutput(output),
                writeFiles,
                ResourceHints.create(),
                p);

    assertThat(
        WriteFilesTranslation.isRunnerDeterminedSharding(appliedPTransform),
        equalTo(
            writeFiles.getNumShardsProvider() == null && writeFiles.getComputeNumShards() == null));

    assertThat(
        WriteFilesTranslation.isAutoSharded(appliedPTransform),
        equalTo(writeFiles.getWithAutoSharding()));
    assertThat(
        WriteFilesTranslation.isWindowedWrites(appliedPTransform),
        equalTo(writeFiles.getWindowedWrites()));
    assertThat(
        WriteFilesTranslation.<String, Void, String>getSink(appliedPTransform),
        equalTo(writeFiles.getSink()));
  }

  /**
   * A simple {@link FileBasedSink} for testing serialization/deserialization. Not mocked to avoid
   * any issues serializing mocks.
   */
  private static class DummySink extends FileBasedSink<Object, Void, Object> {

    DummySink() {
      super(
          StaticValueProvider.of(FileSystems.matchNewResource("nowhere", false)),
          DynamicFileDestinations.constant(
              new DummyFilenamePolicy(), SerializableFunctions.constant(null)));
    }

    @Override
    public WriteOperation<Void, Object> createWriteOperation() {
      return new DummyWriteOperation(this);
    }

    @Override
    public boolean equals(@Nullable Object other) {
      if (!(other instanceof DummySink)) {
        return false;
      }

      DummySink that = (DummySink) other;

      return getTempDirectoryProvider().isAccessible()
          && that.getTempDirectoryProvider().isAccessible()
          && getTempDirectoryProvider().get().equals(that.getTempDirectoryProvider().get());
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          DummySink.class,
          getTempDirectoryProvider().isAccessible() ? getTempDirectoryProvider().get() : null);
    }
  }

  private static class DummyWriteOperation extends FileBasedSink.WriteOperation<Void, Object> {
    public DummyWriteOperation(FileBasedSink<Object, Void, Object> sink) {
      super(sink);
    }

    @Override
    public FileBasedSink.Writer<Void, Object> createWriter() throws Exception {
      throw new UnsupportedOperationException("Should never be called.");
    }
  }

  private static class DummyFilenamePolicy extends FilenamePolicy {
    @Override
    public ResourceId windowedFilename(
        int shardNumber,
        int numShards,
        BoundedWindow window,
        PaneInfo paneInfo,
        OutputFileHints outputFileHints) {
      throw new UnsupportedOperationException("Should never be called.");
    }

    @Nullable
    @Override
    public ResourceId unwindowedFilename(
        int shardNumber, int numShards, OutputFileHints outputFileHints) {
      throw new UnsupportedOperationException("Should never be called.");
    }

    @Override
    public boolean equals(@Nullable Object other) {
      return other instanceof DummyFilenamePolicy;
    }

    @Override
    public int hashCode() {
      return DummyFilenamePolicy.class.hashCode();
    }
  }
}
