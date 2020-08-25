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
package org.apache.beam.runners.flink;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.util.Collections;
import org.apache.beam.runners.flink.FlinkStreamingPipelineTranslator.StreamingShardedWriteFactory;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.DynamicFileDestinations;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.WriteFiles;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunctions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Tests if overrides are properly applied. */
public class FlinkTransformOverridesTest {

  @Rule public transient TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testRunnerDeterminedSharding() {
    FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
    options.setRunner(TestFlinkRunner.class);
    options.setFlinkMaster("[auto]");
    options.setParallelism(5);

    TestPipeline p = TestPipeline.fromOptions(options);

    StreamingShardedWriteFactory<Object, Void, Object> factory =
        new StreamingShardedWriteFactory<>(p.getOptions());

    WriteFiles<Object, Void, Object> original = WriteFiles.to(new TestSink(tmpFolder.toString()));
    @SuppressWarnings("unchecked")
    PCollection<Object> objs = (PCollection) p.apply(Create.empty(VoidCoder.of()));
    AppliedPTransform<PCollection<Object>, WriteFilesResult<Void>, WriteFiles<Object, Void, Object>>
        originalApplication =
            AppliedPTransform.of("writefiles", objs.expand(), Collections.emptyMap(), original, p);

    WriteFiles<Object, Void, Object> replacement =
        (WriteFiles<Object, Void, Object>)
            factory.getReplacementTransform(originalApplication).getTransform();

    assertThat(replacement, not(equalTo((Object) original)));
    assertThat(replacement.getNumShardsProvider().get(), is(10));
  }

  private static class TestSink extends FileBasedSink<Object, Void, Object> {
    @Override
    public void validate(PipelineOptions options) {}

    private static final FilenamePolicy FILENAME_POLICY =
        new FilenamePolicy() {
          @Override
          public ResourceId windowedFilename(
              int shardNumber,
              int numShards,
              BoundedWindow window,
              PaneInfo paneInfo,
              OutputFileHints outputFileHints) {
            throw new UnsupportedOperationException("should not be called");
          }

          @Nullable
          @Override
          public ResourceId unwindowedFilename(
              int shardNumber, int numShards, OutputFileHints outputFileHints) {
            throw new UnsupportedOperationException("should not be called");
          }
        };

    TestSink(String tmpFolder) {
      super(
          ValueProvider.StaticValueProvider.of(FileSystems.matchNewResource(tmpFolder, true)),
          DynamicFileDestinations.constant(FILENAME_POLICY, SerializableFunctions.identity()));
    }

    @Override
    public WriteOperation<Void, Object> createWriteOperation() {
      throw new IllegalArgumentException("Should not be used");
    }
  }
}
