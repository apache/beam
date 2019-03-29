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

import static java.util.Arrays.asList;
import static org.apache.beam.sdk.testing.RegexMatcher.matches;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.core.Every.everyItem;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.runners.core.construction.PTransformMatchers;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Charsets;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.RemoteEnvironment;
import org.apache.flink.streaming.api.environment.RemoteStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.internal.util.reflection.Whitebox;

/** Tests for {@link FlinkPipelineExecutionEnvironment}. */
@RunWith(JUnit4.class)
public class FlinkPipelineExecutionEnvironmentTest implements Serializable {

  @Rule public transient TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void shouldRecognizeAndTranslateStreamingPipeline() {
    FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
    options.setRunner(TestFlinkRunner.class);
    options.setFlinkMaster("[auto]");

    FlinkPipelineExecutionEnvironment flinkEnv = new FlinkPipelineExecutionEnvironment(options);
    Pipeline pipeline = Pipeline.create();

    pipeline
        .apply(GenerateSequence.from(0).withRate(1, Duration.standardSeconds(1)))
        .apply(
            ParDo.of(
                new DoFn<Long, String>() {

                  @ProcessElement
                  public void processElement(ProcessContext c) throws Exception {
                    c.output(Long.toString(c.element()));
                  }
                }))
        .apply(Window.into(FixedWindows.of(Duration.standardHours(1))))
        .apply(TextIO.write().withNumShards(1).withWindowedWrites().to("/dummy/path"));

    flinkEnv.translate(pipeline);

    // no exception should be thrown
  }

  @Test
  public void shouldPrepareFilesToStageWhenFlinkMasterIsSetExplicitly() throws IOException {
    FlinkPipelineOptions options = testPreparingResourcesToStage("localhost:8081");

    assertThat(options.getFilesToStage().size(), is(1));
    assertThat(options.getFilesToStage().get(0), matches(".*\\.jar"));
  }

  @Test
  public void shouldNotPrepareFilesToStageWhenFlinkMasterIsSetToAuto() throws IOException {
    FlinkPipelineOptions options = testPreparingResourcesToStage("[auto]");

    assertThat(options.getFilesToStage().size(), is(2));
    assertThat(options.getFilesToStage(), everyItem(not(matches(".*\\.jar"))));
  }

  @Test
  public void shouldNotPrepareFilesToStagewhenFlinkMasterIsSetToCollection() throws IOException {
    FlinkPipelineOptions options = testPreparingResourcesToStage("[collection]");

    assertThat(options.getFilesToStage().size(), is(2));
    assertThat(options.getFilesToStage(), everyItem(not(matches(".*\\.jar"))));
  }

  @Test
  public void shouldNotPrepareFilesToStageWhenFlinkMasterIsSetToLocal() throws IOException {
    FlinkPipelineOptions options = testPreparingResourcesToStage("[local]");

    assertThat(options.getFilesToStage().size(), is(2));
    assertThat(options.getFilesToStage(), everyItem(not(matches(".*\\.jar"))));
  }

  @Test
  public void shouldUseDefaultTempLocationIfNoneSet() {
    FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
    options.setRunner(TestFlinkRunner.class);
    options.setFlinkMaster("clusterAddress");

    FlinkPipelineExecutionEnvironment flinkEnv = new FlinkPipelineExecutionEnvironment(options);

    Pipeline pipeline = Pipeline.create(options);
    flinkEnv.translate(pipeline);

    String defaultTmpDir = System.getProperty("java.io.tmpdir");

    assertThat(options.getFilesToStage(), hasItem(startsWith(defaultTmpDir)));
  }

  @Test
  public void shouldUsePreparedFilesOnRemoteEnvironment() throws Exception {
    FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
    options.setRunner(TestFlinkRunner.class);
    options.setFlinkMaster("clusterAddress");

    FlinkPipelineExecutionEnvironment flinkEnv = new FlinkPipelineExecutionEnvironment(options);

    Pipeline pipeline = Pipeline.create(options);
    flinkEnv.translate(pipeline);

    ExecutionEnvironment executionEnvironment = flinkEnv.getBatchExecutionEnvironment();
    assertThat(executionEnvironment, instanceOf(RemoteEnvironment.class));

    @SuppressWarnings("unchecked")
    List<URL> jarFiles = (List<URL>) Whitebox.getInternalState(executionEnvironment, "jarFiles");

    List<URL> urlConvertedStagedFiles = convertFilesToURLs(options.getFilesToStage());

    assertThat(jarFiles, is(urlConvertedStagedFiles));
  }

  @Test
  public void shouldUsePreparedFilesOnRemoteStreamEnvironment() throws Exception {
    FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
    options.setRunner(TestFlinkRunner.class);
    options.setFlinkMaster("clusterAddress");
    options.setStreaming(true);

    FlinkPipelineExecutionEnvironment flinkEnv = new FlinkPipelineExecutionEnvironment(options);

    Pipeline pipeline = Pipeline.create(options);
    flinkEnv.translate(pipeline);

    StreamExecutionEnvironment streamExecutionEnvironment =
        flinkEnv.getStreamExecutionEnvironment();
    assertThat(streamExecutionEnvironment, instanceOf(RemoteStreamEnvironment.class));

    @SuppressWarnings("unchecked")
    List<URL> jarFiles =
        (List<URL>) Whitebox.getInternalState(streamExecutionEnvironment, "jarFiles");

    List<URL> urlConvertedStagedFiles = convertFilesToURLs(options.getFilesToStage());

    assertThat(jarFiles, is(urlConvertedStagedFiles));
  }

  @Test
  public void shouldUseTransformOverrides() {
    boolean[] testParameters = {true, false};
    for (boolean streaming : testParameters) {
      FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
      options.setStreaming(streaming);
      options.setRunner(FlinkRunner.class);
      FlinkPipelineExecutionEnvironment flinkEnv = new FlinkPipelineExecutionEnvironment(options);
      Pipeline p = Mockito.spy(Pipeline.create(options));

      flinkEnv.translate(p);

      ArgumentCaptor<ImmutableList> captor = ArgumentCaptor.forClass(ImmutableList.class);
      Mockito.verify(p).replaceAll(captor.capture());
      ImmutableList<PTransformOverride> overridesList = captor.getValue();

      assertThat(overridesList.isEmpty(), is(false));
      assertThat(
          overridesList.size(), is(FlinkTransformOverrides.getDefaultOverrides(options).size()));
    }
  }

  @Test
  public void shouldUseStreamingTransformOverridesWithUnboundedSources() {
    FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
    // no explicit streaming mode set
    options.setRunner(FlinkRunner.class);
    FlinkPipelineExecutionEnvironment flinkEnv = new FlinkPipelineExecutionEnvironment(options);
    Pipeline p = Mockito.spy(Pipeline.create(options));

    // Add unbounded source which will set the streaming mode to true
    p.apply(GenerateSequence.from(0));

    flinkEnv.translate(p);

    ArgumentCaptor<ImmutableList> captor = ArgumentCaptor.forClass(ImmutableList.class);
    Mockito.verify(p).replaceAll(captor.capture());
    ImmutableList<PTransformOverride> overridesList = captor.getValue();

    assertThat(
        overridesList,
        hasItem(
            PTransformOverride.of(
                PTransformMatchers.urnEqualTo(PTransformTranslation.CREATE_VIEW_TRANSFORM_URN),
                CreateStreamingFlinkView.Factory.INSTANCE)));
  }

  @Test
  public void testTranslationModeOverrideWithUnboundedSources() {
    FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
    options.setRunner(FlinkRunner.class);
    options.setStreaming(false);

    FlinkPipelineExecutionEnvironment flinkEnv = new FlinkPipelineExecutionEnvironment(options);
    Pipeline pipeline = Pipeline.create(options);
    pipeline.apply(GenerateSequence.from(0));
    flinkEnv.translate(pipeline);

    assertThat(options.isStreaming(), Matchers.is(true));
  }

  @Test
  public void testTranslationModeNoOverrideWithoutUnboundedSources() {
    boolean[] testArgs = new boolean[] {true, false};
    for (boolean streaming : testArgs) {
      FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
      options.setRunner(FlinkRunner.class);
      options.setStreaming(streaming);

      FlinkPipelineExecutionEnvironment flinkEnv = new FlinkPipelineExecutionEnvironment(options);
      Pipeline pipeline = Pipeline.create(options);
      pipeline.apply(GenerateSequence.from(0).to(10));
      flinkEnv.translate(pipeline);

      assertThat(options.isStreaming(), Matchers.is(streaming));
    }
  }

  @Test
  public void shouldLogWarningWhenCheckpointingIsDisabled() {
    Pipeline pipeline = Pipeline.create();
    pipeline.getOptions().setRunner(TestFlinkRunner.class);

    pipeline
        // Add an UnboundedSource to check for the warning if checkpointing is disabled
        .apply(GenerateSequence.from(0))
        .apply(
            ParDo.of(
                new DoFn<Long, Void>() {
                  @ProcessElement
                  public void processElement(ProcessContext ctx) {
                    throw new RuntimeException("Failing here is ok.");
                  }
                }));

    final PrintStream oldErr = System.err;
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    PrintStream replacementStdErr = new PrintStream(byteArrayOutputStream);
    try {
      System.setErr(replacementStdErr);
      // Run pipeline and fail during execution
      pipeline.run();
      fail("Should have failed");
    } catch (Exception e) {
      // We want to fail here
    } finally {
      System.setErr(oldErr);
    }
    replacementStdErr.flush();
    assertThat(
        new String(byteArrayOutputStream.toByteArray(), Charsets.UTF_8),
        containsString(
            "UnboundedSources present which rely on checkpointing, but checkpointing is disabled."));
  }

  private FlinkPipelineOptions testPreparingResourcesToStage(String flinkMaster)
      throws IOException {
    Pipeline pipeline = Pipeline.create();
    String tempLocation = tmpFolder.newFolder().getAbsolutePath();

    File notEmptyDir = tmpFolder.newFolder();
    notEmptyDir.createNewFile();
    String notEmptyDirPath = notEmptyDir.getAbsolutePath();
    String notExistingPath = "/path/to/not/existing/dir";

    FlinkPipelineOptions options =
        setPipelineOptions(flinkMaster, tempLocation, asList(notEmptyDirPath, notExistingPath));
    FlinkPipelineExecutionEnvironment flinkEnv = new FlinkPipelineExecutionEnvironment(options);
    flinkEnv.translate(pipeline);
    return options;
  }

  private FlinkPipelineOptions setPipelineOptions(
      String flinkMaster, String tempLocation, List<String> filesToStage) {
    FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
    options.setRunner(TestFlinkRunner.class);
    options.setFlinkMaster(flinkMaster);
    options.setTempLocation(tempLocation);
    options.setFilesToStage(filesToStage);
    return options;
  }

  private static List<URL> convertFilesToURLs(List<String> filePaths) {
    return filePaths.stream()
        .map(
            file -> {
              try {
                return new File(file).getAbsoluteFile().toURI().toURL();
              } catch (MalformedURLException e) {
                throw new RuntimeException("Failed to convert to URL", e);
              }
            })
        .collect(Collectors.toList());
  }
}
