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
package org.apache.beam.it.gcp;

import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatPipeline;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatResult;
import static org.apache.beam.sdk.util.construction.resources.PipelineResources.detectClassPathResourcesToStage;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.it.common.PipelineLauncher.LaunchConfig;
import org.apache.beam.it.common.PipelineLauncher.LaunchInfo;
import org.apache.beam.it.common.PipelineLauncher.Sdk;
import org.apache.beam.it.common.PipelineOperator.Result;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class WordCountIT extends IOLoadTestBase {

  @Rule public TestPipeline wcPipeline = TestPipeline.create();

  @Before
  public void setup() {
    buildPipeline();
  }

  @Test
  public void testWordCountDataflow() throws IOException {
    LaunchConfig options =
        LaunchConfig.builder("test-wordcount")
            .setSdk(Sdk.JAVA)
            .setPipeline(wcPipeline)
            .addParameter("runner", "DataflowRunner")
            .build();

    LaunchInfo launchInfo = pipelineLauncher.launch(project, region, options);
    assertThatPipeline(launchInfo).isRunning();
    Result result =
        pipelineOperator.waitUntilDone(createConfig(launchInfo, Duration.ofMinutes(20)));
    assertThatResult(result).isLaunchFinished();
  }

  @Test
  public void testWordCountDataflowWithGCSFilesToStage() throws IOException {

    PipelineOptions pipelineOptions = wcPipeline.getOptions();
    List<String> filesToStage =
        detectClassPathResourcesToStage(DataflowRunner.class.getClassLoader(), pipelineOptions);
    filesToStage.add("gs://apache-beam-samples/shakespeare/kinglear.txt");

    LaunchConfig options =
        LaunchConfig.builder("test-wordcount")
            .setSdk(Sdk.JAVA)
            .setPipeline(wcPipeline)
            .addParameter("runner", "DataflowRunner")
            .addParameter("filesToStage", String.join(",", filesToStage))
            .build();

    LaunchInfo launchInfo = pipelineLauncher.launch(project, region, options);
    assertThatPipeline(launchInfo).isRunning();
    Result result =
        pipelineOperator.waitUntilDone(createConfig(launchInfo, Duration.ofMinutes(20)));
    assertThatResult(result).isLaunchFinished();
  }

  /** Build WordCount pipeline. */
  private void buildPipeline() {
    wcPipeline
        .apply(TextIO.read().from("gs://apache-beam-samples/shakespeare/kinglear.txt"))
        .apply(
            FlatMapElements.into(TypeDescriptors.strings())
                .via((String line) -> Arrays.asList(line.split("[^\\p{L}]+"))))
        .apply(Filter.by((String word) -> !word.isEmpty()))
        .apply(Count.perElement())
        .apply(
            MapElements.into(TypeDescriptors.strings())
                .via(
                    (KV<String, Long> wordCount) ->
                        wordCount.getKey() + ": " + wordCount.getValue()))
        .apply(TextIO.write().to("wordcounts"));
  }
}
