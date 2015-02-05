/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.dataflow.spark;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner.EvaluationResults;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.base.Charsets;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static com.google.api.client.repackaged.com.google.common.base.Joiner.on;
import static java.io.File.separator;

/**
 * A test for the transforms registered in TransformTranslator.
 * Builds a regular Dataflow pipeline with each of the mapped
 * transforms, and makes sure that they work when the pipeline is
 * executed in Spark.
 */
public class TransformTranslatorTest {

  @Rule public TestName name = new TestName();

  private Pipeline testPipeline;
  private DirectPipelineRunner directRunner;
  private SparkPipelineRunner sparkRunner;
  private EvaluationResults directRunResult;
  private EvaluationResult sparkRunResult;
  private String testDataDirName;

  @Before public void init() throws IOException {
    testPipeline = Pipeline.create(PipelineOptionsFactory.create());
    sparkRunner = SparkPipelineRunner.create();
    directRunner = DirectPipelineRunner.createForTest();
    testDataDirName = on(separator).join("target", "test-data", name.getMethodName()) + separator;
    FileUtils.deleteDirectory(new File(testDataDirName));
    new File(testDataDirName).mkdirs();
  }

  public void run() {
    directRunResult = directRunner.run(testPipeline);
    sparkRunResult = sparkRunner.run(testPipeline);
  }

  /**
   * Builds a simple pipeline with TextIO.Read and TextIO.Write, runs the pipeline
   * in DirectPipelineRunner and on SparkPipelineRunner, with the mapped dataflow-to-spark
   * transforms. Finally it makes sure that the results are the same for both runs.
   */
  @Test public void testTextIOReadAndWriteTransforms() throws IOException {
    String outFile = on(separator).join(testDataDirName, "test_text_out");
    PCollection<String> lines =  testPipeline
        .apply(TextIO.Read.from("src/test/resources/test_text.txt"));
    lines.apply(TextIO.Write.to(outFile));
    run();

    List<String> directOutput = Files.readAllLines(Paths.get(outFile + "-00000-of-00001"),
        Charsets.UTF_8);

    List<String> sparkOutput = Files.readAllLines(Paths.get(
            on(separator).join(outFile, "part-00000")),
        Charsets.UTF_8);

    Assert.assertArrayEquals(directOutput.toArray(), sparkOutput.toArray());
  }
}
