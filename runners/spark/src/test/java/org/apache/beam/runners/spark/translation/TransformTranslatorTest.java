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

package org.apache.beam.runners.spark.translation;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.values.PCollection;

import com.google.common.base.Charsets;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * A test for the transforms registered in TransformTranslator.
 * Builds a regular Beam pipeline with each of the mapped
 * transforms, and makes sure that they work when the pipeline is
 * executed in Spark.
 */
public class TransformTranslatorTest {
  private static final Logger LOG = LoggerFactory.getLogger(TransformTranslatorTest.class);
  @Rule public TemporaryFolder tmp = new TemporaryFolder();

  /**
   * Builds a simple pipeline with TextIO.Read and TextIO.Write, runs the pipeline
   * in DirectRunner and on SparkRunner, with the mapped dataflow-to-spark
   * transforms. Finally it makes sure that the results are the same for both runs.
   */
  @Test
  public void testTextIOReadAndWriteTransforms() throws IOException {
    String directOut = runPipeline(DirectRunner.class);
    String sparkOut = runPipeline(SparkRunner.class);

    File directOutFile = new File(directOut);
    List<String> directOutput =
            readFromOutputFiles(directOutFile.getParentFile(), directOutFile.getName());

    File sparkOutFile = new File(sparkOut);
    List<String> sparkOutput =
            readFromOutputFiles(sparkOutFile.getParentFile(), sparkOutFile.getName());

    // sort output to get a stable result (PCollections are not ordered)
    assertThat(sparkOutput, containsInAnyOrder(directOutput.toArray()));
  }

  private String runPipeline(Class<? extends PipelineRunner<?>> runner) throws IOException {
    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(runner);
    Pipeline p = Pipeline.create(options);
    File outFile = tmp.newFile();
    PCollection<String> lines =  p.apply(TextIO.Read.from("src/test/resources/test_text.txt"));
    lines.apply(TextIO.Write.to(outFile.getAbsolutePath()));
    p.run();
    return outFile.getAbsolutePath();
  }

  private List<String> readFromOutputFiles(File parent, String outPattern) throws IOException {
    // example pattern: outprefix-00000-of-00001
    Pattern pattern = Pattern.compile(String.format("%s-[0-9]{5}-of-[0-9]{5}", outPattern));
    List<String> lines = new ArrayList<>();
    if (parent.exists() && parent.isDirectory()) {
      //noinspection ConstantConditions
      for (File f : parent.listFiles()) {
        if (pattern.matcher(f.getName()).matches()) {
          LOG.info("For " + outPattern + " reading file " + f.getName());
          lines.addAll(FileUtils.readLines(f, Charsets.UTF_8));
        }
      }
    }
    return lines;
  }
}
