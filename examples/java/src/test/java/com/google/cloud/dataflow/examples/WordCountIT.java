/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.examples;

import com.google.cloud.dataflow.examples.WordCount.CountWords;
import com.google.cloud.dataflow.examples.WordCount.FormatAsTextFn;
import com.google.cloud.dataflow.examples.WordCount.WordCountOptions;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.testing.RunnableOnService;
import com.google.cloud.dataflow.sdk.transforms.MapElements;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * End-to-end tests of WordCount.
 */
@RunWith(JUnit4.class)
public class WordCountIT extends BatchE2ETest {

  @Test
  @Category(RunnableOnService.class)
  public void testE2EWordCount() throws Exception {
    String[] args = {
        "--jobName=wordcount-" + generateTestIdentifier() + "-prod",
        "--project=apache-beam-testing",
        "--runner=DataflowPipelineRunner",
        "--stagingLocation=gs://apache-beam-testing-storage/staging",
        "--workerLogLevelOverrides="
        + "{\"com.google.cloud.dataflow.sdk.util.UploadIdResponseInterceptor\":\"DEBUG\"}"};

    WordCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
        .as(WordCountOptions.class);
    Pipeline p = Pipeline.create(options);

    p.apply(TextIO.Read.named("ReadLines").from(options.getInputFile()))
     .apply(new CountWords())
     .apply(MapElements.via(new FormatAsTextFn()))
     .apply(TextIO.Write.named("WriteCounts").to(options.getOutput()));

    p.run();
  }
}
