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
package org.apache.beam.examples;

import java.io.File;
import java.nio.charset.StandardCharsets;
import org.apache.beam.examples.DebuggingWordCount.WordCountOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.Files;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DebuggingWordCount}. */
@RunWith(JUnit4.class)
public class DebuggingWordCountTest {
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  private String getFilePath(String filePath) {
    if (filePath.contains(":")) {
      return filePath.replace("\\", "/").split(":", -1)[1];
    }
    return filePath;
  }

  @Test
  public void testDebuggingWordCount() throws Exception {
    File inputFile = tmpFolder.newFile();
    File outputFile = tmpFolder.newFile();
    Files.asCharSink(inputFile, StandardCharsets.UTF_8)
        .write("stomach secret Flourish message Flourish here Flourish");
    WordCountOptions options = TestPipeline.testingPipelineOptions().as(WordCountOptions.class);
    options.setInputFile(getFilePath(inputFile.getAbsolutePath()));
    options.setOutput(getFilePath(outputFile.getAbsolutePath()));
    DebuggingWordCount.runDebuggingWordCount(options);
  }
}
