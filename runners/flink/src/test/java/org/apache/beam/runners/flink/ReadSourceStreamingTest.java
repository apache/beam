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

import java.io.File;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Joiner;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.test.util.TestBaseUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Reads from a bounded source in streaming. */
public class ReadSourceStreamingTest extends AbstractTestBase {

  protected String resultDir;
  protected String resultPath;

  public ReadSourceStreamingTest() {}

  private static final String[] EXPECTED_RESULT =
      new String[] {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"};

  @Before
  public void preSubmit() throws Exception {
    // Beam Write will add shard suffix to fileName, see ShardNameTemplate.
    // So tempFile need have a parent to compare.
    File resultParent = createAndRegisterTempFile("result");
    resultDir = resultParent.toURI().toString();
    resultPath = new File(resultParent, "file.txt").getAbsolutePath();
  }

  @After
  public void postSubmit() throws Exception {
    TestBaseUtils.compareResultsByLinesInMemory(Joiner.on('\n').join(EXPECTED_RESULT), resultDir);
  }

  @Test
  public void testStreaming() {
    runProgram(resultPath, true);
  }

  @Test
  public void testBatch() {
    runProgram(resultPath, false);
  }

  private static void runProgram(String resultPath, boolean streaming) {

    Pipeline p =
        streaming ? FlinkTestPipeline.createForStreaming() : FlinkTestPipeline.createForBatch();

    p.apply(GenerateSequence.from(0).to(10))
        .apply(
            ParDo.of(
                new DoFn<Long, String>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) throws Exception {
                    c.output(c.element().toString());
                  }
                }))
        .apply(TextIO.write().to(resultPath));

    p.run();
  }
}
