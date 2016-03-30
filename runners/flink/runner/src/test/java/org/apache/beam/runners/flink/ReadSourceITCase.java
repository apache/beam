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

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.CountingInput;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.base.Joiner;
import org.apache.flink.test.util.JavaProgramTestBase;

public class ReadSourceITCase extends JavaProgramTestBase {

  protected String resultPath;

  public ReadSourceITCase(){
  }

  private static final String[] EXPECTED_RESULT = new String[] {
     "0", "1", "2", "3", "4", "5", "6", "7", "8", "9"};

  @Override
  protected void preSubmit() throws Exception {
    resultPath = getTempDirPath("result");
    System.out.println(resultPath);
  }

  @Override
  protected void postSubmit() throws Exception {
    compareResultsByLinesInMemory(Joiner.on('\n').join(EXPECTED_RESULT), resultPath);
  }

  @Override
  protected void testProgram() throws Exception {
    runProgram(resultPath);
  }

  private static void runProgram(String resultPath) {

    Pipeline p = FlinkTestPipeline.createForBatch();

    PCollection<String> result = p
        .apply(CountingInput.upTo(10))
        .apply(ParDo.of(new DoFn<Long, String>() {
          @Override
          public void processElement(ProcessContext c) throws Exception {
            c.output(c.element().toString());
          }
        }));

    result.apply(TextIO.Write.to(resultPath));

    p.run();
  }
}


