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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollectionView;

import org.apache.flink.test.util.JavaProgramTestBase;

import java.io.Serializable;

public class SideInputITCase extends JavaProgramTestBase implements Serializable {

  private static final String expected = "Hello!";

  protected String resultPath;

  @Override
  protected void testProgram() throws Exception {


    Pipeline p = FlinkTestPipeline.createForBatch();


    final PCollectionView<String> sidesInput = p
        .apply(Create.of(expected))
        .apply(View.<String>asSingleton());

    p.apply(Create.of("bli"))
        .apply(ParDo.of(new DoFn<String, String>() {
          @Override
          public void processElement(ProcessContext c) throws Exception {
            String s = c.sideInput(sidesInput);
            c.output(s);
          }
        }).withSideInputs(sidesInput)).apply(TextIO.Write.to(resultPath));

    p.run();
  }

  @Override
  protected void preSubmit() throws Exception {
    resultPath = getTempDirPath("result");
  }

  @Override
  protected void postSubmit() throws Exception {
    compareResultsByLinesInMemory(expected, resultPath);
  }
}
