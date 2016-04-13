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
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import org.apache.flink.test.util.JavaProgramTestBase;

import java.io.Serializable;

public class MaybeEmptyTestITCase extends JavaProgramTestBase implements Serializable {

  protected String resultPath;

  protected final String expected = "test";

  public MaybeEmptyTestITCase() {
  }

  @Override
  protected void preSubmit() throws Exception {
    resultPath = getTempDirPath("result");
  }

  @Override
  protected void postSubmit() throws Exception {
    compareResultsByLinesInMemory(expected, resultPath);
  }

  @Override
  protected void testProgram() throws Exception {

    Pipeline p = FlinkTestPipeline.createForBatch();

    p.apply(Create.of((Void) null)).setCoder(VoidCoder.of())
        .apply(ParDo.of(
            new DoFn<Void, String>() {
              @Override
              public void processElement(DoFn<Void, String>.ProcessContext c) {
                c.output(expected);
              }
            })).apply(TextIO.Write.to(resultPath));
    p.run();
  }

}
