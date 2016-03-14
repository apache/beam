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

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import org.apache.beam.runners.spark.EvaluationResult;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.SparkPipelineRunner;
import org.junit.Test;

import java.io.Serializable;

public class DoFnOutputTest implements Serializable {
  @Test
  public void test() throws Exception {
    SparkPipelineOptions options = SparkPipelineOptionsFactory.create();
    options.setRunner(SparkPipelineRunner.class);
    Pipeline pipeline = Pipeline.create(options);

    PCollection<String> strings = pipeline.apply(Create.of("a"));
    // Test that values written from startBundle() and finishBundle() are written to
    // the output
    PCollection<String> output = strings.apply(ParDo.of(new DoFn<String, String>() {
      @Override
      public void startBundle(Context c) throws Exception {
        c.output("start");
      }
      @Override
      public void processElement(ProcessContext c) throws Exception {
        c.output(c.element());
      }
      @Override
      public void finishBundle(Context c) throws Exception {
        c.output("finish");
      }
    }));

    DataflowAssert.that(output).containsInAnyOrder("start", "a", "finish");

    EvaluationResult res = SparkPipelineRunner.create().run(pipeline);
    res.close();
  }
}
