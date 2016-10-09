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

import java.io.Serializable;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;

/**
 * DoFN output test.
 */
public class DoFnOutputTest implements Serializable {
  @Test
  public void test() throws Exception {
    SparkPipelineOptions options = PipelineOptionsFactory.as(SparkPipelineOptions.class);
    options.setRunner(SparkRunner.class);
    Pipeline p = Pipeline.create(options);

    PCollection<String> strings = p.apply(Create.of("a"));
    // Test that values written from startBundle() and finishBundle() are written to
    // the output
    PCollection<String> output = strings.apply(ParDo.of(new OldDoFn<String, String>() {
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

    PAssert.that(output).containsInAnyOrder("start", "a", "finish");

    p.run();
  }
}
