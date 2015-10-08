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
import com.google.cloud.dataflow.sdk.coders.StringDelegateCoder;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import java.io.Serializable;
import java.net.URI;
import org.junit.Test;

import static org.junit.Assert.fail;

public class SideEffectsTest implements Serializable {
  @Test
  public void test() throws Exception {
    SparkPipelineOptions options = SparkPipelineOptionsFactory.create();
    options.setRunner(SparkPipelineRunner.class);
    Pipeline pipeline = Pipeline.create(options);

    pipeline.getCoderRegistry().registerCoder(URI.class, StringDelegateCoder.of(URI.class));

    PCollection<String> strings = pipeline.apply(Create.of("a"));
    strings.apply(ParDo.of(new DoFn<String, String>() {
      @Override
      public void processElement(ProcessContext c) throws Exception {
        throw new IllegalStateException("Side effect");
      }
    }));

    try {
      pipeline.run();
      fail("Run should thrown an exception");
    } catch (Exception e) {
      // expected
    }
  }
}