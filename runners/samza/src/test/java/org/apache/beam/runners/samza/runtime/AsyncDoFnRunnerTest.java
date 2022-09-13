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
package org.apache.beam.runners.samza.runtime;

import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.runners.samza.TestSamzaRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Test;

public class AsyncDoFnRunnerTest {

  @Test
  public void test() {
    SamzaPipelineOptions options = PipelineOptionsFactory.as(SamzaPipelineOptions.class);
    options.setRunner(TestSamzaRunner.class);
    options.setMaxBundleSize(10);
    Pipeline p = Pipeline.create(options);

    p.apply(Create.of(1, 2, 3, 4, 5, 6, 7))
        .apply(
            MapElements.into(TypeDescriptors.voids())
                .via(
                    x -> {
                      System.out.println(x);
                      return null;
                    }));

    p.run().waitUntilFinish();
  }
}
