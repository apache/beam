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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.net.URI;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.runners.spark.translation.streaming.utils.TestPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringDelegateCoder;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.junit.Rule;
import org.junit.Test;

/**
 * Side effects test.
 */
public class SideEffectsTest implements Serializable {

  static class UserException extends RuntimeException {
  }

  @Rule
  public transient final TestPipelineOptions pipelineOptions = new TestPipelineOptions();

  @Test
  public void test() throws Exception {
    Pipeline p = Pipeline.create(pipelineOptions.getOptions());

    p.getCoderRegistry().registerCoder(URI.class, StringDelegateCoder.of(URI.class));

    p.apply(Create.of("a")).apply(ParDo.of(new OldDoFn<String, String>() {
      @Override
      public void processElement(ProcessContext c) throws Exception {
        throw new UserException();
      }
    }));

    try {
      p.run();
      fail("Run should thrown an exception");
    } catch (RuntimeException e) {
      assertTrue(e.getCause() instanceof UserException);
      assertNotNull(e.getCause());
    }
  }
}
