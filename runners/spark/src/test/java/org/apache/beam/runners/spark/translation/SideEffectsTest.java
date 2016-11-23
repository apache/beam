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

import static org.hamcrest.core.Is.isA;

import java.io.Serializable;
import org.apache.beam.runners.spark.translation.streaming.utils.SparkTestPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Side effects test.
 */
public class SideEffectsTest implements Serializable {
  private static class UserException extends RuntimeException {
  }

  @Rule
  public final transient SparkTestPipelineOptions pipelineOptions = new SparkTestPipelineOptions();
  @Rule
  public final transient ExpectedException expectedException = ExpectedException.none();

  @Test
  public void test() throws Exception {
    Pipeline p = Pipeline.create(pipelineOptions.getOptions());

    p.apply(Create.of("a")).apply(ParDo.of(new DoFn<String, String>() {
      @ProcessElement
      public void processElement(ProcessContext c) throws Exception {
        throw new UserException();
      }
    }));

    expectedException.expectCause(isA(UserException.class));
    p.run();
  }
}
