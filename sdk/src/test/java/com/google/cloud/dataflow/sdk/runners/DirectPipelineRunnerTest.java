/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.runners;

import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.google.cloud.dataflow.sdk.coders.AtomicCoder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.options.DirectPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

/** Tests for {@link DirectPipelineRunner}. */
@RunWith(JUnit4.class)
public class DirectPipelineRunnerTest implements Serializable {

  private static final long serialVersionUID = 0L;

  @Rule
  public transient ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testToString() {
    PipelineOptions options = PipelineOptionsFactory.create();
    DirectPipelineRunner runner = DirectPipelineRunner.fromOptions(options);
    assertEquals("DirectPipelineRunner#" + runner.hashCode(),
        runner.toString());
  }

  private static class CrashingCoder<T> extends AtomicCoder<T> {
    private static final long serialVersionUID = 0L;

    @Override
    public void encode(T value, OutputStream stream, Context context) throws CoderException {
      throw new CoderException("Called CrashingCoder.encode");
    }

    @Override
    public T decode(
        InputStream inStream, com.google.cloud.dataflow.sdk.coders.Coder.Context context)
            throws CoderException {
      throw new CoderException("Called CrashingCoder.decode");
    }
  }

  @Test
  public void testCoderException() {
    DirectPipeline pipeline = DirectPipeline.createForTest();

    pipeline
        .apply("CreateTestData", Create.of(42))
        .apply("CrashDuringCoding", ParDo.of(new DoFn<Integer, String>() {
          private static final long serialVersionUID = 0L;

          @Override
          public void processElement(ProcessContext context) {
            context.output("hello");
          }
        }))
        .setCoder(new CrashingCoder<String>());

      expectedException.expect(RuntimeException.class);
      expectedException.expectCause(isA(CoderException.class));
      pipeline.run();
  }

  @Test
  public void testDirectPipelineOptions() {
    DirectPipelineOptions options = PipelineOptionsFactory.create().as(DirectPipelineOptions.class);
    assertNull(options.getDirectPipelineRunnerRandomSeed());
  }
}
