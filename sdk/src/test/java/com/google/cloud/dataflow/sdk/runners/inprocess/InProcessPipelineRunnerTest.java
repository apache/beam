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
package com.google.cloud.dataflow.sdk.runners.inprocess;

import static org.hamcrest.Matchers.is;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.InProcessPipelineResult;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SimpleFunction;
import com.google.cloud.dataflow.sdk.transforms.display.DisplayData;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import com.fasterxml.jackson.annotation.JsonValue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;

/**
 * Tests for basic {@link InProcessPipelineRunner} functionality.
 */
@RunWith(JUnit4.class)
public class InProcessPipelineRunnerTest implements Serializable {
  @Rule public transient ExpectedException thrown = ExpectedException.none();

  @Test
  public void wordCountShouldSucceed() throws Throwable {
    Pipeline p = getPipeline();

    PCollection<KV<String, Long>> counts =
        p.apply(Create.of("foo", "bar", "foo", "baz", "bar", "foo"))
            .apply(MapElements.via(new SimpleFunction<String, String>() {
              @Override
              public String apply(String input) {
                return input;
              }
            }))
            .apply(Count.<String>perElement());
    PCollection<String> countStrs =
        counts.apply(MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
          @Override
          public String apply(KV<String, Long> input) {
            String str = String.format("%s: %s", input.getKey(), input.getValue());
            return str;
          }
        }));

    DataflowAssert.that(countStrs).containsInAnyOrder("baz: 1", "bar: 2", "foo: 3");

    InProcessPipelineResult result = ((InProcessPipelineResult) p.run());
    result.awaitCompletion();
  }

  @Test
  public void transformDisplayDataExceptionShouldFail() {
    DoFn<Integer, Integer> brokenDoFn = new DoFn<Integer, Integer>() {
      @Override
      public void processElement(ProcessContext c) throws Exception {}

      @Override
      public void populateDisplayData(DisplayData.Builder builder) {
        throw new RuntimeException("oh noes!");
      }
    };

    Pipeline p = getPipeline();
    p
        .apply(Create.of(1, 2, 3))
        .apply(ParDo.of(brokenDoFn));

    thrown.expectMessage(brokenDoFn.getClass().getName());
    thrown.expectCause(ThrowableMessageMatcher.hasMessage(is("oh noes!")));
    p.run();
  }

  @Test
  public void pipelineOptionsDisplayDataExceptionShouldFail() {
    Object brokenValueType = new Object() {
      @JsonValue
      public int getValue () {
        return 42;
      }

      @Override
      public String toString() {
        throw new RuntimeException("oh noes!!");
      }
    };

    Pipeline p = getPipeline();
    p.getOptions().as(ObjectPipelineOptions.class).setValue(brokenValueType);

    p.apply(Create.of(1, 2, 3));

    thrown.expectMessage(PipelineOptions.class.getName());
    thrown.expectCause(ThrowableMessageMatcher.hasMessage(is("oh noes!!")));
    p.run();
  }

  /** {@link PipelineOptions} to inject broken object type. */
  public interface ObjectPipelineOptions extends PipelineOptions {
    Object getValue();
    void setValue(Object value);
  }


  private Pipeline getPipeline() {
    PipelineOptions opts = PipelineOptionsFactory.create();
    opts.setRunner(InProcessPipelineRunner.class);

    Pipeline p = Pipeline.create(opts);
    return p;
  }
}

