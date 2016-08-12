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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.VarIntCoder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.InProcessPipelineResult;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.SimpleFunction;
import com.google.cloud.dataflow.sdk.transforms.display.DisplayData;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import com.google.common.collect.ImmutableMap;

import com.fasterxml.jackson.annotation.JsonValue;

import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

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

  private static AtomicInteger changed;
  @Test
  public void reusePipelineSucceeds() throws Throwable {
    Pipeline p = getPipeline();

    changed = new AtomicInteger(0);

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

    counts.apply(ParDo.of(new DoFn<KV<String, Long>, Void>() {
      @Override
      public void processElement(ProcessContext c) throws Exception {
        changed.getAndIncrement();
      }
    }));

    DataflowAssert.that(countStrs).containsInAnyOrder("baz: 1", "bar: 2", "foo: 3");

    InProcessPipelineResult result = ((InProcessPipelineResult) p.run());
    result.awaitCompletion();

    InProcessPipelineResult otherResult = ((InProcessPipelineResult) p.run());
    otherResult.awaitCompletion();

    assertThat("Each element should have been processed twice", changed.get(), equalTo(6));
  }

  @Test
  public void byteArrayCountShouldSucceed() {
    Pipeline p = getPipeline();

    SerializableFunction<Integer, byte[]> getBytes = new SerializableFunction<Integer, byte[]>() {
      @Override
      public byte[] apply(Integer input) {
        try {
          return CoderUtils.encodeToByteArray(VarIntCoder.of(), input);
        } catch (CoderException e) {
          fail("Unexpected Coder Exception " + e);
          throw new AssertionError("Unreachable");
        }
      }
    };
    TypeDescriptor<byte[]> td = new TypeDescriptor<byte[]>() {
    };
    PCollection<byte[]> foos =
        p.apply(Create.of(1, 1, 1, 2, 2, 3)).apply(MapElements.via(getBytes).withOutputType(td));
    PCollection<byte[]> msync =
        p.apply(Create.of(1, -2, -8, -16)).apply(MapElements.via(getBytes).withOutputType(td));
    PCollection<byte[]> bytes =
        PCollectionList.of(foos).and(msync).apply(Flatten.<byte[]>pCollections());
    PCollection<KV<byte[], Long>> counts = bytes.apply(Count.<byte[]>perElement());
    PCollection<KV<Integer, Long>> countsBackToString =
        counts.apply(MapElements.via(new SimpleFunction<KV<byte[], Long>, KV<Integer, Long>>() {
          @Override
          public KV<Integer, Long> apply(KV<byte[], Long> input) {
            try {
              return KV.of(CoderUtils.decodeFromByteArray(VarIntCoder.of(), input.getKey()),
                  input.getValue());
            } catch (CoderException e) {
              fail("Unexpected Coder Exception " + e);
              throw new AssertionError("Unreachable");
        }
      }
    }));

    Map<Integer, Long> expected = ImmutableMap.<Integer, Long>builder().put(1, 4L)
        .put(2, 2L)
        .put(3, 1L)
        .put(-2, 1L)
        .put(-8, 1L)
        .put(-16, 1L)
        .build();
    DataflowAssert.thatMap(countsBackToString).isEqualTo(expected);
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

