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
package org.apache.beam.runners.direct;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;

import org.apache.beam.runners.direct.InProcessPipelineRunner.InProcessPipelineResult;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;

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

    PAssert.that(countStrs).containsInAnyOrder("baz: 1", "bar: 2", "foo: 3");

    InProcessPipelineResult result = ((InProcessPipelineResult) p.run());
    result.awaitCompletion();
  }

  @Test(timeout = 5000L)
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
    PAssert.thatMap(countsBackToString).isEqualTo(expected);
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

  /** {@link PipelineOptions} to inject bad object implementations. */
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
