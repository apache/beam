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

package org.apache.beam.runners.core;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import java.io.Serializable;
import org.apache.beam.sdk.runners.PTransformMatcher;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link PTransformMatcher}.
 */
@RunWith(JUnit4.class)
public class PTransformMatchersTest implements Serializable {
  @Rule
  public transient TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Test
  public void classEqualToMatchesSameClass() {
    PTransformMatcher matcher = PTransformMatchers.classEqualTo(ParDo.Bound.class);
    PCollection<Integer> input = p.apply(Create.of(1));
    ParDo.Bound<Integer, Integer> pardo = ParDo.of(new DoFn<Integer, Integer>() {
      @ProcessElement
      public void doStuff(ProcessContext ctxt) {
      }
    });
    PCollection<Integer> output = input.apply(pardo);

    AppliedPTransform<?, ?, ?> application =
        AppliedPTransform
            .<PCollection<Integer>, PCollection<Integer>,
                PTransform<? super PCollection<Integer>, PCollection<Integer>>>
                of("DoStuff", input.expand(), output.expand(), pardo, p);

    assertThat(matcher.matches(application), is(true));
  }

  @Test
  public void classEqualToMatchesSubClass() {
    class MyPTransform extends PTransform<PCollection<Integer>, PCollection<Integer>> {
      @Override
      public PCollection<Integer> expand(PCollection<Integer> input) {
        return input;
      }
    }
    PTransformMatcher matcher = PTransformMatchers.classEqualTo(MyPTransform.class);
    PCollection<Integer> input = p.apply(Create.of(1));
    MyPTransform subclass = new MyPTransform() {};

    assertThat(subclass.getClass(), not(Matchers.<Class<?>>equalTo(MyPTransform.class)));
    assertThat(subclass, instanceOf(MyPTransform.class));
    PCollection<Integer> output = input.apply(subclass);

    AppliedPTransform<?, ?, ?> application =
        AppliedPTransform
            .<PCollection<Integer>, PCollection<Integer>,
                PTransform<PCollection<Integer>, PCollection<Integer>>>
                of("DoStuff", input.expand(), output.expand(), subclass, p);

    assertThat(matcher.matches(application), is(false));
  }

  @Test
  public void classEqualToDoesNotMatchUnrelatedClass() {
    PTransformMatcher matcher = PTransformMatchers.classEqualTo(ParDo.Bound.class);
    PCollection<Integer> input = p.apply(Create.of(1));
    Window.Bound<Integer> window = Window.into(new GlobalWindows());
    PCollection<Integer> output = input.apply(window);

    AppliedPTransform<?, ?, ?> application =
        AppliedPTransform
            .<PCollection<Integer>, PCollection<Integer>,
                PTransform<PCollection<Integer>, PCollection<Integer>>>
                of("DoStuff", input.expand(), output.expand(), window, p);

    assertThat(matcher.matches(application), is(false));
  }
}
