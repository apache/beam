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
package org.apache.beam.sdk.values;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.AppliedPTransform;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Create.Values;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link PValueBase}. */
@RunWith(JUnit4.class)
public class PValueBaseTest {
  TestPipeline p;
  private PValueBase pvalue;

  @Before
  public void setup() throws Exception {
    p = TestPipeline.create();
    pvalue =
        new PValueBase(p) {
          @Override
          public String getName() {
            return "foo.out";
          }
        };
  }

  @Test
  public void testRecordAsOutput() {
    AppliedPTransform<PBegin, PCollection<Integer>, Values<Integer>> transform =
        AppliedPTransform.of(
            "foo",
            p.begin(),
            PCollection.<Integer>createPrimitiveOutputInternal(
                p, WindowingStrategy.globalDefault(), IsBounded.BOUNDED),
            Create.of(1, 2, 3));
    assertThat(pvalue.getProducingTransformInternal(), is(nullValue()));
    pvalue.recordAsOutput(transform);
    assertThat(
        pvalue.getProducingTransformInternal(),
        Matchers.<AppliedPTransform<?, ?, ?>>equalTo(transform));

    AppliedPTransform<PBegin, PCollection<Integer>, Values<Integer>> repl =
        AppliedPTransform.of(
            "bar",
            p.begin(),
            PCollection.<Integer>createPrimitiveOutputInternal(
                p, WindowingStrategy.globalDefault(), IsBounded.BOUNDED),
            Create.of(1, 2, 3));
    assertThat(transform, not(equalTo(repl)));
    pvalue.recordAsOutput(repl);
    assertThat(
        pvalue.getProducingTransformInternal(),
        Matchers.<AppliedPTransform<?, ?, ?>>equalTo(transform));
  }

  @Test
  public void testReplaceAsOutput() {
    AppliedPTransform<PBegin, PCollection<Integer>, Values<Integer>> transform =
        AppliedPTransform.of(
            "foo",
            p.begin(),
            PCollection.<Integer>createPrimitiveOutputInternal(
                p, WindowingStrategy.globalDefault(), IsBounded.BOUNDED),
            Create.of(1, 2, 3));
    assertThat(pvalue.getProducingTransformInternal(), is(nullValue()));
    pvalue.recordAsOutput(transform);
    assertThat(
        pvalue.getProducingTransformInternal(),
        Matchers.<AppliedPTransform<?, ?, ?>>equalTo(transform));

    AppliedPTransform<PBegin, PCollection<Integer>, Values<Integer>> repl =
        AppliedPTransform.of(
            "bar",
            p.begin(),
            PCollection.<Integer>createPrimitiveOutputInternal(
                p, WindowingStrategy.globalDefault(), IsBounded.BOUNDED),
            Create.of(1, 2, 3));
    assertThat(transform, not(equalTo(repl)));
    pvalue.replaceAsOutput(transform, repl);
    assertThat(
        pvalue.getProducingTransformInternal(), Matchers.<AppliedPTransform<?, ?, ?>>equalTo(repl));
  }
}
