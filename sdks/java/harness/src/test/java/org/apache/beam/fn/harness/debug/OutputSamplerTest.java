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
package org.apache.beam.fn.harness.debug;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.vendor.grpc.v1p48p1.com.google.protobuf.ByteString;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class OutputSamplerTest {
  public BeamFnApi.SampledElement encodeInt(Integer i) throws IOException {
    VarIntCoder coder = VarIntCoder.of();
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    coder.encode(i, stream);
    return BeamFnApi.SampledElement.newBuilder()
        .setElement(ByteString.copyFrom(stream.toByteArray()))
        .build();
  }

  /**
   * Test that the first N are always sampled.
   *
   * @throws Exception when encoding fails (shouldn't happen).
   */
  @Test
  public void testSamplesFirstN() throws Exception {
    VarIntCoder coder = VarIntCoder.of();
    OutputSampler<Integer> outputSampler = new OutputSampler<>(coder, 10, 10);

    // Purposely go over maxSamples and sampleEveryN. This helps to increase confidence.
    for (int i = 0; i < 15; ++i) {
      outputSampler.sample(i);
    }

    // The expected list is only 0..9 inclusive.
    List<BeamFnApi.SampledElement> expected = new ArrayList<>();
    for (int i = 0; i < 10; ++i) {
      expected.add(encodeInt(i));
    }

    List<BeamFnApi.SampledElement> samples = outputSampler.samples();
    assertThat(samples, containsInAnyOrder(expected.toArray()));
  }

  /**
   * Test that the previous values are overwritten and only the most recent `maxSamples` are kept.
   *
   * @throws Exception when encoding fails (shouldn't happen).
   */
  @Test
  public void testActsLikeCircularBuffer() throws Exception {
    VarIntCoder coder = VarIntCoder.of();
    OutputSampler<Integer> outputSampler = new OutputSampler<>(coder, 5, 20);

    for (int i = 0; i < 100; ++i) {
      outputSampler.sample(i);
    }

    // The first 10 are always sampled, but with maxSamples = 5, the first ten are downsampled to
    // 4..9 inclusive. Then,
    // the 20th element is sampled (19) and every 20 after.
    List<BeamFnApi.SampledElement> expected = new ArrayList<>();
    expected.add(encodeInt(19));
    expected.add(encodeInt(39));
    expected.add(encodeInt(59));
    expected.add(encodeInt(79));
    expected.add(encodeInt(99));

    List<BeamFnApi.SampledElement> samples = outputSampler.samples();
    assertThat(samples, containsInAnyOrder(expected.toArray()));
  }

  /**
   * Test that sampling a PCollection while retrieving samples from multiple threads is ok.
   *
   * @throws Exception
   */
  @Test
  public void testConcurrentSamples() throws Exception {
    VarIntCoder coder = VarIntCoder.of();
    OutputSampler<Integer> outputSampler = new OutputSampler<>(coder, 100000, 1);

    // Iteration count was empirically chosen to have a high probability of failure without the
    // test going for too long.
    Thread sampleThreadA =
        new Thread(
            () -> {
              for (int i = 0; i < 10000000; i++) {
                outputSampler.sample(i);
              }
            });

    Thread sampleThreadB =
        new Thread(
            () -> {
              for (int i = 0; i < 10000000; i++) {
                outputSampler.sample(i);
              }
            });

    sampleThreadA.start();
    sampleThreadB.start();

    for (int i = 0; i < 10000; i++) {
      outputSampler.samples();
    }
  }
}
