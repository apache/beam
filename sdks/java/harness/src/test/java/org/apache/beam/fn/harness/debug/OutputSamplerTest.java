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

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
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
    OutputSampler<Integer> outputSampler = new OutputSampler<>(coder, 10, 2);

    CountDownLatch startSignal = new CountDownLatch(1);
    CountDownLatch doneSignal = new CountDownLatch(2);

    // Iteration count was empirically chosen to have a high probability of failure without the
    // test going for too long.
    // Generates a range of numbers from 0 to 1000000.
    Thread sampleThreadA =
        new Thread(
            () -> {
              try {
                startSignal.await();
              } catch (InterruptedException e) {
                return;
              }

              for (int i = 0; i < 1000000; i++) {
                outputSampler.sample(i);
              }

              doneSignal.countDown();
            });

    // Generates a range of numbers from -1000000 to 0.
    Thread sampleThreadB =
        new Thread(
            () -> {
              try {
                startSignal.await();
              } catch (InterruptedException e) {
                return;
              }

              for (int i = -1000000; i < 0; i++) {
                outputSampler.sample(i);
              }

              doneSignal.countDown();
            });

    // Ready the threads.
    sampleThreadA.start();
    sampleThreadB.start();

    // Start the threads at the same time.
    startSignal.countDown();

    // Generate contention by sampling at the same time as the samples are generated.
    List<BeamFnApi.SampledElement> samples = new ArrayList<>();
    while (doneSignal.getCount() > 0) {
      samples.addAll(outputSampler.samples());
    }

    // Stop the threads and sort the samples from which thread it came from.
    sampleThreadA.join();
    sampleThreadB.join();
    List<Integer> samplesFromThreadA = new ArrayList<>();
    List<Integer> samplesFromThreadB = new ArrayList<>();
    for (BeamFnApi.SampledElement sampledElement : samples) {
      int el = coder.decode(sampledElement.getElement().newInput());
      if (el >= 0) {
        samplesFromThreadA.add(el);
      } else {
        samplesFromThreadB.add(el);
      }
    }

    // Copy the array and sort it.
    List<Integer> sortedSamplesFromThreadA = new ArrayList<>(samplesFromThreadA);
    List<Integer> sortedSamplesFromThreadB = new ArrayList<>(samplesFromThreadB);
    Collections.sort(sortedSamplesFromThreadA);
    Collections.sort(sortedSamplesFromThreadB);

    // Order is preserved when getting the samples. If there is a weird race condition, these
    // numbers may be out of order.
    assertEquals(samplesFromThreadA, sortedSamplesFromThreadA);
    assertEquals(samplesFromThreadB, sortedSamplesFromThreadB);
  }
}
