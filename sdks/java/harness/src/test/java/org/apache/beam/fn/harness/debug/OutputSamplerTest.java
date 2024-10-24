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
import javax.annotation.Nullable;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
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

  public BeamFnApi.SampledElement encodeGlobalWindowedInt(Integer i) throws IOException {
    WindowedValue.WindowedValueCoder<Integer> coder =
        WindowedValue.FullWindowedValueCoder.of(VarIntCoder.of(), GlobalWindow.Coder.INSTANCE);

    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    coder.encode(WindowedValue.valueInGlobalWindow(i), stream);
    return BeamFnApi.SampledElement.newBuilder()
        .setElement(ByteString.copyFrom(stream.toByteArray()))
        .build();
  }

  public BeamFnApi.SampledElement encodeException(
      Integer i, String error, String ptransformId, @Nullable String processBundleId)
      throws IOException {
    VarIntCoder coder = VarIntCoder.of();
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    coder.encode(i, stream);

    BeamFnApi.SampledElement.Exception.Builder builder =
        BeamFnApi.SampledElement.Exception.newBuilder()
            .setTransformId(ptransformId)
            .setError(error);

    if (processBundleId != null) {
      builder.setInstructionId(processBundleId);
    }

    return BeamFnApi.SampledElement.newBuilder()
        .setElement(ByteString.copyFrom(stream.toByteArray()))
        .setException(builder)
        .build();
  }

  /**
   * Test that the first N are always sampled.
   *
   * @throws IOException when encoding fails (shouldn't happen).
   */
  @Test
  public void testSamplesFirstN() throws IOException {
    VarIntCoder coder = VarIntCoder.of();
    OutputSampler<Integer> outputSampler = new OutputSampler<>(coder, 10, 10, false);

    // Purposely go over maxSamples and sampleEveryN. This helps to increase confidence.
    for (int i = 0; i < 15; ++i) {
      outputSampler.sample(WindowedValue.valueInGlobalWindow(i));
    }

    // The expected list is only 0..9 inclusive.
    List<BeamFnApi.SampledElement> expected = new ArrayList<>();
    for (int i = 0; i < 10; ++i) {
      expected.add(encodeInt(i));
    }

    List<BeamFnApi.SampledElement> samples = outputSampler.samples();
    assertThat(samples, containsInAnyOrder(expected.toArray()));
  }

  @Test
  public void testWindowedValueSample() throws IOException {
    WindowedValue.WindowedValueCoder<Integer> coder =
        WindowedValue.FullWindowedValueCoder.of(VarIntCoder.of(), GlobalWindow.Coder.INSTANCE);

    OutputSampler<Integer> outputSampler = new OutputSampler<>(coder, 10, 10, false);
    outputSampler.sample(WindowedValue.valueInGlobalWindow(0));

    // The expected list is only 0..9 inclusive.
    List<BeamFnApi.SampledElement> expected = ImmutableList.of(encodeGlobalWindowedInt(0));
    List<BeamFnApi.SampledElement> samples = outputSampler.samples();
    assertThat(samples, containsInAnyOrder(expected.toArray()));
  }

  @Test
  public void testNonWindowedValueSample() throws IOException {
    VarIntCoder coder = VarIntCoder.of();

    OutputSampler<Integer> outputSampler = new OutputSampler<>(coder, 10, 10, false);
    outputSampler.sample(WindowedValue.valueInGlobalWindow(0));

    // The expected list is only 0..9 inclusive.
    List<BeamFnApi.SampledElement> expected = ImmutableList.of(encodeInt(0));
    List<BeamFnApi.SampledElement> samples = outputSampler.samples();
    assertThat(samples, containsInAnyOrder(expected.toArray()));
  }

  /**
   * Test that the previous values are overwritten and only the most recent `maxSamples` are kept.
   *
   * @throws IOException when encoding fails (shouldn't happen).
   */
  @Test
  public void testActsLikeCircularBuffer() throws IOException {
    VarIntCoder coder = VarIntCoder.of();
    OutputSampler<Integer> outputSampler = new OutputSampler<>(coder, 5, 20, false);

    for (int i = 0; i < 100; ++i) {
      outputSampler.sample(WindowedValue.valueInGlobalWindow(i));
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
   * Test that elements with exceptions can be sampled. TODO: test that the exception metadata is
   * set.
   *
   * @throws IOException when encoding fails (shouldn't happen).
   */
  @Test
  public void testCanSampleExceptions() throws IOException {
    VarIntCoder coder = VarIntCoder.of();
    OutputSampler<Integer> outputSampler = new OutputSampler<>(coder, 5, 20, false);

    WindowedValue<Integer> windowedValue = WindowedValue.valueInGlobalWindow(1);
    ElementSample<Integer> elementSample = outputSampler.sample(windowedValue);

    Exception exception = new RuntimeException("Test exception");
    String ptransformId = "ptransform";
    String processBundleId = "processBundle";
    outputSampler.exception(elementSample, exception, ptransformId, processBundleId);

    List<BeamFnApi.SampledElement> expected = new ArrayList<>();
    expected.add(encodeException(1, exception.toString(), ptransformId, processBundleId));

    List<BeamFnApi.SampledElement> samples = outputSampler.samples();
    assertThat(samples, containsInAnyOrder(expected.toArray()));
  }

  /**
   * Test that in the event that an exception happens multiple times in a bundle, it's only recorded
   * at the source.
   *
   * @throws IOException when encoding fails (shouldn't happen).
   */
  @Test
  public void testNoDuplicateExceptions() throws IOException {
    VarIntCoder coder = VarIntCoder.of();
    OutputSampler<Integer> outputSampler = new OutputSampler<>(coder, 5, 20, false);

    ElementSample<Integer> elementSampleA =
        outputSampler.sample(WindowedValue.valueInGlobalWindow(1));
    ElementSample<Integer> elementSampleB =
        outputSampler.sample(WindowedValue.valueInGlobalWindow(2));

    Exception exception = new RuntimeException("Test exception");
    String ptransformIdA = "ptransformA";
    String ptransformIdB = "ptransformB";
    String processBundleId = "processBundle";
    outputSampler.exception(elementSampleA, exception, ptransformIdA, processBundleId);
    outputSampler.exception(elementSampleB, exception, ptransformIdB, processBundleId);

    List<BeamFnApi.SampledElement> expected = new ArrayList<>();
    expected.add(encodeException(1, exception.toString(), ptransformIdA, processBundleId));
    expected.add(encodeInt(2));

    List<BeamFnApi.SampledElement> samples = outputSampler.samples();
    assertThat(samples, containsInAnyOrder(expected.toArray()));
  }

  /**
   * Test that exception metadata is only set if there is a process bundle.
   *
   * @throws IOException when encoding fails (shouldn't happen).
   */
  @Test
  public void testExceptionOnlySampledIfNonNullProcessBundle() throws IOException {
    VarIntCoder coder = VarIntCoder.of();
    OutputSampler<Integer> outputSampler = new OutputSampler<>(coder, 5, 20, false);

    WindowedValue<Integer> windowedValue = WindowedValue.valueInGlobalWindow(1);
    ElementSample<Integer> elementSample = outputSampler.sample(windowedValue);

    Exception exception = new RuntimeException("Test exception");
    String ptransformId = "ptransform";
    outputSampler.exception(elementSample, exception, ptransformId, null);

    List<BeamFnApi.SampledElement> expected = new ArrayList<>();
    expected.add(encodeInt(1));

    List<BeamFnApi.SampledElement> samples = outputSampler.samples();
    assertThat(samples, containsInAnyOrder(expected.toArray()));
  }

  /**
   * Tests that multiple samples don't push out exception samples.
   *
   * @throws IOException when encoding fails (shouldn't happen).
   */
  @Test
  public void testExceptionSamplesAreNotRemoved() throws IOException {
    VarIntCoder coder = VarIntCoder.of();
    OutputSampler<Integer> outputSampler = new OutputSampler<>(coder, 5, 20, false);

    WindowedValue<Integer> windowedValue = WindowedValue.valueInGlobalWindow(0);
    ElementSample<Integer> elementSample = outputSampler.sample(windowedValue);

    for (int i = 1; i < 100; ++i) {
      outputSampler.sample(WindowedValue.valueInGlobalWindow(i));
    }

    Exception exception = new RuntimeException("Test exception");
    String ptransformId = "ptransform";
    String processBundleId = "processBundle";
    outputSampler.exception(elementSample, exception, ptransformId, processBundleId);

    // The first 10 are always sampled, but with maxSamples = 5, the first ten are downsampled to
    // 4..9 inclusive. Then, the 20th element is sampled (19) and every 20 after. Finally,
    // exceptions are added to the list.
    List<BeamFnApi.SampledElement> expected = new ArrayList<>();
    expected.add(encodeInt(19));
    expected.add(encodeInt(39));
    expected.add(encodeInt(59));
    expected.add(encodeInt(79));
    expected.add(encodeInt(99));
    expected.add(encodeException(0, exception.toString(), ptransformId, processBundleId));

    List<BeamFnApi.SampledElement> samples = outputSampler.samples();
    assertThat(samples, containsInAnyOrder(expected.toArray()));
  }

  /**
   * Test that elements the onlySampleExceptions flag works.
   *
   * @throws IOException when encoding fails (shouldn't happen).
   */
  @Test
  public void testOnlySampleExceptions() throws IOException {
    VarIntCoder coder = VarIntCoder.of();
    OutputSampler<Integer> outputSampler = new OutputSampler<>(coder, 5, 20, true);

    WindowedValue<Integer> windowedValue = WindowedValue.valueInGlobalWindow(1);
    outputSampler.sample(WindowedValue.valueInGlobalWindow(2));
    ElementSample<Integer> elementSample = outputSampler.sample(windowedValue);

    Exception exception = new RuntimeException("Test exception");
    String ptransformId = "ptransform";
    String processBundleId = "processBundle";
    outputSampler.exception(elementSample, exception, ptransformId, processBundleId);

    List<BeamFnApi.SampledElement> expected = new ArrayList<>();
    expected.add(encodeException(1, exception.toString(), ptransformId, processBundleId));

    List<BeamFnApi.SampledElement> samples = outputSampler.samples();
    assertThat(samples, containsInAnyOrder(expected.toArray()));
  }

  /**
   * Test that sampling a PCollection while retrieving samples from multiple threads is ok.
   *
   * @throws IOException, InterruptedException
   */
  @Test
  public void testConcurrentSamples() throws IOException, InterruptedException {
    VarIntCoder coder = VarIntCoder.of();
    OutputSampler<Integer> outputSampler = new OutputSampler<>(coder, 10, 2, false);

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
                ElementSample<Integer> sample =
                    outputSampler.sample(WindowedValue.valueInGlobalWindow(i));
                outputSampler.exception(sample, new RuntimeException(""), "ptransformId", "pbId");
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
                ElementSample<Integer> sample =
                    outputSampler.sample(WindowedValue.valueInGlobalWindow(i));
                outputSampler.exception(sample, new RuntimeException(""), "ptransformId", "pbId");
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
