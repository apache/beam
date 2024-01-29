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
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.fnexecution.v1.BeamFnApi.SampledElement;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DataSamplerTest {
  byte[] encodeInt(Integer i) throws IOException {
    VarIntCoder coder = VarIntCoder.of();
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    coder.encode(i, stream, Coder.Context.NESTED);
    return stream.toByteArray();
  }

  byte[] encodeString(String s) throws IOException {
    StringUtf8Coder coder = StringUtf8Coder.of();
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    coder.encode(s, stream, Coder.Context.NESTED);
    return stream.toByteArray();
  }

  byte[] encodeByteArray(byte[] b) throws IOException {
    ByteArrayCoder coder = ByteArrayCoder.of();
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    coder.encode(b, stream, Coder.Context.NESTED);
    return stream.toByteArray();
  }

  <T> WindowedValue<T> globalWindowedValue(T el) {
    return WindowedValue.valueInGlobalWindow(el);
  }

  BeamFnApi.InstructionResponse getAllSamples(DataSampler dataSampler) {
    BeamFnApi.InstructionRequest request =
        BeamFnApi.InstructionRequest.newBuilder()
            .setSampleData(BeamFnApi.SampleDataRequest.newBuilder().build())
            .build();
    return dataSampler.handleDataSampleRequest(request).build();
  }

  BeamFnApi.InstructionResponse getSamplesForPCollection(
      DataSampler dataSampler, String pcollection) {
    BeamFnApi.InstructionRequest request =
        BeamFnApi.InstructionRequest.newBuilder()
            .setSampleData(
                BeamFnApi.SampleDataRequest.newBuilder().addPcollectionIds(pcollection).build())
            .build();
    return dataSampler.handleDataSampleRequest(request).build();
  }

  BeamFnApi.InstructionResponse getSamplesForPCollections(
      DataSampler dataSampler, Iterable<String> pcollections) {
    BeamFnApi.InstructionRequest request =
        BeamFnApi.InstructionRequest.newBuilder()
            .setSampleData(
                BeamFnApi.SampleDataRequest.newBuilder().addAllPcollectionIds(pcollections).build())
            .build();
    return dataSampler.handleDataSampleRequest(request).build();
  }

  void assertHasSamples(
      BeamFnApi.InstructionResponse response, String pcollection, Iterable<byte[]> elements) {
    Map<String, BeamFnApi.SampleDataResponse.ElementList> elementSamplesMap =
        response.getSampleData().getElementSamplesMap();

    assertFalse(elementSamplesMap.isEmpty());

    BeamFnApi.SampleDataResponse.ElementList elementList = elementSamplesMap.get(pcollection);
    assertNotNull(elementList);

    List<BeamFnApi.SampledElement> expectedSamples = new ArrayList<>();
    for (byte[] el : elements) {
      expectedSamples.add(
          BeamFnApi.SampledElement.newBuilder().setElement(ByteString.copyFrom(el)).build());
    }

    assertTrue(elementList.getElementsList().containsAll(expectedSamples));
  }

  void assertHasSamples(
      BeamFnApi.InstructionResponse response,
      String pcollection,
      List<BeamFnApi.SampledElement> elements) {
    Map<String, BeamFnApi.SampleDataResponse.ElementList> elementSamplesMap =
        response.getSampleData().getElementSamplesMap();

    assertFalse(elementSamplesMap.isEmpty());

    BeamFnApi.SampleDataResponse.ElementList elementList = elementSamplesMap.get(pcollection);
    assertNotNull(elementList);

    assertTrue(elementList.getElementsList().containsAll(elements));
  }

  /**
   * Smoke test that a samples show in the output map.
   *
   * @throws Exception
   */
  @Test
  public void testSingleOutput() throws Exception {
    DataSampler sampler = new DataSampler();

    VarIntCoder coder = VarIntCoder.of();
    sampler.sampleOutput("pcollection-id", coder).sample(globalWindowedValue(1));

    BeamFnApi.InstructionResponse samples = getAllSamples(sampler);
    assertHasSamples(samples, "pcollection-id", Collections.singleton(encodeInt(1)));
  }

  /**
   * Smoke test that a sample shows in the output map.
   *
   * @throws Exception
   */
  @Test
  public void testNestedContext() throws Exception {
    DataSampler sampler = new DataSampler();

    String rawString = "hello";
    byte[] byteArray = rawString.getBytes(StandardCharsets.US_ASCII);
    ByteArrayCoder coder = ByteArrayCoder.of();
    sampler.sampleOutput("pcollection-id", coder).sample(globalWindowedValue(byteArray));

    BeamFnApi.InstructionResponse samples = getAllSamples(sampler);
    assertHasSamples(samples, "pcollection-id", Collections.singleton(encodeByteArray(byteArray)));
  }

  /**
   * Test that sampling multiple PCollections under the same descriptor is OK.
   *
   * @throws Exception
   */
  @Test
  public void testMultipleOutputs() throws Exception {
    DataSampler sampler = new DataSampler();

    VarIntCoder coder = VarIntCoder.of();
    sampler.sampleOutput("pcollection-id-1", coder).sample(globalWindowedValue(1));
    sampler.sampleOutput("pcollection-id-2", coder).sample(globalWindowedValue(2));

    BeamFnApi.InstructionResponse samples = getAllSamples(sampler);
    assertHasSamples(samples, "pcollection-id-1", Collections.singleton(encodeInt(1)));
    assertHasSamples(samples, "pcollection-id-2", Collections.singleton(encodeInt(2)));
  }

  /**
   * Test that the response contains samples from the same PCollection across descriptors.
   *
   * @throws Exception
   */
  @Test
  public void testMultipleSamePCollections() throws Exception {
    DataSampler sampler = new DataSampler();

    VarIntCoder coder = VarIntCoder.of();
    sampler.sampleOutput("pcollection-id", coder).sample(globalWindowedValue(1));
    sampler.sampleOutput("pcollection-id", coder).sample(globalWindowedValue(2));

    BeamFnApi.InstructionResponse samples = getAllSamples(sampler);
    assertHasSamples(samples, "pcollection-id", ImmutableList.of(encodeInt(1), encodeInt(2)));
  }

  void generateStringSamples(DataSampler sampler) {
    StringUtf8Coder coder = StringUtf8Coder.of();
    sampler.sampleOutput("a", coder).sample(globalWindowedValue("a1"));
    sampler.sampleOutput("a", coder).sample(globalWindowedValue("a2"));
    sampler.sampleOutput("b", coder).sample(globalWindowedValue("b1"));
    sampler.sampleOutput("b", coder).sample(globalWindowedValue("b2"));
    sampler.sampleOutput("c", coder).sample(globalWindowedValue("c1"));
    sampler.sampleOutput("c", coder).sample(globalWindowedValue("c2"));
  }

  /**
   * Test that samples can be filtered based on PCollection id.
   *
   * @throws Exception
   */
  @Test
  public void testFiltersSinglePCollectionId() throws Exception {
    DataSampler sampler = new DataSampler(10, 10, false);
    generateStringSamples(sampler);

    BeamFnApi.InstructionResponse samples = getSamplesForPCollection(sampler, "a");
    assertHasSamples(samples, "a", ImmutableList.of(encodeString("a1"), encodeString("a2")));
  }

  /**
   * Test that samples can be filtered both on PCollection and ProcessBundleDescriptor id.
   *
   * @throws Exception
   */
  @Test
  public void testFiltersMultiplePCollectionIds() throws Exception {
    List<String> pcollectionIds = ImmutableList.of("a", "c");

    DataSampler sampler = new DataSampler(10, 10, false);
    generateStringSamples(sampler);

    BeamFnApi.InstructionResponse samples = getSamplesForPCollections(sampler, pcollectionIds);
    assertThat(samples.getSampleData().getElementSamplesMap().size(), equalTo(2));
    assertHasSamples(samples, "a", ImmutableList.of(encodeString("a1"), encodeString("a2")));
    assertHasSamples(samples, "c", ImmutableList.of(encodeString("c1"), encodeString("c2")));
  }

  /**
   * Test that samples can be taken from the DataSampler while adding new OutputSamplers. This fails
   * with a ConcurrentModificationException if there is a bug.
   *
   * @throws Exception
   */
  @Test
  public void testConcurrentNewSampler() throws Exception {
    DataSampler sampler = new DataSampler();
    VarIntCoder coder = VarIntCoder.of();

    // Make threads that will create 100 individual OutputSamplers each.
    Thread[] sampleThreads = new Thread[100];
    CountDownLatch startSignal = new CountDownLatch(1);
    CountDownLatch doneSignal = new CountDownLatch(sampleThreads.length);

    for (int i = 0; i < sampleThreads.length; i++) {
      sampleThreads[i] =
          new Thread(
              () -> {
                try {
                  startSignal.await();
                } catch (InterruptedException e) {
                  return;
                }

                for (int j = 0; j < 100; j++) {
                  sampler.sampleOutput("pcollection-" + j, coder).sample(globalWindowedValue(0));
                }

                doneSignal.countDown();
              });
      sampleThreads[i].start();
    }

    startSignal.countDown();
    while (doneSignal.getCount() > 0) {
      sampler.handleDataSampleRequest(
          BeamFnApi.InstructionRequest.newBuilder()
              .setSampleData(BeamFnApi.SampleDataRequest.newBuilder())
              .build());
    }

    for (Thread sampleThread : sampleThreads) {
      sampleThread.join();
    }
  }

  /**
   * Tests that including the "enable_always_on_exception_sampling" can sample.
   *
   * @throws Exception
   */
  @Test
  public void testEnableAlwaysOnExceptionSampling() throws Exception {
    ExperimentalOptions experimentalOptions = PipelineOptionsFactory.as(ExperimentalOptions.class);
    experimentalOptions.setExperiments(
        Collections.singletonList("enable_always_on_exception_sampling"));
    DataSampler sampler = DataSampler.create(experimentalOptions);
    assertNotNull(sampler);

    VarIntCoder coder = VarIntCoder.of();
    OutputSampler<Integer> outputSampler = sampler.sampleOutput("pcollection-id", coder);
    ElementSample<Integer> elementSample = outputSampler.sample(globalWindowedValue(1));
    outputSampler.exception(elementSample, new RuntimeException(), "", "");

    outputSampler.sample(globalWindowedValue(2));

    BeamFnApi.InstructionResponse samples = getAllSamples(sampler);
    List<SampledElement> expectedSamples =
        ImmutableList.of(
            SampledElement.newBuilder()
                .setElement(ByteString.copyFrom(encodeInt(1)))
                .setException(
                    SampledElement.Exception.newBuilder()
                        .setError(new RuntimeException().toString()))
                .build());
    assertHasSamples(samples, "pcollection-id", expectedSamples);
  }

  /**
   * Tests that "disable_always_on_exception_sampling" overrides the always on experiment.
   *
   * @throws Exception
   */
  @Test
  public void testDisableAlwaysOnExceptionSampling() throws Exception {
    ExperimentalOptions experimentalOptions = PipelineOptionsFactory.as(ExperimentalOptions.class);
    experimentalOptions.setExperiments(
        ImmutableList.of(
            "enable_always_on_exception_sampling", "disable_always_on_exception_sampling"));
    DataSampler sampler = DataSampler.create(experimentalOptions);
    assertNull(sampler);
  }

  /**
   * Tests that the "enable_data_sampling" experiment overrides
   * "disable_always_on_exception_sampling".
   *
   * @throws Exception
   */
  @Test
  public void testDisableAlwaysOnExceptionSamplingWithEnableDataSampling() throws Exception {
    ExperimentalOptions experimentalOptions = PipelineOptionsFactory.as(ExperimentalOptions.class);
    experimentalOptions.setExperiments(
        ImmutableList.of(
            "enable_data_sampling",
            "enable_always_on_exception_sampling",
            "disable_always_on_exception_sampling"));
    DataSampler sampler = DataSampler.create(experimentalOptions);
    assertNotNull(sampler);

    VarIntCoder coder = VarIntCoder.of();
    OutputSampler<Integer> outputSampler = sampler.sampleOutput("pcollection-id", coder);
    ElementSample<Integer> elementSample = outputSampler.sample(globalWindowedValue(1));
    outputSampler.exception(elementSample, new RuntimeException(), "", "");

    outputSampler.sample(globalWindowedValue(2));

    BeamFnApi.InstructionResponse samples = getAllSamples(sampler);
    List<SampledElement> expectedSamples =
        ImmutableList.of(
            SampledElement.newBuilder()
                .setElement(ByteString.copyFrom(encodeInt(1)))
                .setException(
                    SampledElement.Exception.newBuilder()
                        .setError(new RuntimeException().toString()))
                .build());
    assertHasSamples(samples, "pcollection-id", expectedSamples);
  }
}
