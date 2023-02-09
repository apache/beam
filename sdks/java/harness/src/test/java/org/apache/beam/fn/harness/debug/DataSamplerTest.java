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
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.vendor.grpc.v1p48p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DataSamplerTest {
  byte[] encodeInt(Integer i) throws IOException {
    VarIntCoder coder = VarIntCoder.of();
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    coder.encode(i, stream);
    return stream.toByteArray();
  }

  byte[] encodeString(String s) throws IOException {
    StringUtf8Coder coder = StringUtf8Coder.of();
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    coder.encode(s, stream);
    return stream.toByteArray();
  }

  BeamFnApi.InstructionResponse getAllSamples(DataSampler dataSampler) {
    BeamFnApi.InstructionRequest request =
        BeamFnApi.InstructionRequest.newBuilder()
            .setSample(BeamFnApi.SampleDataRequest.newBuilder().build())
            .build();
    return dataSampler.handleDataSampleRequest(request).build();
  }

  BeamFnApi.InstructionResponse getSamplesForPCollection(
      DataSampler dataSampler, String pcollection) {
    BeamFnApi.InstructionRequest request =
        BeamFnApi.InstructionRequest.newBuilder()
            .setSample(
                BeamFnApi.SampleDataRequest.newBuilder().addPcollectionIds(pcollection).build())
            .build();
    return dataSampler.handleDataSampleRequest(request).build();
  }

  BeamFnApi.InstructionResponse getSamplesForDescriptors(
      DataSampler dataSampler, List<String> descriptors) {
    BeamFnApi.InstructionRequest request =
        BeamFnApi.InstructionRequest.newBuilder()
            .setSample(
                BeamFnApi.SampleDataRequest.newBuilder()
                    .addAllProcessBundleDescriptorIds(descriptors)
                    .build())
            .build();
    return dataSampler.handleDataSampleRequest(request).build();
  }

  BeamFnApi.InstructionResponse getSamplesFor(
      DataSampler dataSampler, String descriptor, String pcollection) {
    BeamFnApi.InstructionRequest request =
        BeamFnApi.InstructionRequest.newBuilder()
            .setSample(
                BeamFnApi.SampleDataRequest.newBuilder()
                    .addProcessBundleDescriptorIds(descriptor)
                    .addPcollectionIds(pcollection)
                    .build())
            .build();
    return dataSampler.handleDataSampleRequest(request).build();
  }

  void assertHasSamples(
      BeamFnApi.InstructionResponse response, String pcollection, Iterable<byte[]> elements) {
    Map<String, BeamFnApi.SampleDataResponse.ElementList> elementSamplesMap =
        response.getSample().getElementSamplesMap();

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

  /**
   * Smoke test that a samples show in the output map.
   *
   * @throws Exception
   */
  @Test
  public void testSingleOutput() throws Exception {
    DataSampler sampler = new DataSampler();

    VarIntCoder coder = VarIntCoder.of();
    sampler.sampleOutput("descriptor-id", "pcollection-id", coder).sample(1);

    BeamFnApi.InstructionResponse samples = getAllSamples(sampler);
    assertHasSamples(samples, "pcollection-id", Collections.singleton(encodeInt(1)));
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
    sampler.sampleOutput("descriptor-id", "pcollection-id-1", coder).sample(1);
    sampler.sampleOutput("descriptor-id", "pcollection-id-2", coder).sample(2);

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
  public void testMultipleDescriptors() throws Exception {
    DataSampler sampler = new DataSampler();

    VarIntCoder coder = VarIntCoder.of();
    sampler.sampleOutput("descriptor-id-1", "pcollection-id", coder).sample(1);
    sampler.sampleOutput("descriptor-id-2", "pcollection-id", coder).sample(2);

    BeamFnApi.InstructionResponse samples = getAllSamples(sampler);
    assertHasSamples(samples, "pcollection-id", ImmutableList.of(encodeInt(1), encodeInt(2)));
  }

  void generateStringSamples(DataSampler sampler) {
    StringUtf8Coder coder = StringUtf8Coder.of();
    sampler.sampleOutput("a", "1", coder).sample("a1");
    sampler.sampleOutput("a", "2", coder).sample("a2");
    sampler.sampleOutput("b", "1", coder).sample("b1");
    sampler.sampleOutput("b", "2", coder).sample("b2");
  }

  /**
   * Test that samples can be filtered based on ProcessBundleDescriptor id.
   *
   * @throws Exception
   */
  @Test
  public void testFiltersSingleDescriptorId() throws Exception {
    DataSampler sampler = new DataSampler(10, 10);
    generateStringSamples(sampler);

    BeamFnApi.InstructionResponse samples =
        getSamplesForDescriptors(sampler, ImmutableList.of("a"));
    assertHasSamples(samples, "1", Collections.singleton(encodeString("a1")));
    assertHasSamples(samples, "2", Collections.singleton(encodeString("a2")));
  }

  /**
   * Test that samples are unioned based on ProcessBundleDescriptor id.
   *
   * @throws Exception
   */
  @Test
  public void testFiltersMultipleDescriptorId() throws Exception {
    DataSampler sampler = new DataSampler(10, 10);
    generateStringSamples(sampler);

    BeamFnApi.InstructionResponse samples =
        getSamplesForDescriptors(sampler, ImmutableList.of("a", "b"));
    assertHasSamples(samples, "1", ImmutableList.of(encodeString("a1"), encodeString("b1")));
    assertHasSamples(samples, "2", ImmutableList.of(encodeString("a2"), encodeString("b2")));
  }

  /**
   * Test that samples can be filtered based on PCollection id.
   *
   * @throws Exception
   */
  @Test
  public void testFiltersSinglePCollectionId() throws Exception {
    DataSampler sampler = new DataSampler(10, 10);
    generateStringSamples(sampler);

    BeamFnApi.InstructionResponse samples = getSamplesForPCollection(sampler, "1");
    assertHasSamples(samples, "1", ImmutableList.of(encodeString("a1"), encodeString("b1")));
  }

  /**
   * Test that samples can be filtered both on PCollection and ProcessBundleDescriptor id.
   *
   * @throws Exception
   */
  @Test
  public void testFiltersDescriptorAndPCollectionIds() throws Exception {
    List<String> descriptorIds = ImmutableList.of("a", "b");
    List<String> pcollectionIds = ImmutableList.of("1", "2");

    // Try all combinations for descriptor and PCollection ids.
    for (String descriptorId : descriptorIds) {
      for (String pcollectionId : pcollectionIds) {
        DataSampler sampler = new DataSampler(10, 10);
        generateStringSamples(sampler);

        BeamFnApi.InstructionResponse samples = getSamplesFor(sampler, descriptorId, pcollectionId);
        System.out.print("Testing: " + descriptorId + pcollectionId + "...");
        assertThat(samples.getSample().getElementSamplesMap().size(), equalTo(1));
        assertHasSamples(
            samples,
            pcollectionId,
            Collections.singleton(encodeString(descriptorId + pcollectionId)));
        System.out.println("ok");
      }
    }
  }
}
