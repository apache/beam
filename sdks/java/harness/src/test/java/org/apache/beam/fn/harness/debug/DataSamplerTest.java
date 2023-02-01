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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.vendor.grpc.v1p48p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class DataSamplerTest {
  public byte[] encodeInt(Integer i) throws IOException {
    VarIntCoder coder = VarIntCoder.of();
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    coder.encode(i, stream);
    return stream.toByteArray();
  }

  public byte[] encodeString(String s) throws IOException {
    StringUtf8Coder coder = StringUtf8Coder.of();
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    coder.encode(s, stream);
    return stream.toByteArray();
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

    Map<String, List<byte[]>> samples = sampler.allSamples();
    assertThat(samples.get("pcollection-id"), contains(encodeInt(1)));
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

    Map<String, List<byte[]>> samples = sampler.allSamples();
    assertThat(samples.get("pcollection-id-1"), contains(encodeInt(1)));
    assertThat(samples.get("pcollection-id-2"), contains(encodeInt(2)));
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

    Map<String, List<byte[]>> samples = sampler.allSamples();
    assertThat(samples.get("pcollection-id"), contains(encodeInt(1), encodeInt(2)));
  }

  /**
   * Test that samples can be filtered based on ProcessBundleDescriptor id.
   *
   * @throws Exception
   */
  @Test
  public void testFiltersSingleDescriptorId() throws Exception {
    DataSampler sampler = new DataSampler(10, 10);

    StringUtf8Coder coder = StringUtf8Coder.of();
    sampler.sampleOutput("a", "1", coder).sample("a1");
    sampler.sampleOutput("a", "2", coder).sample("a2");
    sampler.sampleOutput("b", "1", coder).sample("b1");
    sampler.sampleOutput("b", "2", coder).sample("b2");

    Map<String, List<byte[]>> samples =
        sampler.samplesForDescriptors(new HashSet<>(Collections.singletonList("a")));
    assertThat(samples.get("1"), contains(encodeString("a1")));
    assertThat(samples.get("2"), contains(encodeString("a2")));
  }

  /**
   * Test that samples are unioned based on ProcessBundleDescriptor id.
   *
   * @throws Exception
   */
  @Test
  public void testFiltersMultipleDescriptorId() throws Exception {
    DataSampler sampler = new DataSampler(10, 10);

    StringUtf8Coder coder = StringUtf8Coder.of();
    sampler.sampleOutput("a", "1", coder).sample("a1");
    sampler.sampleOutput("a", "2", coder).sample("a2");
    sampler.sampleOutput("b", "1", coder).sample("b1");
    sampler.sampleOutput("b", "2", coder).sample("b2");

    Map<String, List<byte[]>> samples = sampler.samplesForDescriptors(ImmutableSet.of("a", "b"));
    assertThat(samples.get("1"), contains(encodeString("a1"), encodeString("b1")));
    assertThat(samples.get("2"), contains(encodeString("a2"), encodeString("b2")));
  }

  /**
   * Test that samples can be filtered based on PCollection id.
   *
   * @throws Exception
   */
  @Test
  public void testFiltersSinglePCollectionId() throws Exception {
    DataSampler sampler = new DataSampler(10, 10);

    StringUtf8Coder coder = StringUtf8Coder.of();
    sampler.sampleOutput("a", "1", coder).sample("a1");
    sampler.sampleOutput("a", "2", coder).sample("a2");
    sampler.sampleOutput("b", "1", coder).sample("b1");
    sampler.sampleOutput("b", "2", coder).sample("b2");

    Map<String, List<byte[]>> samples =
        sampler.samplesForPCollections(new HashSet<>(Collections.singletonList("1")));
    assertThat(samples.get("1"), containsInAnyOrder(encodeString("a1"), encodeString("b1")));
  }

  void generateStringSamples(DataSampler sampler) {
    StringUtf8Coder coder = StringUtf8Coder.of();
    sampler.sampleOutput("a", "1", coder).sample("a1");
    sampler.sampleOutput("a", "2", coder).sample("a2");
    sampler.sampleOutput("b", "1", coder).sample("b1");
    sampler.sampleOutput("b", "2", coder).sample("b2");
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
        Map<String, List<byte[]>> actual =
            sampler.samplesFor(ImmutableSet.of(descriptorId), ImmutableSet.of(pcollectionId));

        System.out.print("Testing: " + descriptorId + pcollectionId + "...");
        assertThat(actual.size(), equalTo(1));
        assertThat(actual.get(pcollectionId), contains(encodeString(descriptorId + pcollectionId)));
        System.out.println("ok");
      }
    }
  }

  /**
   * Test that the DataSampler can respond with the correct samples with filters.
   *
   * @throws Exception
   */
  @Test
  public void testMakesCorrectResponse() throws Exception {
    DataSampler dataSampler = new DataSampler();
    generateStringSamples(dataSampler);

    // SampleDataRequest that filters on PCollection=1 and PBD ids = "a" or "b".
    BeamFnApi.InstructionRequest request =
        BeamFnApi.InstructionRequest.newBuilder()
            .setSample(
                BeamFnApi.SampleDataRequest.newBuilder()
                    .addPcollectionIds("1")
                    .addProcessBundleDescriptorIds("a")
                    .addProcessBundleDescriptorIds("b")
                    .build())
            .build();
    BeamFnApi.InstructionResponse actual = dataSampler.handleDataSampleRequest(request).build();
    BeamFnApi.InstructionResponse expected =
        BeamFnApi.InstructionResponse.newBuilder()
            .setSample(
                BeamFnApi.SampleDataResponse.newBuilder()
                    .putElementSamples(
                        "1",
                        BeamFnApi.SampleDataResponse.ElementList.newBuilder()
                            .addElements(
                                BeamFnApi.SampledElement.newBuilder()
                                    .setElement(ByteString.copyFrom(encodeString("a1")))
                                    .build())
                            .addElements(
                                BeamFnApi.SampledElement.newBuilder()
                                    .setElement(ByteString.copyFrom(encodeString("b1")))
                                    .build())
                            .build())
                    .build())
            .build();
    assertThat(actual, equalTo(expected));
  }
}
