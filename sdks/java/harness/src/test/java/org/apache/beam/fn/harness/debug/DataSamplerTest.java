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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
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

  @Test
  public void testSingleOutput() throws Exception {
    DataSampler sampler = new DataSampler();

    VarIntCoder coder = VarIntCoder.of();
    sampler.sampleOutput("descriptor-id", "pcollection-id", coder).sample(1);

    Map<String, List<byte[]>> samples = sampler.samples();
    assertThat(samples.get("pcollection-id"), contains(encodeInt(1)));
  }

  @Test
  public void testMultipleOutputs() throws Exception {
    DataSampler sampler = new DataSampler();

    VarIntCoder coder = VarIntCoder.of();
    sampler.sampleOutput("descriptor-id", "pcollection-id-1", coder).sample(1);
    sampler.sampleOutput("descriptor-id", "pcollection-id-2", coder).sample(2);

    Map<String, List<byte[]>> samples = sampler.samples();
    assertThat(samples.get("pcollection-id-1"), contains(encodeInt(1)));
    assertThat(samples.get("pcollection-id-2"), contains(encodeInt(2)));
  }

  @Test
  public void testMultipleDescriptors() throws Exception {
    DataSampler sampler = new DataSampler();

    VarIntCoder coder = VarIntCoder.of();
    sampler.sampleOutput("descriptor-id-1", "pcollection-id", coder).sample(1);
    sampler.sampleOutput("descriptor-id-2", "pcollection-id", coder).sample(2);

    Map<String, List<byte[]>> samples = sampler.samples();
    assertThat(samples.get("pcollection-id"), contains(encodeInt(1), encodeInt(2)));
  }

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

  Map<String, List<byte[]>> singletonSample(String pcollectionId, byte[] element) {
    Map<String, List<byte[]>> ret = new HashMap<>();
    List<byte[]> list = new ArrayList<>();
    list.add(element);
    ret.put(pcollectionId, list);
    return ret;
  }

  void generateStringSamples(DataSampler sampler) {
    StringUtf8Coder coder = StringUtf8Coder.of();
    sampler.sampleOutput("a", "1", coder).sample("a1");
    sampler.sampleOutput("a", "2", coder).sample("a2");
    sampler.sampleOutput("b", "1", coder).sample("b1");
    sampler.sampleOutput("b", "2", coder).sample("b2");
  }

  @Test
  public void testFiltersDescriptorAndPCollectionIds() throws Exception {
    List<String> descriptorIds = ImmutableList.of("a", "b");
    List<String> pcollectionIds = ImmutableList.of("1", "2");

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
}
