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
import java.util.Map;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class OutputSamplerTest {
  public byte[] encodeInt(Integer i) throws IOException {
    VarIntCoder coder = VarIntCoder.of();
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    coder.encode(i, stream);
    return stream.toByteArray();
  }

  @Test
  public void testSamplesFirstN() throws Exception {
    DataSampler sampler = new DataSampler(10, 10);

    VarIntCoder coder = VarIntCoder.of();
    OutputSampler<Integer> outputSampler =
        sampler.sampleOutput("descriptor-id", "pcollection-id", coder);

    for (int i = 0; i < 15; ++i) {
      outputSampler.sample(i);
    }

    List<byte[]> expected = new ArrayList<>();
    for (int i = 0; i < 10; ++i) {
      expected.add(encodeInt(i));
    }

    Map<String, List<byte[]>> samples = sampler.allSamples();
    assertThat(samples.get("pcollection-id"), containsInAnyOrder(expected.toArray()));
  }

  @Test
  public void testActsLikeCircularBuffer() throws Exception {
    DataSampler sampler = new DataSampler(5, 20);

    VarIntCoder coder = VarIntCoder.of();
    OutputSampler<Integer> outputSampler =
        sampler.sampleOutput("descriptor-id", "pcollection-id", coder);

    for (int i = 0; i < 100; ++i) {
      outputSampler.sample(i);
    }

    List<byte[]> expected = new ArrayList<>();
    expected.add(encodeInt(19));
    expected.add(encodeInt(39));
    expected.add(encodeInt(59));
    expected.add(encodeInt(79));
    expected.add(encodeInt(99));

    Map<String, List<byte[]>> samples = sampler.allSamples();
    assertThat(samples.get("pcollection-id"), containsInAnyOrder(expected.toArray()));
  }
}
