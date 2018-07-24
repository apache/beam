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

package org.apache.beam.sdk.io.synthetic;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.math3.distribution.ConstantRealDistribution;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link SyntheticStep}. */
@RunWith(JUnit4.class)
public class SyntheticStepTest {
  @Rule public final ExpectedException thrown = ExpectedException.none();

  private byte[] intToByteArray(int value) {
    return ByteBuffer.allocate(4).putInt(value).array();
  }

  private SyntheticStep.Options fromString(String jsonString) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    SyntheticStep.Options result = mapper.readValue(jsonString, SyntheticStep.Options.class);
    result.validate();
    return result;
  }

  private SyntheticStep.Options createStepOptions(
      double outputRecordsPerInputRecord,
      boolean preservesInputKeyDistribution,
      long keySizeBytes,
      long valueSizeBytes,
      long numHotKeys,
      double hotKeyFraction,
      int seed,
      SyntheticOptions.Sampler dist) {
    SyntheticStep.Options options = new SyntheticStep.Options();
    options.outputRecordsPerInputRecord = outputRecordsPerInputRecord;
    options.preservesInputKeyDistribution = preservesInputKeyDistribution;
    options.keySizeBytes = keySizeBytes;
    options.valueSizeBytes = valueSizeBytes;
    options.numHotKeys = numHotKeys;
    options.hotKeyFraction = hotKeyFraction;
    options.setSeed(seed);
    options.delayDistribution = dist;
    return options;
  }

  @Test
  public void testInvalidSyntheticStepOptionsJsonFormat() throws Exception {
    thrown.expect(JsonParseException.class);
    String syntheticStepOptions = "unknown json format";
    fromString(syntheticStepOptions);
  }

  @Test
  public void testFromString() throws Exception {
    String syntheticStepOptions =
        "{\"outputRecordsPerInputRecord\":5.0,\"preservesInputKeyDistribution\":false,"
            + "\"keySizeBytes\":10,\"valueSizeBytes\":20,\"numHotKeys\":3,\"hotKeyFraction\":0.3,"
            + "\"seed\":123456,\"delayDistribution\":{\"type\":\"const\",\"const\":10}}";
    SyntheticStep.Options stepOptions = fromString(syntheticStepOptions);
    assertEquals(5.0, stepOptions.outputRecordsPerInputRecord, 0);
    assertFalse(stepOptions.preservesInputKeyDistribution);
    assertEquals(10, stepOptions.keySizeBytes);
    assertEquals(20, stepOptions.valueSizeBytes);
    assertEquals(3, stepOptions.numHotKeys);
    assertEquals(0.3, stepOptions.hotKeyFraction, 0);
    assertEquals(10, stepOptions.nextDelay(stepOptions.seed));
    assertEquals(123456, stepOptions.seed);
  }

  @Test
  public void testSyntheticStepWithPreservingInputKeyDistribution() throws Exception {
    DoFnTester<KV<byte[], byte[]>, KV<byte[], byte[]>> fnTester =
        DoFnTester.of(
            new SyntheticStep(
                createStepOptions(
                    2.0,
                    true,
                    10,
                    20,
                    3,
                    0.3,
                    123456,
                    SyntheticOptions.fromRealDistribution(new ConstantRealDistribution(10)))));
    List<KV<byte[], byte[]>> testOutputs =
        fnTester.processBundle(
            KV.of(intToByteArray(1), intToByteArray(11)),
            KV.of(intToByteArray(2), intToByteArray(22)),
            KV.of(intToByteArray(3), intToByteArray(33)));
    // Check that output elements keep the same keys.
    List<byte[]> testOutputKeys = new ArrayList<>();
    for (KV<byte[], byte[]> item : testOutputs) {
      testOutputKeys.add(item.getKey());
    }
    List<byte[]> expected = new ArrayList<>();
    expected.add(intToByteArray(1));
    expected.add(intToByteArray(1));
    expected.add(intToByteArray(2));
    expected.add(intToByteArray(2));
    expected.add(intToByteArray(3));
    expected.add(intToByteArray(3));
    assertThat(expected, containsInAnyOrder(testOutputKeys.toArray()));
  }

  @Test
  public void testSyntheticStepWithoutPreservingInputKeyDistribution() throws Exception {
    DoFnTester<KV<byte[], byte[]>, KV<byte[], byte[]>> fnTester =
        DoFnTester.of(
            new SyntheticStep(
                createStepOptions(
                    2.0,
                    false,
                    10,
                    20,
                    3,
                    0.3,
                    123456,
                    SyntheticOptions.fromRealDistribution(new ConstantRealDistribution(10)))));
    List<KV<byte[], byte[]>> testOutputs =
        fnTester.processBundle(KV.of(intToByteArray(1), intToByteArray(11)));
    // Check that output elements have new keys.
    assertEquals(2, testOutputs.size());
    assertEquals(10, testOutputs.get(0).getKey().length);
    assertEquals(20, testOutputs.get(0).getValue().length);
  }
}
