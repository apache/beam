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

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
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
  @Rule public final transient TestPipeline p = TestPipeline.create();

  private byte[] intToByteArray(int value) {
    return ByteBuffer.allocate(4).putInt(value).array();
  }

  @Test
  public void testSyntheticStepWithPreservingInputKeyDistribution() throws Exception {
    SyntheticStep.Options options =
        SyntheticTestUtils.optionsFromString(
            "{\"outputRecordsPerInputRecord\": 2,"
                + " \"preservesInputKeyDistribution\": true,"
                + "\"keySizeBytes\": 10,"
                + "\"valueSizeBytes\": 20,"
                + "\"numHotKeys\": 3,"
                + "\"hotKeyFraction\": 0.3,"
                + "\"seed\": 123456}",
            SyntheticStep.Options.class);
    options.delayDistribution =
        SyntheticOptions.fromRealDistribution(new ConstantRealDistribution(10));

    PCollection<byte[]> result =
        p.apply(
                Create.of(
                    ImmutableList.of(
                        KV.of(intToByteArray(1), intToByteArray(11)),
                        KV.of(intToByteArray(2), intToByteArray(22)),
                        KV.of(intToByteArray(3), intToByteArray(33)))))
            .apply(ParDo.of(new SyntheticStep(options)))
            .apply(Keys.create());

    List<byte[]> expected =
        ImmutableList.of(
            intToByteArray(1),
            intToByteArray(1),
            intToByteArray(2),
            intToByteArray(2),
            intToByteArray(3),
            intToByteArray(3));
    PAssert.that(result).containsInAnyOrder(expected);
    p.run().waitUntilFinish();
  }

  @Test
  public void testSyntheticStepWithoutPreservingInputKeyDistribution() throws Exception {
    SyntheticStep.Options options =
        SyntheticTestUtils.optionsFromString(
            "{\"outputRecordsPerInputRecord\": 2,"
                + " \"preservesInputKeyDistribution\": false,"
                + "\"keySizeBytes\": 10,"
                + "\"valueSizeBytes\": 20,"
                + "\"numHotKeys\": 3,"
                + "\"hotKeyFraction\": 0.3,"
                + "\"seed\": 123456}",
            SyntheticStep.Options.class);
    options.delayDistribution =
        SyntheticOptions.fromRealDistribution(new ConstantRealDistribution(10));

    PCollection<KV<byte[], byte[]>> result =
        p.apply(Create.of(ImmutableList.of(KV.of(intToByteArray(1), intToByteArray(11)))))
            .apply(ParDo.of(new SyntheticStep(options)));

    PAssert.that(result)
        .satisfies(
            (Iterable<KV<byte[], byte[]>> input) -> {
              int count = 0;
              for (KV<byte[], byte[]> elm : input) {
                count += 1;
                assertEquals(10, elm.getKey().length);
                assertEquals(20, elm.getValue().length);
              }
              assertEquals(2, count);
              return null;
            });
    p.run().waitUntilFinish();
  }
}
