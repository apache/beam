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
package org.apache.beam.fn.harness.data;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.testng.AssertJUnit.assertEquals;

import org.apache.beam.sdk.coders.Coder;
import org.junit.Test;

/** Tests for {@link ShouldSample}. */
public class ShouldSampleTest {

  @Test()
  public void testShouldSampleAlwaysReturnsTrueIfCheap() throws Exception {
    Coder<String> elementCoder = (Coder<String>) mock(Coder.class);
    ShouldSample<String> testObject = new ShouldSample<String>(elementCoder);
    String testInput = "testInput";
    when(elementCoder.isRegisterByteSizeObserverCheap(testInput)).thenReturn(true);

    for (int i = 0; i < 1000; i++) {
      assertTrue(testObject.shouldSampleElement(testInput));
    }
  }

  @Test()
  public void testShouldSampleReturnsFalseAndTrueIfExpensive() throws Exception {
    Coder<String> elementCoder = (Coder<String>) mock(Coder.class);
    ShouldSample<String> testObject = new ShouldSample<String>(elementCoder);
    String testInput = "testInput";
    when(elementCoder.isRegisterByteSizeObserverCheap(testInput)).thenReturn(false);

    int numCalls = 1000;
    int falseCount = 0;
    for (int i = 0; i < numCalls; i++) {
      boolean sample = testObject.shouldSampleElement(testInput);
      if (!sample) {
        falseCount += 1;
      }
    }
    assertThat(falseCount, greaterThan(0));
    assertThat(falseCount, lessThan(numCalls));
  }

  @Test()
  public void testShouldSampleReturnsTrueForFirstSamplingCutoffCalls() throws Exception {
    // Rerun the test 100 times, since we are testing a random behaviour with a small
    // sample size = ShouldSample.SAMPLING_CUTOFF.
    for (int j = 0; j < 100; j++) {
      Coder<String> elementCoder = (Coder<String>) mock(Coder.class);
      ShouldSample<String> testObject = new ShouldSample<String>(elementCoder);
      String testInput = "testInput";
      when(elementCoder.isRegisterByteSizeObserverCheap(testInput)).thenReturn(false);

      int trueCount = 0;
      for (int i = 0; i < ShouldSample.SAMPLING_CUTOFF; i++) {
        boolean sample = testObject.shouldSampleElement(testInput);
        if (sample) {
          trueCount += 1;
        }
      }
      assertEquals(ShouldSample.SAMPLING_CUTOFF, trueCount);
    }
  }
}
