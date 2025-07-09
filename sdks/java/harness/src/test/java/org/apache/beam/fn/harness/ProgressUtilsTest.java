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
package org.apache.beam.fn.harness;

import static org.junit.Assert.assertEquals;

import java.io.Serializable;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.Progress;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ProgressUtils}. */
@RunWith(JUnit4.class)
public class ProgressUtilsTest implements Serializable {
  @Test
  public void testScaledProgress() throws Exception {
    Progress elementProgress = Progress.from(2, 8);
    // There is only one window.
    Progress scaledResult = ProgressUtils.scaleProgress(elementProgress, 0, 1);
    assertEquals(2, scaledResult.getWorkCompleted(), 0.0);
    assertEquals(8, scaledResult.getWorkRemaining(), 0.0);

    // We are at the first window of 3 in total.
    scaledResult = ProgressUtils.scaleProgress(elementProgress, 0, 3);
    assertEquals(2, scaledResult.getWorkCompleted(), 0.0);
    assertEquals(28, scaledResult.getWorkRemaining(), 0.0);

    // We are at the second window of 3 in total.
    scaledResult = ProgressUtils.scaleProgress(elementProgress, 1, 3);
    assertEquals(12, scaledResult.getWorkCompleted(), 0.0);
    assertEquals(18, scaledResult.getWorkRemaining(), 0.0);

    // We are at the last window of 3 in total.
    scaledResult = ProgressUtils.scaleProgress(elementProgress, 2, 3);
    assertEquals(22, scaledResult.getWorkCompleted(), 0.0);
    assertEquals(8, scaledResult.getWorkRemaining(), 0.0);
  }
}
