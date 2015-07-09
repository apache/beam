/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.testing;

import static org.junit.Assert.assertEquals;

import com.google.cloud.dataflow.sdk.runners.worker.logging.DataflowWorkerLoggingMDC;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link RestoreDataflowLoggingMDC}. */
@RunWith(JUnit4.class)
public class RestoreDataflowLoggingMDCTest {
  @Rule public TestRule restoreMDC = new RestoreDataflowLoggingMDC();

  /**
   * Asserts that all fields of <tt>DataflowWorkerLoggingMDC</tt> field are null, except a single
   * index with the expected value.
   */
  void assertOneNonNullThreadStatic(int idx, String expected) {
    String[] strs =
        new String[] {
          DataflowWorkerLoggingMDC.getJobId(),
          DataflowWorkerLoggingMDC.getStageName(),
          DataflowWorkerLoggingMDC.getStepName(),
          DataflowWorkerLoggingMDC.getWorkerId(),
          DataflowWorkerLoggingMDC.getWorkId()
        };
    for (int i = 0; i < strs.length; i++) {
      assertEquals(i == idx ? expected : null, strs[i]);
    }
  }

  @Test
  public void testLoggingParamsClearedA() {
    DataflowWorkerLoggingMDC.setJobId("job");
    assertOneNonNullThreadStatic(0, "job");
  }

  @Test
  public void testLoggingParamsClearedB() {
    DataflowWorkerLoggingMDC.setStageName("stage");
    assertOneNonNullThreadStatic(1, "stage");
  }

  @Test
  public void testLoggingParamsClearedC() {
    DataflowWorkerLoggingMDC.setStepName("step");
    assertOneNonNullThreadStatic(2, "step");
  }

  @Test
  public void testLoggingParamsClearedD() {
    DataflowWorkerLoggingMDC.setWorkerId("worker");
    assertOneNonNullThreadStatic(3, "worker");
  }

  @Test
  public void testLoggingParamsClearedE() {
    DataflowWorkerLoggingMDC.setWorkId("work");
    assertOneNonNullThreadStatic(4, "work");
  }
}
