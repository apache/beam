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

package com.google.cloud.dataflow.sdk.util;

import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.theInstance;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link FinishedTriggersBitSet}.
 */
@RunWith(JUnit4.class)
public class FinishedTriggersBitSetTest {
  /**
   * Tests that after a trigger is set to finished, it reads back as finished.
   */
  @Test
  public void testSetGet() {
    FinishedTriggersProperties.verifyGetAfterSet(FinishedTriggersBitSet.emptyWithCapacity(1));
  }

  /**
   * Tests that clearing a trigger recursively clears all of that triggers subTriggers, but no
   * others.
   */
  @Test
  public void testClearRecursively() {
    FinishedTriggersProperties.verifyClearRecursively(FinishedTriggersBitSet.emptyWithCapacity(1));
  }

  @Test
  public void testCopy() throws Exception {
    FinishedTriggersBitSet finishedSet = FinishedTriggersBitSet.emptyWithCapacity(10);
    assertThat(finishedSet.copy().getBitSet(), not(theInstance(finishedSet.getBitSet())));
  }
}
