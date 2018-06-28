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
package org.apache.beam.runners.core.triggers;

import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.theInstance;
import static org.junit.Assert.assertThat;

import java.util.HashSet;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link FinishedTriggersSet}. */
@RunWith(JUnit4.class)
public class FinishedTriggersSetTest {
  /** Tests that after a trigger is set to finished, it reads back as finished. */
  @Test
  public void testSetGet() {
    FinishedTriggersProperties.verifyGetAfterSet(FinishedTriggersSet.fromSet(new HashSet<>()));
  }

  /**
   * Tests that clearing a trigger recursively clears all of that triggers subTriggers, but no
   * others.
   */
  @Test
  public void testClearRecursively() {
    FinishedTriggersProperties.verifyClearRecursively(FinishedTriggersSet.fromSet(new HashSet<>()));
  }

  @Test
  public void testCopy() throws Exception {
    FinishedTriggersSet finishedSet = FinishedTriggersSet.fromSet(new HashSet<>());
    assertThat(
        finishedSet.copy().getFinishedTriggers(),
        not(theInstance(finishedSet.getFinishedTriggers())));
  }
}
