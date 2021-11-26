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
package org.apache.beam.fn.harness.logging;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.model.Statement;

/** Tests for {@link RestoreBeamFnLoggingMDC}. */
@RunWith(JUnit4.class)
public class RestoreBeamFnLoggingMDCTest {
  @Rule public TestRule restoreMDCAfterTest = new RestoreBeamFnLoggingMDC();

  @Test
  public void testOldValuesAreRestored() throws Throwable {
    // We need our own instance here so that we don't squash any saved values.
    TestRule restoreMDC = new RestoreBeamFnLoggingMDC();

    final boolean[] evaluateRan = new boolean[1];
    BeamFnLoggingMDC.setInstructionId("oldInstruction");

    restoreMDC
        .apply(
            new Statement() {
              @Override
              public void evaluate() {
                evaluateRan[0] = true;
                // Ensure parameters are cleared before the test runs
                assertNull("null Instruction", BeamFnLoggingMDC.getInstructionId());

                // Simulate updating parameters for the test
                BeamFnLoggingMDC.setInstructionId("newInstruction");

                // Ensure that the values changed
                assertEquals("newInstruction", BeamFnLoggingMDC.getInstructionId());
              }
            },
            Description.EMPTY)
        .evaluate();

    // Validate that the statement ran and that the values were reverted
    assertTrue(evaluateRan[0]);
    assertEquals("oldInstruction", BeamFnLoggingMDC.getInstructionId());
  }
}
