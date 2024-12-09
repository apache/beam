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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;

public class RestrictionInterrupterTest {

  @Test
  public void testTryInterrupt() {
    RestrictionInterrupter<Integer> interrupter =
        new RestrictionInterrupter<Integer>(
            () -> Instant.ofEpochSecond(0), Duration.standardSeconds(30));
    interrupter.setTimeSupplier(() -> Instant.ofEpochSecond(10));
    assertFalse(interrupter.tryInterrupt(1));
    interrupter.setTimeSupplier(() -> Instant.ofEpochSecond(15));
    assertFalse(interrupter.tryInterrupt(2));
    interrupter.setTimeSupplier(() -> Instant.ofEpochSecond(30));
    assertFalse(interrupter.tryInterrupt(3));
    interrupter.setTimeSupplier(() -> Instant.ofEpochSecond(40));
    assertFalse(interrupter.tryInterrupt(3));
    assertTrue(interrupter.tryInterrupt(4));
    assertTrue(interrupter.tryInterrupt(5));
    interrupter.setTimeSupplier(() -> Instant.ofEpochSecond(50));
    assertTrue(interrupter.tryInterrupt(5));
  }

  @Test
  public void testTryInterruptNoPreviousPosition() {
    RestrictionInterrupter<Integer> interrupter =
        new RestrictionInterrupter<Integer>(
            () -> Instant.ofEpochSecond(0), Duration.standardSeconds(30));
    interrupter.setTimeSupplier(() -> Instant.ofEpochSecond(40));
    assertFalse(interrupter.tryInterrupt(1));
    assertFalse(interrupter.tryInterrupt(1));
    assertTrue(interrupter.tryInterrupt(2));
    interrupter.setTimeSupplier(() -> Instant.ofEpochSecond(50));
    assertTrue(interrupter.tryInterrupt(3));
  }
}
