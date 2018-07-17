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
package org.apache.beam.sdk.transforms.windowing;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests the {@link AfterWatermark} triggers. */
@RunWith(JUnit4.class)
public class AfterWatermarkTest {

  @Test
  public void testFromEndOfWindowToString() {
    Trigger trigger = AfterWatermark.pastEndOfWindow();
    assertEquals("AfterWatermark.pastEndOfWindow()", trigger.toString());
  }

  @Test
  public void testEarlyFiringsToString() {
    Trigger trigger = AfterWatermark.pastEndOfWindow().withEarlyFirings(StubTrigger.named("t1"));

    assertEquals("AfterWatermark.pastEndOfWindow().withEarlyFirings(t1)", trigger.toString());
  }

  @Test
  public void testLateFiringsToString() {
    Trigger trigger = AfterWatermark.pastEndOfWindow().withLateFirings(StubTrigger.named("t1"));

    assertEquals("AfterWatermark.pastEndOfWindow().withLateFirings(t1)", trigger.toString());
  }

  @Test
  public void testEarlyAndLateFiringsToString() {
    Trigger trigger =
        AfterWatermark.pastEndOfWindow()
            .withEarlyFirings(StubTrigger.named("t1"))
            .withLateFirings(StubTrigger.named("t2"));

    assertEquals(
        "AfterWatermark.pastEndOfWindow().withEarlyFirings(t1).withLateFirings(t2)",
        trigger.toString());
  }

  @Test
  public void testToStringExcludesNeverTrigger() {
    Trigger trigger =
        AfterWatermark.pastEndOfWindow()
            .withEarlyFirings(Never.ever())
            .withLateFirings(Never.ever());

    assertEquals("AfterWatermark.pastEndOfWindow()", trigger.toString());
  }
}
