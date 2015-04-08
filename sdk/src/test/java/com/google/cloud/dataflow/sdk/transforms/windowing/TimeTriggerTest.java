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

package com.google.cloud.dataflow.sdk.transforms.windowing;

import static org.junit.Assert.assertEquals;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for the various time operations in {@link TimeTrigger}.
 */
@RunWith(JUnit4.class)
public class TimeTriggerTest {

  @Test
  public void testAlignTo() {
    assertEquals(new Instant(100),
        TimeTrigger.alignedTo(new Instant(100), new Duration(10), new Instant(0)));
    assertEquals(new Instant(110),
        TimeTrigger.alignedTo(new Instant(105), new Duration(10), new Instant(0)));
    assertEquals(new Instant(105),
        TimeTrigger.alignedTo(new Instant(105), new Duration(10), new Instant(5)));
    assertEquals(new Instant(115),
        TimeTrigger.alignedTo(new Instant(110), new Duration(10), new Instant(5)));
  }

}
