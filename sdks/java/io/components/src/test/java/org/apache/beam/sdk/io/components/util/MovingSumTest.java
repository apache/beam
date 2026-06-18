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
package org.apache.beam.sdk.io.components.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class MovingSumTest {

  @Test
  public void testMovingSumBasic() {
    MovingSum movingSum = new MovingSum(10000, 1000);
    assertFalse(movingSum.hasData(1000));

    movingSum.add(1000, 5);
    assertTrue(movingSum.hasData(1000));
    assertEquals(5, movingSum.sum(1000));
    assertEquals(1, movingSum.count(1000));

    movingSum.add(1500, 10);
    assertEquals(15, movingSum.sum(1500));
    assertEquals(2, movingSum.count(1500));

    // Advance by 2 buckets (from 1000 to 3000)
    assertEquals(15, movingSum.sum(3000));
    movingSum.add(3500, 20);
    assertEquals(35, movingSum.sum(3500));
    assertEquals(3, movingSum.count(3500));

    // Wait 11 seconds (moving completely outside window)
    assertEquals(0, movingSum.sum(12000));
    assertEquals(0, movingSum.count(12000));
    assertFalse(movingSum.hasData(12000));
  }

  @Test
  public void testInvalidArguments() {
    assertThrows(IllegalArgumentException.class, () -> new MovingSum(100, 1000));
    assertThrows(IllegalArgumentException.class, () -> new MovingSum(1000, 0));
  }
}
