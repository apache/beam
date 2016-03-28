/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.io;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests {@link MovingFunction}.
 */
@RunWith(JUnit4.class)
public class MovingFunctionTest {

  private static final long SAMPLE_PERIOD = 100;
  private static final long SAMPLE_UPDATE = 10;
  private static final int SIGNIFICANT_BUCKETS = 2;
  private static final int SIGNIFICANT_SAMPLES = 10;

  private MovingFunction newFunc() {
    return new
        MovingFunction(SAMPLE_PERIOD, SAMPLE_UPDATE, SIGNIFICANT_BUCKETS,
                       SIGNIFICANT_SAMPLES, SimpleFunction.SUM);

  }

  @Test
  public void significantSamples() {
    MovingFunction f = newFunc();
    assertFalse(f.isSignificant());
    for (int i = 0; i < SIGNIFICANT_SAMPLES - 1; i++) {
      f.add(0, 0);
      assertFalse(f.isSignificant());
    }
    f.add(0, 0);
    assertTrue(f.isSignificant());
  }

  @Test
  public void significantBuckets() {
    MovingFunction f = newFunc();
    assertFalse(f.isSignificant());
    f.add(0, 0);
    assertFalse(f.isSignificant());
    f.add(SAMPLE_UPDATE, 0);
    assertTrue(f.isSignificant());
  }

  @Test
  public void sum() {
    MovingFunction f = newFunc();
    for (int i = 0; i < SAMPLE_PERIOD; i++) {
      f.add(i, i);
      assertEquals(((i + 1) * i) / 2, f.get(i));
    }
  }

  @Test
  public void movingSum() {
    MovingFunction f = newFunc();
    int lost = 0;
    for (int i = 0; i < SAMPLE_PERIOD * 2; i++) {
      f.add(i , 1);
      if (i >= SAMPLE_PERIOD) {
        if (i % SAMPLE_UPDATE == 0) {
          lost += SAMPLE_UPDATE;
        }
      }
      assertEquals(i + 1 - lost, f.get(i));
    }
  }

  @Test
  public void jumpingSum() {
    MovingFunction f = newFunc();
    f.add(0, 1);
    f.add(SAMPLE_PERIOD -1, 1);
    assertEquals(2, f.get(SAMPLE_PERIOD -1));
    assertEquals(1, f.get(SAMPLE_PERIOD + 3 * SAMPLE_UPDATE));
    assertEquals(0, f.get(SAMPLE_PERIOD * 2));
  }
}
