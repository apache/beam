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

package org.apache.beam.sdk.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.transforms.Combine;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests {@link BucketingFunction}.
 */
@RunWith(JUnit4.class)
public class BucketingFunctionTest {

  private static final int BUCKET_WIDTH = 10;
  private static final int SIGNIFICANT_BUCKETS = 2;
  private static final int SIGNIFICANT_SAMPLES = 10;

  private static final Combine.BinaryCombineLongFn SUM =
      new Combine.BinaryCombineLongFn() {
        @Override
        public long apply(long left, long right) {
          return left + right;
        }

        @Override
        public long identity() {
          return 0;
        }
      };

  private BucketingFunction newFunc() {
    return new
        BucketingFunction(BUCKET_WIDTH, SIGNIFICANT_BUCKETS,
                          SIGNIFICANT_SAMPLES, SUM);
  }

  @Test
  public void significantSamples() {
    BucketingFunction f = newFunc();
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
    BucketingFunction f = newFunc();
    assertFalse(f.isSignificant());
    f.add(0, 0);
    assertFalse(f.isSignificant());
    f.add(BUCKET_WIDTH, 0);
    assertTrue(f.isSignificant());
  }

  @Test
  public void sum() {
    BucketingFunction f = newFunc();
    for (int i = 0; i < 100; i++) {
      f.add(i, i);
      assertEquals(((i + 1) * i) / 2, f.get());
    }
  }

  @Test
  public void movingSum() {
    BucketingFunction f = newFunc();
    int lost = 0;
    for (int i = 0; i < 200; i++) {
      f.add(i, 1);
      if (i >= 100) {
        f.remove(i - 100);
        if (i % BUCKET_WIDTH == BUCKET_WIDTH - 1) {
          lost += BUCKET_WIDTH;
        }
      }
      assertEquals(i + 1 - lost, f.get());
    }
  }
}
