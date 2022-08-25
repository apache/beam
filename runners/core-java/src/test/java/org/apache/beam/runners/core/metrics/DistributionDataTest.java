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
package org.apache.beam.runners.core.metrics;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DistributionData}. */
@RunWith(JUnit4.class)
public class DistributionDataTest {
  @Test
  public void testSingleton() {
    DistributionData data = DistributionData.singleton(5);
    assertEquals(5, data.sum());
    assertEquals(1, data.count());
    assertEquals(5, data.min());
    assertEquals(5, data.max());
  }

  @Test
  public void testCreate() {
    DistributionData data = DistributionData.create(5, 2, 1, 4);
    assertEquals(5, data.sum());
    assertEquals(2, data.count());
    assertEquals(1, data.min());
    assertEquals(4, data.max());
  }

  @Test
  public void testCombine() {
    DistributionData data = DistributionData.create(5, 2, 1, 4).combine(7);
    assertEquals(12, data.sum());
    assertEquals(3, data.count());
    assertEquals(1, data.min());
    assertEquals(7, data.max());

    data = DistributionData.create(5, 2, 1, 4).combine(1, 2, 0, 1);
    assertEquals(6, data.sum());
    assertEquals(4, data.count());
    assertEquals(0, data.min());
    assertEquals(4, data.max());

    data = DistributionData.create(5, 2, 1, 4).combine(DistributionData.EMPTY);
    assertEquals(5, data.sum());
    assertEquals(2, data.count());
    assertEquals(1, data.min());
    assertEquals(4, data.max());
  }
}
