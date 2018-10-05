/**
 * Copyright 2016-2017 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.inmem;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test vector clocks.
 */
public class VectorClockTest {

  @Test
  public void testUpdate() {
    VectorClock clock = new VectorClock(2);
    clock.update(1, 1);
    assertEquals(0, clock.getCurrent());
    clock.update(2, 0);
    assertEquals(1, clock.getCurrent());
    clock.update(3, 0);
    assertEquals(1, clock.getCurrent());
    clock.update(4, 1);
    assertEquals(3, clock.getCurrent());
  }

  // test that updates backwards in time have no effect
  @Test
  public void testTimeNonUniformity() {
    VectorClock clock = new VectorClock(2);
    clock.update(1, 1);
    clock.update(3, 0);
    clock.update(4, 1);
    clock.update(2, 1);
    assertEquals(3, clock.getCurrent());
  }

}