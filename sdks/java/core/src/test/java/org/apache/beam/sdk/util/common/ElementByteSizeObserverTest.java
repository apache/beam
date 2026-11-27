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
package org.apache.beam.sdk.util.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Observable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link ElementByteSizeObserver}. */
@RunWith(JUnit4.class)
public class ElementByteSizeObserverTest {

  private static class RecordingByteSizeObserver extends ElementByteSizeObserver {
    final List<Long> reported = new ArrayList<>();

    @Override
    protected void reportElementSize(long elementByteSize) {
      reported.add(elementByteSize);
    }
  }

  @Test
  public void testUpdateLongAndBoxedWithAndWithoutScaling() {
    RecordingByteSizeObserver observer = new RecordingByteSizeObserver();

    observer.update(10L);
    observer.update(Integer.valueOf(5));
    observer.update(Long.valueOf(7L));
    observer.update((Observable) null, Long.valueOf(3L));
    observer.advance();

    assertEquals(1, observer.reported.size());
    assertEquals(25L, observer.reported.get(0).longValue());

    observer.setScalingFactor(2.5);
    observer.update(4L);
    observer.update(Integer.valueOf(2));
    observer.advance();

    assertEquals(2, observer.reported.size());
    assertEquals(15L, observer.reported.get(1).longValue());
  }

  @Test
  public void testUpdateUnexpectedTypeThrowsAssertionError() {
    RecordingByteSizeObserver observer = new RecordingByteSizeObserver();
    assertThrows(AssertionError.class, () -> observer.update("not-a-number"));
  }

  @Test
  public void testResetClearsAccumulatedStateWithoutReporting() {
    RecordingByteSizeObserver observer = new RecordingByteSizeObserver();
    observer.setScalingFactor(3.0);
    observer.setLazy();
    observer.update(10L);
    assertTrue(observer.getIsLazy());

    observer.reset();

    assertFalse(observer.getIsLazy());
    assertTrue(observer.reported.isEmpty());

    observer.update(7L);
    observer.advance();
    assertEquals(1, observer.reported.size());
    assertEquals(7L, observer.reported.get(0).longValue());
  }
}
