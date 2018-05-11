/*
 * Copyright 2016-2018 Seznam.cz, a.s.
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
package cz.seznam.euphoria.core.client.dataset.windowing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Iterables;
import cz.seznam.euphoria.core.client.util.Pair;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import org.junit.Test;

public class SessionTest {

  @Test
  public void testWindowAssignment() {
    Session<?> windowing = Session.of(Duration.ofMillis(10));

    Iterable<TimeInterval> windows = windowing.assignWindowsToElement(new TimestampedElement<>(13));
    assertEquals(1, Iterables.size(windows));
    assertEquals(new TimeInterval(13, 23), Iterables.getOnlyElement(windows));
  }

  @Test
  public void testWindowMerging() {
    Session<?> windowing = Session.of(Duration.ofMillis(10));

    Collection<Pair<Collection<TimeInterval>, TimeInterval>> merged =
        windowing.mergeWindows(Arrays.asList(new TimeInterval(5, 15), new TimeInterval(12, 22)));

    assertEquals(1, merged.size());
    assertEquals(new TimeInterval(5, 22), Iterables.getOnlyElement(merged).getSecond());

    Collection<Pair<Collection<TimeInterval>, TimeInterval>> nonMerged =
        windowing.mergeWindows(Arrays.asList(new TimeInterval(5, 15), new TimeInterval(16, 22)));

    assertTrue(nonMerged.isEmpty());
  }
}
