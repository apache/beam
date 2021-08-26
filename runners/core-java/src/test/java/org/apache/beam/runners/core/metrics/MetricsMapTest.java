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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link MetricsMap}. */
@RunWith(JUnit4.class)
public class MetricsMapTest {

  public MetricsMap<String, AtomicLong> metricsMap =
      new MetricsMap<>(unusedKey -> new AtomicLong());

  @Test
  public void testCreateSeparateInstances() {
    AtomicLong foo = metricsMap.get("foo");
    AtomicLong bar = metricsMap.get("bar");

    assertThat(foo, not(sameInstance(bar)));
  }

  @Test
  public void testReuseInstances() {
    AtomicLong foo1 = metricsMap.get("foo");
    AtomicLong foo2 = metricsMap.get("foo");

    assertThat(foo1, sameInstance(foo2));
  }

  @Test
  public void testGet() {
    assertThat(metricsMap.tryGet("foo"), nullValue(AtomicLong.class));

    AtomicLong foo = metricsMap.get("foo");
    assertThat(metricsMap.tryGet("foo"), sameInstance(foo));
  }

  @Test
  public void testGetEntries() {
    AtomicLong foo = metricsMap.get("foo");
    AtomicLong bar = metricsMap.get("bar");
    assertThat(
        metricsMap.entries(), containsInAnyOrder(hasEntry("foo", foo), hasEntry("bar", bar)));
  }

  @Test
  public void testEquals() {
    MetricsMap<String, AtomicLong> metricsMap = new MetricsMap<>(unusedKey -> new AtomicLong());
    MetricsMap<String, AtomicLong> equal = new MetricsMap<>(unusedKey -> new AtomicLong());
    Assert.assertEquals(metricsMap, equal);
    Assert.assertEquals(metricsMap.hashCode(), equal.hashCode());
  }

  @Test
  public void testNotEquals() {
    MetricsMap<String, AtomicLong> metricsMap = new MetricsMap<>(unusedKey -> new AtomicLong());

    Assert.assertNotEquals(metricsMap, new Object());

    MetricsMap<String, AtomicLong> differentMetrics =
        new MetricsMap<>(unusedKey -> new AtomicLong());
    differentMetrics.get("key");
    Assert.assertNotEquals(metricsMap, differentMetrics);
    Assert.assertNotEquals(metricsMap.hashCode(), differentMetrics.hashCode());
  }

  private static Matcher<Map.Entry<String, AtomicLong>> hasEntry(
      final String key, final AtomicLong value) {
    return new TypeSafeMatcher<Entry<String, AtomicLong>>() {

      @Override
      public void describeTo(Description description) {
        description
            .appendText("Map.Entry{key=")
            .appendValue(key)
            .appendText(", value=")
            .appendValue(value)
            .appendText("}");
      }

      @Override
      protected boolean matchesSafely(Entry<String, AtomicLong> item) {
        return Objects.equals(key, item.getKey()) && Objects.equals(value, item.getValue());
      }
    };
  }
}
