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
package org.apache.beam.sdk.extensions.euphoria.core.testkit;

import static java.util.Arrays.asList;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.AssignEventTime;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.TopPerKey;
import org.apache.beam.sdk.extensions.euphoria.core.client.util.Triple;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Correctness tests of {@link TopPerKey}. */
@RunWith(JUnit4.class)
public class TopPerKeyTest extends AbstractOperatorTest {

  @Test
  public void testAllInOneWindow() {
    execute(
        new AbstractTestCase<Item, Triple<String, String, Integer>>() {

          @Override
          protected PCollection<Triple<String, String, Integer>> getOutput(
              PCollection<Item> input) {
            final PCollection<Item> timestampedElements =
                AssignEventTime.of(input).using(Item::getTimestamp).output();
            return TopPerKey.of(timestampedElements)
                .keyBy(Item::getKey)
                .valueBy(Item::getValue)
                .scoreBy(Item::getScore)
                .windowBy(FixedWindows.of(Duration.millis(10)))
                .triggeredBy(DefaultTrigger.of())
                .discardingFiredPanes()
                .withAllowedLateness(Duration.ZERO)
                .output();
          }

          @Override
          public List<Triple<String, String, Integer>> getUnorderedOutput() {
            return asList(
                Triple.of("one", "one-999", 999),
                Triple.of("two", "two", 10),
                Triple.of("three", "3-three", 2));
          }

          @Override
          protected List<Item> getInput() {
            return asList(
                new Item("one", "one-ZZZ-1", 1, 0L),
                new Item("one", "one-ZZZ-2", 2, 1L),
                new Item("one", "one-3", 3, 2L),
                new Item("one", "one-999", 999, 3L),
                new Item("two", "two", 10, 4L),
                new Item("three", "1-three", 1, 5L),
                new Item("three", "2-three", 0, 6L),
                new Item("one", "one-XXX-100", 100, 7L),
                new Item("three", "3-three", 2, 8L));
          }

          @Override
          protected TypeDescriptor<Item> getInputType() {
            return new TypeDescriptor<Item>() {};
          }
        });
  }

  @Test
  public void testTwoWindows() {
    execute(
        new AbstractTestCase<Item, Triple<String, String, Integer>>() {

          @Override
          protected PCollection<Triple<String, String, Integer>> getOutput(
              PCollection<Item> input) {
            final PCollection<Item> timestampedElements =
                AssignEventTime.of(input).using(Item::getTimestamp).output();
            return TopPerKey.of(timestampedElements)
                .keyBy(Item::getKey)
                .valueBy(Item::getValue)
                .scoreBy(Item::getScore)
                .windowBy(FixedWindows.of(Duration.millis(10)))
                .triggeredBy(DefaultTrigger.of())
                .discardingFiredPanes()
                .withAllowedLateness(Duration.ZERO)
                .output();
          }

          @Override
          public List<Triple<String, String, Integer>> getUnorderedOutput() {
            return asList(
                // first window
                Triple.of("one", "one-999", 999),
                Triple.of("two", "two", 10),
                Triple.of("three", "3-three", 2),
                // second window
                Triple.of("one", "one-XXX-100", 100),
                Triple.of("three", "2-three", 0));
          }

          @Override
          protected List<Item> getInput() {
            return asList(
                new Item("one", "one-ZZZ-1", 1, 14L),
                new Item("one", "one-ZZZ-2", 2, 1L),
                new Item("one", "one-3", 3, 13L),
                new Item("one", "one-999", 999, 3L),
                new Item("two", "two", 10, 4L),
                new Item("three", "1-three", 1, 5L),
                new Item("three", "2-three", 0, 16L),
                new Item("one", "one-XXX-100", 100, 12L),
                new Item("three", "3-three", 2, 8L));
          }

          @Override
          protected TypeDescriptor<Item> getInputType() {
            return new TypeDescriptor<Item>() {};
          }
        });
  }

  static final class Item implements Serializable {

    private final String key, value;
    private final int score;
    private final long timestamp;

    Item(String key, String value, int score, long timestamp) {
      this.key = key;
      this.value = value;
      this.score = score;
      this.timestamp = timestamp;
    }

    String getKey() {
      return key;
    }

    String getValue() {
      return value;
    }

    int getScore() {
      return score;
    }

    long getTimestamp() {
      return timestamp;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Item item = (Item) o;
      return score == item.score
          && timestamp == item.timestamp
          && Objects.equals(key, item.key)
          && Objects.equals(value, item.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, value, score, timestamp);
    }
  }
}
