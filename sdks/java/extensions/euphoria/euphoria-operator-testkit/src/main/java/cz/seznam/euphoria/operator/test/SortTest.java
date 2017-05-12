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
package cz.seznam.euphoria.operator.test;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.partitioning.RangePartitioning;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.operator.Sort;
import cz.seznam.euphoria.operator.test.junit.AbstractOperatorTest;
import cz.seznam.euphoria.operator.test.junit.Processing;
import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

@Processing(Processing.Type.ALL)
public class SortTest extends AbstractOperatorTest {

  static final class Item implements Serializable {
    private final long time;
    private final String key;
    private final int score;
    Item(String key, int score) {
      this(0L, key, score);
    }
    Item(long time, String key, int score) {
      this.time = time;
      this.key = key;
      this.score = score;
    }
    long getTime() { return time; }
    String getKey() { return key; }
    int getScore() { return score; }
    @Override
    public boolean equals(Object o) {
      if (o instanceof Item) {
        Item item = (Item) o;
        return time == item.time 
            && score == item.score 
            && Objects.equals(key, item.key);
        }
      return false;
    }
    @Override
    public int hashCode() { return Objects.hash(time, key, score); }
    @Override
    public String toString() {
      return "Item [" + time + ", " + key + ", " + score + "]";
    }
  }

  @Test
  public void testOnSinglePartition() {
    execute(new AbstractTestCase<Item, Item>() {
      @Override
      protected Dataset<Item>
      getOutput(Dataset<Item> input) {
        Dataset<Item> sorted = Sort.of(input)
            .by(Item::getScore)
            .setNumPartitions(1)
            .output();
        return sorted;
      }

      @Override
      public void validate(Partitions<Item> partitions) {
        Assert.assertEquals(1, partitions.size());
        List<Item> items = partitions.get(0);
        assertEquals(9, items.size());

        List<String> scores = items.stream()
            .map(Item::getKey)
            .collect(Collectors.toList());

        assertEquals(Arrays.asList(
            "1-three",
            "one-ZZZ-1",
            "one-ZZZ-2",
            "one-3",
            "two",
            "2-three",
            "3-three",
            "one-XXX-100",
            "one-999"), scores);
      }

      @Override
      protected Partitions<Item> getInput() {
        return Partitions
            .add(
                new Item("one-ZZZ-1", 1),
                new Item("one-ZZZ-2", 2),
                new Item("one-3", 3),
                new Item("one-999", 999),
                new Item("two", 10),
                new Item("1-three", 0),
                new Item("2-three", 11))
            .add(
                new Item("one-XXX-100", 100),
                new Item("3-three", 21))
            .build();
      }

      @Override
      public int getNumOutputPartitions() {
        return 1;
      }
    });
  }

  @Test
  public void testOnCustomPartitioner() {
    execute(new AbstractTestCase<Item, Item>() {
      @Override
      protected Dataset<Item>
      getOutput(Dataset<Item> input) {
        Dataset<Item> sorted = Sort.of(input)
            .by(Item::getScore)
            .setNumPartitions(2)
            .setPartitioner(i -> i < 20 ? 0 : 1)
            .output();
        return sorted;
      }

      @Override
      public void validate(Partitions<Item> partitions) {
        Assert.assertEquals(2, partitions.size());
        List<Item> lowItems = partitions.get(0);
        assertEquals(6, lowItems.size());
        List<String> lowScores = lowItems.stream()
            .map(Item::getKey)
            .collect(Collectors.toList());
        assertEquals(Arrays.asList(
            "1-three",
            "one-ZZZ-1",
            "one-ZZZ-2",
            "one-3",
            "two",
            "2-three"), lowScores);

        List<Item> highItems = partitions.get(1);
        assertEquals(3, highItems.size());
        List<String> highScores = highItems.stream()
            .map(Item::getKey)
            .collect(Collectors.toList());
        assertEquals(Arrays.asList(
            "3-three",
            "one-XXX-100",
            "one-999"), highScores);
      }

      @Override
      protected Partitions<Item> getInput() {
        return Partitions
            .add(
                new Item("one-ZZZ-1", 1),
                new Item("one-ZZZ-2", 2),
                new Item("one-3", 3),
                new Item("one-999", 999),
                new Item("two", 10),
                new Item("1-three", 0),
                new Item("2-three", 11))
            .add(
                new Item("one-XXX-100", 100),
                new Item("3-three", 21))
            .build();
      }

      @Override
      public int getNumOutputPartitions() {
        return 2;
      }
    });
  }

  @Test
  public void testOnWindowingWithCustomPartitioner() {
    execute(new AbstractTestCase<Item, Item>() {
      @Override
      protected Dataset<Item>
      getOutput(Dataset<Item> input) {
        Dataset<Item> sorted = Sort.of(input)
            .by(Item::getScore)
            .windowBy(Time.of(Duration.ofSeconds(2)), Item::getTime)
            .setPartitioning(new RangePartitioning<>(10, 20))
            .output();
        return sorted;
      }

      @Override
      public void validate(Partitions<Item> partitions) {
        Assert.assertEquals(3, partitions.size());

        // (0-10>
        List<Item> lowItems = partitions.get(0);
        assertEquals(6, lowItems.size());
        assertEquals(Arrays.asList("two"), between(lowItems, 0, 2000));
        assertEquals(Arrays.asList("1-three", "one-ZZZ-2", "one-3"), between(lowItems, 2000, 4000));
        assertEquals(Arrays.asList("one-ZZZ-1", "2-three"), between(lowItems, 4000, 6000));
        
        // (10-20>
        List<Item> midItems = partitions.get(1);
        assertEquals(1, midItems.size());
        assertEquals(Arrays.asList(), between(midItems, 0, 2000));
        assertEquals(Arrays.asList("4-four"), between(midItems, 2000, 4000));
        assertEquals(Arrays.asList(), between(midItems, 4000, 6000));
        
        // (20-MAX>
        List<Item> highItems = partitions.get(2);
        assertEquals(3, highItems.size());
        assertEquals(Arrays.asList("one-XXX-100", "one-999"), between(highItems, 0, 2000));
        assertEquals(Arrays.asList(), between(highItems, 2000, 4000));
        assertEquals(Arrays.asList("3-three"), between(highItems, 4000, 6000));
      }

      @Override
      protected Partitions<Item> getInput() {
        return Partitions
            .add(
                new Item(0, "two", 8),
                new Item(1000, "one-999", 999),
                new Item(2000, "one-3", 3),
                new Item(3000, "one-ZZZ-2", 2),
                new Item(3000, "1-three", 0),
                new Item(4000, "one-ZZZ-1", 1),
                new Item(4000, "2-three", 9))
            .add(
                new Item(1000, "one-XXX-100", 100),
                new Item(3000, "4-four", 11),
                new Item(5000, "3-three", 21))
            .build();
      }

      @Override
      public int getNumOutputPartitions() {
        return 3;
      }
    });
  }
  
  // take sequential sublist of items between lo inclusive and hi exclusive
  private static List<String> between(List<Item> items, long lo, long hi) {
    List<String> ret = new ArrayList<>();
    for (Item item: items) {
      if (item.getTime() >= lo && item.getTime() < hi) {
        ret.add(item.getKey());
      // already passed the interval
      } else if (!ret.isEmpty()) {
        break;
      }
    }
    return ret;
  }
}
