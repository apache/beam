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
package cz.seznam.euphoria.operator.test;

import static java.util.Arrays.asList;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.operator.TopPerKey;
import cz.seznam.euphoria.core.client.util.Triple;
import cz.seznam.euphoria.operator.test.junit.AbstractOperatorTest;
import cz.seznam.euphoria.operator.test.junit.Processing;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import org.junit.Test;

/**
 * TODO: add javadoc.
 */
@Processing(Processing.Type.ALL)
public class TopPerKeyTest extends AbstractOperatorTest {

  @Test
  public void testOnBatch() {
    execute(
        new AbstractTestCase<Item, Triple<String, String, Integer>>() {
          @Override
          protected Dataset<Triple<String, String, Integer>> getOutput(Dataset<Item> input) {
            return TopPerKey.of(input)
                .keyBy(Item::getKey)
                .valueBy(Item::getValue)
                .scoreBy(Item::getScore)
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
                new Item("one", "one-ZZZ-1", 1),
                new Item("one", "one-ZZZ-2", 2),
                new Item("one", "one-3", 3),
                new Item("one", "one-999", 999),
                new Item("two", "two", 10),
                new Item("three", "1-three", 1),
                new Item("three", "2-three", 0),
                new Item("one", "one-XXX-100", 100),
                new Item("three", "3-three", 2));
          }
        });
  }

  static final class Item implements Serializable {
    private final String key, value;
    private final int score;

    Item(String key, String value, int score) {
      this.key = key;
      this.value = value;
      this.score = score;
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

    @Override
    public boolean equals(Object o) {
      if (o instanceof Item) {
        Item item = (Item) o;
        return score == item.score
            && Objects.equals(key, item.key)
            && Objects.equals(value, item.value);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, value, score);
    }
  }
}
