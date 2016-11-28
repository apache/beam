package cz.seznam.euphoria.operator.test;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.operator.TopPerKey;
import cz.seznam.euphoria.core.client.util.Triple;
import cz.seznam.euphoria.operator.test.junit.AbstractOperatorTest;
import cz.seznam.euphoria.operator.test.junit.Processing;
import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;
import java.util.Objects;

import static java.util.Arrays.asList;

@Processing(Processing.Type.ALL)
public class TopPerKeyTest extends AbstractOperatorTest {

  static final class Item implements Serializable {
    private final String key, value;
    private final int score;
    Item(String key, String value, int score) {
      this.key = key;
      this.value = value;
      this.score = score;
    }
    String getKey() { return key; }
    String getValue() { return value; }
    int getScore() { return score; }
    @Override
    public boolean equals(Object o) {
      if (o instanceof Item) {
        Item item = (Item) o;
        return score == item.score &&
            Objects.equals(key, item.key) &&
            Objects.equals(value, item.value);
      }
      return false;
    }
    @Override
    public int hashCode() { return Objects.hash(key, value, score); }
  }

  @Test
  public void testOnBatch() throws Exception {
    execute(new AbstractTestCase<Item, Triple<String, String, Integer>>() {
      @Override
      protected Dataset<Triple<String, String, Integer>>
      getOutput(Dataset<Item> input) {
        return TopPerKey.of(input)
            .keyBy(Item::getKey)
            .valueBy(Item::getValue)
            .scoreBy(Item::getScore)
            .setNumPartitions(1)
            .output();
      }

      @Override
      public void validate(Partitions<Triple<String, String, Integer>> partitions) {
        Assert.assertEquals(1, partitions.size());
        assertUnorderedEquals(
            asList(
                Triple.of("one", "one-999", 999),
                Triple.of("two", "two", 10),
                Triple.of("three", "3-three", 2)),
            partitions.get(0));
      }

      @Override
      protected Partitions<Item> getInput() {
        return Partitions
            .add(
                new Item("one", "one-ZZZ-1", 1),
                new Item("one", "one-ZZZ-2", 2),
                new Item("one", "one-3", 3),
                new Item("one", "one-999", 999),
                new Item("two", "two", 10),
                new Item("three", "1-three", 1),
                new Item("three", "2-three", 0))
            .add(
                new Item("one", "one-XXX-100", 100),
                new Item("three", "3-three", 2))
            .build();
      }

      @Override
      public int getNumOutputPartitions() {
        return 1;
      }
    });
  }
}
