package cz.seznam.euphoria.operator.test;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.TopPerKey;
import cz.seznam.euphoria.core.client.util.Triple;
import org.junit.Assert;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static java.util.Arrays.asList;

public class TopPerKeyTest extends OperatorTest {

  static final class Item implements Serializable {
    private String key, value;
    private int score;
    Item(String key, String value, int score) {
      this.key = key;
      this.value = value;
      this.score = score;
    }
    String getKey() { return key; }
    String getValue() { return value; }
    int getScore() { return score; }
    public boolean equals(Object o) {
      if (o instanceof Item) {
        Item item = (Item) o;
        return score == item.score &&
            Objects.equals(key, item.key) &&
            Objects.equals(value, item.value);
      }
      return false;
    }
    public int hashCode() { return Objects.hash(key, value, score); }
  }

  @Override
  protected List<TestCase> getTestCases() {
    return Collections.singletonList(testOnBatch());
  }

  TestCase<Triple<String, String, Integer>> testOnBatch() {
    return new AbstractTestCase<Item, Triple<String, String, Integer>>() {
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
      protected DataSource<Item> getDataSource() {
        return ListDataSource.bounded(
            asList(
                new Item("one", "one-ZZZ-1", 1),
                new Item("one", "one-ZZZ-2", 2),
                new Item("one", "one-3", 3),
                new Item("one", "one-999", 999),
                new Item("two", "two", 10),
                new Item("three", "1-three", 1),
                new Item("three", "2-three", 0)),
            asList(
                new Item("one", "one-XXX-100", 100),
                new Item("three", "3-three", 2)));
      }

      @Override
      public int getNumOutputPartitions() {
        return 1;
      }

      @Override
      public void validate(List<List<Triple<String, String, Integer>>> partitions) {
        Assert.assertEquals(1, partitions.size());
        assertUnorderedEquals(
            asList(
                Triple.of("one", "one-999", 999),
                Triple.of("two", "two", 10),
                Triple.of("three", "3-three", 2)),
            partitions.get(0));
      }
    };
  }
}
