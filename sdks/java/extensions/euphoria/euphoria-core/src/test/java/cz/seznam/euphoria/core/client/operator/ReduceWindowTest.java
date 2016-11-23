
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.windowing.Time;
import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import java.time.Duration;
import java.util.Arrays;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test behavior of operator {@code ReduceWindow}.
 */
public class ReduceWindowTest {

  @Test
  @SuppressWarnings("unchecked")
  public void testSimpleBuild() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);

    Dataset<Long> output = ReduceWindow.of(dataset)
        .valueBy(e -> "")
        .reduceBy(e -> 1L)
        .applyIf(false, b -> b.setNumPartitions(1))
        .output();

    ReduceWindow<String, String, Long, ?> producer;
    producer = (ReduceWindow<String, String, Long, ?>) output.getProducer();
    assertEquals(1L, (long) producer.getReducer().apply(Arrays.asList("blah")));
    assertEquals(2, producer.partitioning.getNumPartitions());
    assertEquals("", producer.valueExtractor.apply("blah"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSimpleBuildWithoutValue() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 2);
    Windowing<String, ?> windowing = Time.of(Duration.ofHours(1));

    Dataset<Long> output = ReduceWindow.of(dataset)
        .reduceBy(e -> 1L)
        .windowBy(windowing, s -> 0L)
        .applyIf(true, b -> b.setNumPartitions(1))
        .output();

    ReduceWindow<String, String, Long, ?> producer;
    producer = (ReduceWindow<String, String, Long, ?>) output.getProducer();
    assertEquals(1L, (long) producer.getReducer().apply(Arrays.asList("blah")));
    assertEquals(1, producer.partitioning.getNumPartitions());
    assertEquals("blah", producer.valueExtractor.apply("blah"));
    assertEquals(windowing, producer.windowing);
    assertNotNull(producer.getEventTimeAssigner());
  }


}
