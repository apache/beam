package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.Collector;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class FlatMapTest {

  @Test
  public void testBuild() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 1);

    Dataset<String> mapped = FlatMap.named("FlatMap1")
       .of(dataset)
       .using((String s, Collector<String> c) -> c.collect(s))
       .output();

    assertEquals(flow, mapped.getFlow());
    assertEquals(1, flow.size());

    FlatMap map = (FlatMap) flow.operators().iterator().next();
    assertEquals(flow, map.getFlow());
    assertEquals("FlatMap1", map.getName());
    assertNotNull(map.getFunctor());
    assertEquals(mapped, map.output());
  }

  @Test
  public void testBuild_ImplicitName() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 1);

    Dataset<String> mapped = FlatMap.of(dataset)
            .using((String s, Collector<String> c) -> c.collect(s))
            .output();

    FlatMap map = (FlatMap) flow.operators().iterator().next();
    assertEquals("FlatMap", map.getName());
  }
}