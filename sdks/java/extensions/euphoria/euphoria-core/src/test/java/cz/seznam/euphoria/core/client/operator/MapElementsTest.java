package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import org.junit.Test;

import static org.junit.Assert.*;

public class MapElementsTest {

  @Test
  public void testBuild() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 1);

    Dataset<String> mapped = MapElements.named("Map1")
       .of(dataset)
       .using(s -> s)
       .output();

    assertEquals(flow, mapped.getFlow());
    assertEquals(1, flow.size());

    MapElements map = (MapElements) flow.operators().iterator().next();
    assertEquals(flow, map.getFlow());
    assertEquals("Map1", map.getName());
    assertNotNull(map.mapper);
    assertEquals(mapped, map.output());
  }

  @Test
  public void testBuild_ImplicitName() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 1);

    Dataset<String> mapped = MapElements.of(dataset)
            .using(s -> s)
            .output();

    MapElements map = (MapElements) flow.operators().iterator().next();
    assertEquals("Map", map.getName());
  }
}