package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import org.junit.Test;

import static org.junit.Assert.*;

public class FilterTest {

  @Test
  public void testBuild() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 1);

    Dataset<String> filtered = Filter.named("Filter1")
            .of(dataset)
            .by(s -> !s.equals(""))
            .output();

    assertEquals(flow, filtered.getFlow());
    assertEquals(1, flow.size());

    Filter filter = (Filter) flow.operators().iterator().next();
    assertEquals(flow, filter.getFlow());
    assertEquals("Filter1", filter.getName());
    assertNotNull(filter.predicate);
    assertEquals(filtered, filter.output());
  }

  @Test
  public void testBuild_ImplicitName() {
    Flow flow = Flow.create("TEST");
    Dataset<String> dataset = Util.createMockDataset(flow, 1);

    Dataset<String> filtered = Filter.of(dataset)
            .by(s -> !s.equals(""))
            .output();


    Filter filter = (Filter) flow.operators().iterator().next();
    assertEquals("Filter", filter.getName());
  }
}