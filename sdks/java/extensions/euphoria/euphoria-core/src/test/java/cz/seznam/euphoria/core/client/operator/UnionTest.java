package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import org.junit.Test;

import static org.junit.Assert.*;

public class UnionTest {
  @Test
  public void testBuild() {
    Flow flow = Flow.create("TEST");
    Dataset<String> left = Util.createMockDataset(flow, 2);
    Dataset<String> right = Util.createMockDataset(flow, 3);

    Dataset<String> unioned = Union.named("Union1")
            .of(left, right)
            .output();

    assertEquals(flow, unioned.getFlow());
    assertEquals(1, flow.size());

    Union union = (Union) flow.operators().iterator().next();
    assertEquals(flow, union.getFlow());
    assertEquals("Union1", union.getName());
    assertEquals(unioned, union.output());
  }

  @Test
  public void testBuild_ImplicitName() {
    Flow flow = Flow.create("TEST");
    Dataset<String> left = Util.createMockDataset(flow, 2);
    Dataset<String> right = Util.createMockDataset(flow, 3);

    Dataset<String> unioned = Union.of(left, right).output();

    Union union = (Union) flow.operators().iterator().next();
    assertEquals("Union", union.getName());
  }
}