package cz.seznam.euphoria.operator.test.flink;

import cz.seznam.euphoria.flink.TestFlinkExecutor;
import cz.seznam.euphoria.operator.test.AllOperatorTest;
import org.junit.Ignore;

public class FlinkOperatorTest extends AllOperatorTest {

  public FlinkOperatorTest() {
    super(s -> new TestFlinkExecutor());
  }

  // TODO
  // ignore tests using windowing operators

  @Override
  @Ignore
  public void testDistinct() throws Exception {
    // TODO
  }

  @Override
  @Ignore
  public void testCountByKey() throws Exception {
    // TODO
  }

  @Override
  public void testSumByKey() throws Exception {
    // TODO
  }

  @Override
  public void testJoin() throws Exception {
    // TODO
  }

  @Override
  public void testGroupBy() throws Exception {
    // TODO
  }

  @Override
  public void testReduceByKey() throws Exception {
    // TODO
  }

  @Override
  public void testReduceStateByKey() throws Exception {
    // TODO
  }
}
