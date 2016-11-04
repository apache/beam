package cz.seznam.euphoria.spark;

import cz.seznam.euphoria.operator.test.AllOperatorTest;

public class SparkExecutorTestKitTest extends AllOperatorTest {

  public SparkExecutorTestKitTest() {
    super(settings -> new TestSparkExecutor());
  }

  // FIXME merging windows not supported yet - ignore following tests
  // these overridden methods will be removed

  @Override
  public void testReduceByKey() throws Exception {}

  @Override
  public void testReduceStateByKey() throws Exception {}
}