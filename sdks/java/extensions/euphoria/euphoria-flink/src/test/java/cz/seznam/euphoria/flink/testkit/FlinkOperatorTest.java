package cz.seznam.euphoria.flink.testkit;

import cz.seznam.euphoria.flink.TestFlinkExecutor;
import cz.seznam.euphoria.operator.test.AllOperatorTest;

public class FlinkOperatorTest extends AllOperatorTest {

  public FlinkOperatorTest() {
    super(s -> new TestFlinkExecutor());
  }

  // TODO
  // ignore tests using windowing operators

  @Override
  public void testJoin() throws Exception {
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
