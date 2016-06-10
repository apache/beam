
package cz.seznam.euphoria.operator.test;

import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.core.util.Settings;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test all operators.
 */
@Ignore
public abstract class AllOperatorTest {

  final UnaryFunction<Settings, Executor> executorFactory;
  final Settings settings;
  Executor executor;

  protected AllOperatorTest(UnaryFunction<Settings, Executor> executorFactory) {
    this(executorFactory, new Settings());
  }

  protected AllOperatorTest(
      UnaryFunction<Settings, Executor> executorFactory, Settings settings) {

    this.executorFactory = executorFactory;
    this.settings = settings;
  }

  @Before
  public void setup() {
    executor = executorFactory.apply(settings);
  }

  @Test
  public void testDistinct() {
    DistinctTest t = new DistinctTest();
    t.runTests(executor);
  }

  @Test
  public void testCountByKey() {
    CountByKeyTest t = new CountByKeyTest();
    t.runTests(executor);
  }

  @Test
  public void testFilter() {
    FilterTest t = new FilterTest();
    t.runTests(executor);
  }

  @Test
  public void testFlatMap() {
    FlatMapTest t = new FlatMapTest();
    t.runTests(executor);
  }

  @Test
  public void testMapElements() {
    MapElementsTest t = new MapElementsTest();
    t.runTests(executor);
  }

  @Test
  public void testRepartition() {
    RepartitionTest t = new RepartitionTest();
    t.runTests(executor);
  }

  @Test
  public void testSumByKey() {
    SumByKeyTest t = new SumByKeyTest();
    t.runTests(executor);
  }

  @Test
  public void testUnion() {
    UnionTest t = new UnionTest();
    t.runTests(executor);
  }

  @Test
  public void testJoin() {
    JoinTest t = new JoinTest();
    t.runTests(executor);
  }

  @Test
  public void testGroupBy() {
    GroupByKeyTest t = new GroupByKeyTest();
    t.runTests(executor);
  }

  @Test
  public void testReduceByKey() {
    ReduceByKeyTest t = new ReduceByKeyTest();
    t.runTests(executor);
  }

  @Test
  public void testReduceStateByKey() {
    ReduceStateByKeyTest t = new ReduceStateByKeyTest();
    t.runTests(executor);
  }

}
