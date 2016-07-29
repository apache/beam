
package cz.seznam.euphoria.operator.test;

import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.core.executor.inmem.InMemFileSystem;
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

    settings.setClass("euphoria.io.datasink.factory.inmem",
            InMemFileSystem.SinkFactory.class);

    InMemFileSystem.get().reset();
  }

  @Test
  public void testDistinct() throws Exception {
    DistinctTest t = new DistinctTest();
    t.runTests(executor, settings);
  }

  @Test
  public void testCountByKey() throws Exception {
    CountByKeyTest t = new CountByKeyTest();
    t.runTests(executor, settings);
  }

  @Test
  public void testFilter() throws Exception {
    FilterTest t = new FilterTest();
    t.runTests(executor, settings);
  }

  @Test
  public void testFlatMap() throws Exception {
    FlatMapTest t = new FlatMapTest();
    t.runTests(executor, settings);
  }

  @Test
  public void testMapElements() throws Exception {
    MapElementsTest t = new MapElementsTest();
    t.runTests(executor, settings);
  }

  @Test
  public void testRepartition() throws Exception {
    RepartitionTest t = new RepartitionTest();
    t.runTests(executor, settings);
  }

  @Test
  public void testSumByKey() throws Exception {
    SumByKeyTest t = new SumByKeyTest();
    t.runTests(executor, settings);
  }

  @Test
  public void testUnion() throws Exception {
    UnionTest t = new UnionTest();
    t.runTests(executor, settings);
  }

  @Test
  public void testJoin() throws Exception {
    JoinTest t = new JoinTest();
    t.runTests(executor, settings);
  }

  @Test
  public void testGroupBy() throws Exception {
    GroupByKeyTest t = new GroupByKeyTest();
    t.runTests(executor, settings);
  }

  @Test
  public void testReduceByKey() throws Exception {
    ReduceByKeyTest t = new ReduceByKeyTest();
    t.runTests(executor, settings);
  }

  @Test
  public void testReduceStateByKey() throws Exception {
    ReduceStateByKeyTest t = new ReduceStateByKeyTest();
    t.runTests(executor, settings);
  }

}
