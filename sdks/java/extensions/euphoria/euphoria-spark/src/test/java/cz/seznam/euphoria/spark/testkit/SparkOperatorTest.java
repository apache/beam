package cz.seznam.euphoria.spark.testkit;

import cz.seznam.euphoria.operator.test.junit.Processing;
import cz.seznam.euphoria.operator.test.junit.Processing.Type;
import cz.seznam.euphoria.operator.test.AllOperatorsSuite;
import cz.seznam.euphoria.operator.test.CountByKeyTest;
import cz.seznam.euphoria.operator.test.DistinctTest;
import cz.seznam.euphoria.operator.test.FilterTest;
import cz.seznam.euphoria.operator.test.FlatMapTest;
import cz.seznam.euphoria.operator.test.GroupByKeyTest;
import cz.seznam.euphoria.operator.test.JoinTest;
import cz.seznam.euphoria.operator.test.MapElementsTest;
import cz.seznam.euphoria.operator.test.RepartitionTest;
import cz.seznam.euphoria.operator.test.SumByKeyTest;
import cz.seznam.euphoria.operator.test.TopPerKeyTest;
import cz.seznam.euphoria.operator.test.UnionTest;
import org.junit.runners.Suite;

@Processing(Type.BOUNDED) // spark supports only bounded processing
// FIXME once all features are supported in the spark-executor, inherit this annotation from the super-class
@Suite.SuiteClasses({
    CountByKeyTest.class,
    DistinctTest.class,
    FilterTest.class,
    FlatMapTest.class,
    GroupByKeyTest.class,
    JoinTest.class,
    MapElementsTest.class,
//    ReduceByKeyTest.class,
//    ReduceStateByKeyTest.class,
    RepartitionTest.class,
    SumByKeyTest.class,
    TopPerKeyTest.class,
    UnionTest.class,
})
public class SparkOperatorTest
    extends AllOperatorsSuite
    implements SparkExecutorProvider {}
