package cz.seznam.euphoria.operator.test;

import cz.seznam.euphoria.operator.test.junit.ExecutorProvider;
import cz.seznam.euphoria.operator.test.junit.ExecutorProviderRunner;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Subclass this class to have the whole suite operate on your executor of choice.
 */
@RunWith(ExecutorProviderRunner.class)
@Suite.SuiteClasses({
    CountByKeyTest.class,
    DistinctTest.class,
    FilterTest.class,
    FlatMapTest.class,
    JoinTest.class,
    JoinWindowEnforcementTest.class,
    MapElementsTest.class,
    ReduceByKeyTest.class,
    ReduceStateByKeyTest.class,
    RepartitionTest.class,
    SumByKeyTest.class,
    TopPerKeyTest.class,
    UnionTest.class,
})
public abstract class AllOperatorsSuite implements ExecutorProvider {

}
