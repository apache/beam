package cz.seznam.euphoria.operator.test.ng.tests;

import cz.seznam.euphoria.operator.test.ng.junit.ExecutorProvider;
import cz.seznam.euphoria.operator.test.ng.junit.ExecutorProviderRunner;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(ExecutorProviderRunner.class)
@Suite.SuiteClasses({
    CountByKeyTest.class,
    DistinctTest.class,
    FilterTest.class,
    // ...
})
/**
 * Subclass this class to have the whole suite operate on your executor of choice.
 */
public abstract class AllOperatorsSuite implements ExecutorProvider {

}
