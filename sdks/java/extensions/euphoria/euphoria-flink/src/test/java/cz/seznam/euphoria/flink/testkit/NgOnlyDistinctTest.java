package cz.seznam.euphoria.flink.testkit;

import cz.seznam.euphoria.operator.test.ng.junit.ExecutorProviderRunner;
import cz.seznam.euphoria.operator.test.ng.tests.DistinctTest;
import org.junit.runner.RunWith;

@RunWith(ExecutorProviderRunner.class)
public class NgOnlyDistinctTest
    extends DistinctTest
    implements NgFlinkExecutorProvider {}
