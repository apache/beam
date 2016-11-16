package cz.seznam.euphoria.flink.testkit;

import cz.seznam.euphoria.operator.test.ng.junit.ExecutorProviderRunner;
import cz.seznam.euphoria.operator.test.ng.tests.FilterTest;
import org.junit.runner.RunWith;

@RunWith(ExecutorProviderRunner.class)
public class NgOnlyFilterTest
    extends FilterTest
    implements NgFlinkExecutorProvider {}
