package cz.seznam.euphoria.spark;

import cz.seznam.euphoria.operator.test.ng.junit.ExecutorProviderRunner;
import cz.seznam.euphoria.operator.test.ng.junit.Processing;
import cz.seznam.euphoria.operator.test.ng.junit.Processing.Type;
import cz.seznam.euphoria.operator.test.ng.tests.FilterTest;
import org.junit.runner.RunWith;

@RunWith(ExecutorProviderRunner.class)
@Processing(Type.BOUNDED) // spark supports only bounded processing
public class NgOnlyFilterTest
    extends FilterTest
    implements NgSparkExecutorProvider {}
