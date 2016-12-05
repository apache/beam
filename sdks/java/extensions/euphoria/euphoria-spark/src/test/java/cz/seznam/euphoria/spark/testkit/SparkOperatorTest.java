package cz.seznam.euphoria.spark.testkit;

import cz.seznam.euphoria.operator.test.AllOperatorsSuite;
import cz.seznam.euphoria.operator.test.junit.Processing;
import cz.seznam.euphoria.operator.test.junit.Processing.Type;

@Processing(Type.BOUNDED) // spark supports only bounded processing
public class SparkOperatorTest
    extends AllOperatorsSuite
    implements SparkExecutorProvider {}
