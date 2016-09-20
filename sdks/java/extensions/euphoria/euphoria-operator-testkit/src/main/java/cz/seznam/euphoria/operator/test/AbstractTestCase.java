
package cz.seznam.euphoria.operator.test;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.util.Settings;

/**
 * Abstract {@code TestCase} to be extended by test classes.
 */
public abstract class AbstractTestCase<I, O> implements OperatorTest.TestCase<O> {
  
  final protected Flow flow;
  final protected Settings settings;
  
  private static String getCallerName() {
    StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
    if (stackTrace.length > 4) {
      return stackTrace[4].getMethodName();
    }
    return "UNKNOWN";
  }

  protected AbstractTestCase() {
    this(getCallerName());
  }

  protected AbstractTestCase(Settings settings) {
    this(getCallerName(), settings);
  }

  protected AbstractTestCase(String name) {
    this(name, new Settings());
  }

  protected AbstractTestCase(String name, Settings settings) {
    this.flow = Flow.create(name, settings);
    this.settings = settings;
  }

  @Override
  @SuppressWarnings("unchecked")
  public final Dataset<O> getOutput(Flow flow) {
    Dataset<I> input = flow.createInput(getDataSource());
    Dataset<O> output = getOutput(input);
    return output;
  }

  protected abstract Dataset<O> getOutput(Dataset<I> input);

  protected abstract DataSource<I> getDataSource();


}
