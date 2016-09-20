
package cz.seznam.euphoria.operator.test.inmem;

import cz.seznam.euphoria.core.executor.inmem.InMemExecutor;
import cz.seznam.euphoria.core.executor.inmem.WatermarkTriggerScheduler;
import cz.seznam.euphoria.operator.test.AllOperatorTest;

/**
 * InMemExecutor applied on operator test cases.
 */
public class InMemOperatorTest extends AllOperatorTest {

  private static final String ALLOWED_LATENESS = "euphoria.inmem.operatortest.allowed.lateness";
  
  public InMemOperatorTest() {
    super(s -> new InMemExecutor()
        .setTriggeringSchedulerSupplier(
            () -> new WatermarkTriggerScheduler(s.getLong(ALLOWED_LATENESS, 500L))));
  }

}
