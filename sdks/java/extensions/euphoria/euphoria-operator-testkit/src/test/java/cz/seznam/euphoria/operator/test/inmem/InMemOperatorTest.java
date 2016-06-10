
package cz.seznam.euphoria.operator.test.inmem;

import cz.seznam.euphoria.core.executor.InMemExecutor;
import cz.seznam.euphoria.operator.test.AllOperatorTest;

/**
 * InMemExecutor applied on operator test cases.
 */
public class InMemOperatorTest extends AllOperatorTest {

  InMemExecutor executor;
  
  public InMemOperatorTest() {
    super(s -> new InMemExecutor());
  }

}
