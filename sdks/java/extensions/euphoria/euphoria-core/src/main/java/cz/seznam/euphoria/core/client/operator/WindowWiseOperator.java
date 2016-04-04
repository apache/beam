
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.Window;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;

/**
 * Operator working on some context.
 */
public abstract class WindowWiseOperator<
    IN, OUT, W extends Window<?, W>, TYPE extends Dataset<OUT>>
    extends Operator<IN, OUT, TYPE> implements WindowAware<IN, W> {

  // the windowing of this operator
  // default is batch behavior
  protected final Windowing<IN, ?, W> windowing;
   
  public WindowWiseOperator(String name, Flow flow,
      Windowing<IN, ?, W> windowing)
  {
    super(name, flow);
    this.windowing = windowing;
  }

  @Override
  public Windowing<IN, ?, W> getWindowing() {
    return windowing;
  }
 

}
