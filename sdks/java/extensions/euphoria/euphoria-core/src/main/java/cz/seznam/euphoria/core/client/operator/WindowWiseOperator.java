
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.Window;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;

/**
 * Operator working on some context.
 */
public abstract class WindowWiseOperator<
    IN, WIN, OUT, W extends Window<?>, TYPE extends Dataset<OUT>>
    extends Operator<IN, OUT, TYPE> implements WindowAware<WIN, W> {

  // the windowing of this operator
  // default is batch behavior
  protected final Windowing<WIN, ?, W> windowing;
   
  public WindowWiseOperator(String name, Flow flow,
      Windowing<WIN, ?, W> windowing) {
    super(name, flow);
    this.windowing = windowing;
  }

  @Override
  public Windowing<WIN, ?, W> getWindowing() {
    return windowing;
  }
 

}
