
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.BatchWindowing;
import cz.seznam.euphoria.core.client.dataset.Window;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;

/**
 * Operator working on some context.
 */
public abstract class WindowWiseOperator<
    IN, WIN, OUT, W extends Window<?, ?>>
    extends Operator<IN, OUT> implements WindowAware<WIN, W> {

  // the windowing of this operator
  // default is batch behavior
  @SuppressWarnings("unchecked")
  protected Windowing<WIN, ?, ?, W> windowing = (Windowing) BatchWindowing.get();
   
  public WindowWiseOperator(String name, Flow flow,
      Windowing<WIN, ?, ?, W> windowing) {
    super(name, flow);

    if (windowing != null) {
      this.windowing = windowing;
    }
  }

  @Override
  public Windowing<WIN, ?, ?, W> getWindowing() {
    return windowing;
  } 

}
