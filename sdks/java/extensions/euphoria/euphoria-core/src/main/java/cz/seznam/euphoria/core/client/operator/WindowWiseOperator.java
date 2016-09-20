
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowContext;
import cz.seznam.euphoria.core.client.flow.Flow;

/**
 * Operator working on some context.
 */
public abstract class WindowWiseOperator<
    IN, WIN, OUT, WLABEL, W extends WindowContext<?, WLABEL>>
    extends Operator<IN, OUT> implements WindowAware<WIN, W> {

  protected Windowing<WIN, ?, WLABEL, W> windowing;

  public WindowWiseOperator(String name,
                            Flow flow,
                            Windowing<WIN, ?, WLABEL, W> windowing /* optional */)
  {
    super(name, flow);
    this.windowing = windowing;
  }

  @Override
  public Windowing<WIN, ?, WLABEL, W> getWindowing() {
    return windowing;
  }

}
