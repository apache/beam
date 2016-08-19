
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.windowing.ElementWindowing;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowContext;
import cz.seznam.euphoria.core.client.flow.Flow;

/**
 * Operator working on some context.
 */
public abstract class WindowWiseOperator<
    IN, WIN, OUT, WLABEL, W extends WindowContext<?, WLABEL>>
    extends Operator<IN, OUT> implements WindowAware<WIN, W> {

  protected ElementWindowing<WIN, ?, WLABEL, W> windowing;

  public WindowWiseOperator(String name,
                            Flow flow,
                            ElementWindowing<WIN, ?, WLABEL, W> windowing /* optional */)
  {
    super(name, flow);
    this.windowing = windowing;
  }

  @Override
  public ElementWindowing<WIN, ?, WLABEL, W> getWindowing() {
    return windowing;
  } 

}
