
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.windowing.Windowing;
import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;

/**
 * Operator working on some context.
 */
public abstract class WindowWiseOperator<
    IN, WIN, OUT, W extends Window>
    extends Operator<IN, OUT> implements WindowAware<WIN, W> {

  protected Windowing<WIN, W> windowing;
  protected UnaryFunction<WIN, Long> eventTimeAssigner;

  public WindowWiseOperator(String name,
                            Flow flow,
                            Windowing<WIN, W> windowing /* optional */,
                            UnaryFunction<WIN, Long> eventTimeAssigner /* optional */)
  {
    super(name, flow);
    this.windowing = windowing;
    this.eventTimeAssigner = eventTimeAssigner;
  }

  @Override
  public Windowing<WIN, W> getWindowing() {
    return windowing;
  }

  @Override
  public UnaryFunction<WIN, Long> getEventTimeAssigner() {
    return eventTimeAssigner;
  }
}
