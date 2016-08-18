
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.Partitioning;
import cz.seznam.euphoria.core.client.dataset.WindowContext;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;

import java.util.Collection;
import java.util.Collections;

/**
 * Operator operating on window level with state information.
 */
public class StateAwareWindowWiseSingleInputOperator<
    IN, WIN, KIN, KEY, OUT, WLABEL, W extends WindowContext<?, WLABEL>,
    OP extends StateAwareWindowWiseSingleInputOperator<IN, WIN, KIN, KEY, OUT, WLABEL, W, OP>>
    extends StateAwareWindowWiseOperator<IN, WIN, KIN, KEY, OUT, WLABEL, W, OP> {

  protected final Dataset<IN> input;
  private final Dataset<OUT> output;

  protected StateAwareWindowWiseSingleInputOperator(
          String name, Flow flow, Dataset<IN> input, UnaryFunction<KIN, KEY> extractor,
          Windowing<WIN, ?, WLABEL, W> windowing /* optional */,
          Partitioning<KEY> partitioning) {
    
    super(name, flow, windowing, extractor, partitioning);
    this.input = input;
    this.output = createOutput(input);
  }

  protected StateAwareWindowWiseSingleInputOperator(
      String name, Flow flow, Dataset<IN> input, UnaryFunction<KIN, KEY> extractor,
      Windowing<WIN, ?, WLABEL,  W> windowing) {
    this(name, flow, input, extractor, windowing, input.getPartitioning());
  }

  @Override
  public Collection<Dataset<IN>> listInputs() {
    return Collections.singletonList(input);
  }

  public Dataset<IN> input() {
    return input;
  }

  public Dataset<OUT> output() {
    return output;
  }

}
