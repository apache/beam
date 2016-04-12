
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.Partitioning;
import cz.seznam.euphoria.core.client.dataset.Window;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * Operator operating on window level with state information.
 */
public class StateAwareWindowWiseSingleInputOperator<
    IN, KEY, OUT, W extends Window<?, W>, TYPE extends Dataset<OUT>,
    OP extends StateAwareWindowWiseSingleInputOperator<IN, KEY, OUT, W, TYPE, OP>>
    extends StateAwareWindowWiseOperator<IN, KEY, OUT, W, TYPE, OP>
    implements StateAware<IN, KEY> {

  protected final Dataset<IN> input;
  private final TYPE output;

  protected StateAwareWindowWiseSingleInputOperator(
          String name, Flow flow, Dataset<IN> input, UnaryFunction<IN, KEY> extractor,
          Windowing<IN, ?, W> windowing,
          Partitioning<KEY> partitioning) {
    
    super(name, flow, windowing, extractor, partitioning);
    this.input = input;
    this.output = createOutput(input);
  }

  protected StateAwareWindowWiseSingleInputOperator(
      String name, Flow flow, Dataset<IN> input, UnaryFunction<IN, KEY> extractor,
      Windowing<IN, ?, W> windowing) {
    this(name, flow, input, extractor, windowing, input.getPartitioning());
  }

  @Override
  public Collection<Dataset<IN>> listInputs() {
    return Collections.singletonList(input);
  }

  public Dataset<IN> input() {
    return input;
  }

  public TYPE output() {
    return output;
  }

}
