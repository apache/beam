
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.BatchWindowing;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.functional.CombinableReduceFunction;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.HashPartitioning;
import cz.seznam.euphoria.core.client.dataset.Window;
import cz.seznam.euphoria.core.client.flow.Flow;

/**
 * Operator outputting distinct (based on equals) elements.
 */
public class Distinct<IN, W extends Window<?, ?>>
    extends StateAwareWindowWiseSingleInputOperator<IN, IN, IN, IN, IN, W, Distinct<IN, W>> {

  public static class Builder1<IN> {
    Dataset<IN> input;
    Builder1(Dataset<IN> input) {
      this.input = input;
    }
    public <W extends Window<?, ?>> Distinct<IN, W> windowBy(
        Windowing<IN, ?, ?, W> windowing) {
      Flow flow = input.getFlow();
      Distinct<IN, W> distinct = new Distinct<>(flow, input, windowing);
      return flow.add(distinct);
    }
    public Dataset<IN> output() {
      return windowBy(BatchWindowing.get()).output();
    }

  }

  public static <IN> Builder1<IN> of(Dataset<IN> input) {
    return new Builder1<>(input);
  }

  Distinct(Flow flow, Dataset<IN> input, Windowing<IN, ?, ?, W> windowing) {
    super("Distinct", flow, input, e -> e, windowing,  new HashPartitioning<>());
  }

  @Override
  public DAG<Operator<?, ?>> getBasicOps() {

    Flow flow = input.getFlow();
    ReduceByKey<IN, IN, Void, Void, W> reduce;
    reduce = new ReduceByKey<>(
        flow, input, e -> e, e -> null,
        windowing,
        (CombinableReduceFunction<Void>) e -> null);

    reduce.setPartitioning(getPartitioning());
    Dataset<Pair<IN, Void>> reduced = reduce.output();
    Map<Pair<IN, Void>, IN> format = new Map<>(flow, reduced, Pair::getFirst);

    DAG<Operator<?, ?>> dag = DAG.of(reduce);
    dag.add(format, reduce);
    return dag;
  }



}
