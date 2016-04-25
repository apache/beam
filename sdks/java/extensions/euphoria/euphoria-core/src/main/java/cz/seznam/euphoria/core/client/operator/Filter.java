
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.functional.UnaryPredicate;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.PCollection;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.graph.DAG;

/**
 * Operator performing a filter operation.
 */
public class Filter<IN, TYPE extends Dataset<IN>>
    extends ElementWiseOperator<IN, IN, TYPE> {

  public static class Builder<IN> {
    Dataset<IN> input;
    Builder(Dataset<IN> input) {
      this.input = input;
    }
    public Filter<IN, Dataset<IN>> by(UnaryPredicate<IN> predicate) {
      Flow flow = input.getFlow();
      Filter<IN, Dataset<IN>> filter = new Filter<>(flow, input, predicate);
      return flow.add(filter);
    }
  }
  public static class BuilderBatch<IN> {
    PCollection<IN> input;
    BuilderBatch(PCollection<IN> input) {
      this.input = input;
    }
    public Filter<IN, PCollection<IN>> by(UnaryPredicate<IN> predicate) {
      Flow flow = input.getFlow();
      return flow.add(new Filter<>(flow, input, predicate));
    }
  }

  public static <IN> Builder<IN> of(Dataset<IN> input) {
    return new Builder<>(input);
  }

  public static <IN> BuilderBatch<IN> of(PCollection<IN> input) {
    return new BuilderBatch<>(input);
  }



  private final UnaryPredicate<IN> predicate;

  Filter(Flow flow, Dataset<IN> input, UnaryPredicate<IN> predicate) {
    super("Filter", flow, input);
    this.predicate = predicate;
  }

  @Override
  @SuppressWarnings("unchecked")
  public TYPE output() {
    return super.output();
  }

  /** This operator can be implemented using FlatMap. */
  @Override
  public DAG<Operator<?, ?, ?>> getBasicOps() {
    return DAG.of(new FlatMap<IN, IN, TYPE>(getFlow(), input,
        (elem, collector) -> {
          if (predicate.apply(elem)) {
            collector.collect(elem);
          }
        }));
  }



}
