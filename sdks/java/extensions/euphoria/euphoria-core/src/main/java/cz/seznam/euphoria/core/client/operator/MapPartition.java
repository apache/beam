
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.BatchWindowing;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.PCollection;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;

/**
 * Map partition. Batch case.
 */
public class MapPartition<IN, OUT> extends MapWindow<
    IN, OUT, BatchWindowing.BatchWindow, PCollection<OUT>> {

  public static class Builder1<IN> {
    final PCollection<IN> input;
    Builder1(PCollection<IN> input) {
      this.input = input;
    }
    public <OUT> MapPartition<IN, OUT> using(UnaryFunctor<Iterable<IN>, OUT> mapper) {
      Flow flow = input.getFlow();
      MapPartition<IN, OUT> mapPartition = new MapPartition<>(flow, input);
      return flow.add(mapPartition);
    }
  }

  public static <IN> Builder1<IN> of(PCollection<IN> input) {
    return new Builder1<>(input);
  }

  MapPartition(Flow flow, Dataset<IN> input) {
    super(flow, input, BatchWindowing.get());
  }

}
