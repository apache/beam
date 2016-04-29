
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.BatchWindowing;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.Window;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;

import java.util.Arrays;
import java.util.Collection;

/**
 * Operator performing a mapping operation on whole window.
 */
public class MapWindow<IN, OUT, W extends Window<?>>
    extends WindowWiseOperator<IN, IN, OUT, W> {

  public static class Builder1<IN> {
    final Dataset<IN> input;
    Builder1(Dataset<IN> input) {
      this.input = input;
    }
    public <OUT> Builder2<IN, OUT>  using(UnaryFunctor<Iterable<IN>, OUT> functor) {
      return new Builder2<>(input, functor);
    }
  }
  public static class Builder2<IN, OUT> {
    final Dataset<IN> input;
    final UnaryFunctor<Iterable<IN>, OUT> mapper;
    Builder2(Dataset<IN> input, UnaryFunctor<Iterable<IN>, OUT> mapper) {
      this.input = input;
      this.mapper = mapper;
    }
    public <W extends Window<?>> MapWindow<IN, OUT, W> windowBy(
        Windowing<IN, ?, W> windowing) {
      Flow flow = input.getFlow();
      MapWindow<IN, OUT, W> mapWindow = new MapWindow<>(flow, input, windowing);
      return flow.add(mapWindow);
    }
    public Dataset<OUT> output() {
      return windowBy(BatchWindowing.get()).output();
    }

  }

  public static <IN> Builder1<IN> of(Dataset<IN> input) {
    return new Builder1<>(input);
  }

  private final Dataset<IN> input;
  private final Dataset<OUT> output;

  MapWindow(Flow flow, Dataset<IN> input, Windowing<IN, ?, W> windowing) {
    super("MapWindow", flow, windowing);
    this.input = input;
    this.output = createOutput(input);
  }

  @Override
  public Collection<Dataset<IN>> listInputs() {
    return Arrays.asList(input);
  }

  @Override
  public Dataset<OUT> output() {
    return output;
  }

}
