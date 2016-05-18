
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.Window;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

/**
 * Operator performing a mapping operation on whole window.
 */
// TODO Is it basic operator?
public class MapWindow<IN, OUT, W extends Window<?, ?>>
    extends WindowWiseOperator<IN, IN, OUT, W> {

  public static class OfBuilder {
    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    public <IN> UsingBuilder<IN> of(Dataset<IN> input) {
      return new UsingBuilder<>(name, input);
    }
  }

  public static class UsingBuilder<IN> {
    private final String name;
    private final Dataset<IN> input;

    UsingBuilder(String name, Dataset<IN> input) {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
    }

    public <OUT> OutputBuilder<IN, OUT> using(UnaryFunctor<IN, OUT> functor) {
      return new OutputBuilder<>(name, input, functor);
    }
  }

  public static class OutputBuilder<IN, OUT> {
    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunctor<IN, OUT> functor;
    private Windowing<IN,?, ?, ?> windowing;

    OutputBuilder(String name, Dataset<IN> input, UnaryFunctor<IN, OUT> functor) {
      this.name = name;
      this.input = input;
      this.functor = functor;
    }

    public Dataset<OUT> output() {
      Flow flow = input.getFlow();
      MapWindow<IN, OUT, ?> mapWindow = new MapWindow<>(name, flow, windowing, input, functor);
      flow.add(mapWindow);

      return mapWindow.output();
    }

    public <W extends Window<?, ?>> OutputBuilder<IN, OUT> windowBy(
            Windowing<IN,?, ?, W> windowing)
    {
      this.windowing = Objects.requireNonNull(windowing);
      return this;
    }
  }

  public static <IN> UsingBuilder<IN> of(Dataset<IN> input) {
    return new UsingBuilder<>("MapWindow", input);
  }

  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  private final Dataset<IN> input;
  private final Dataset<OUT> output;
  private final UnaryFunctor<IN, OUT> functor;

  MapWindow(String name, Flow flow, Windowing<IN, ?, ?, W> windowing,
            Dataset<IN> input, UnaryFunctor<IN, OUT> functor) {
    super(name, flow, windowing);
    this.input = input;
    this.functor = functor;
    this.output = createOutput(input);
  }

  @Override
  public Collection<Dataset<IN>> listInputs() {
    return Collections.singletonList(input);
  }

  @Override
  public Dataset<OUT> output() {
    return output;
  }

}
