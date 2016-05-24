package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.BatchWindowing;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.Window;
import cz.seznam.euphoria.core.client.dataset.Windowing;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.util.Pair;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

/**
 * Operator performing a mapping operation on whole window.
 */
public class MapWindow<IN, OUT, WLABEL, W extends Window<?, WLABEL>, VOUT>
    extends WindowWiseOperator<IN, IN, VOUT, WLABEL, W> {

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

    public <OUT> WindowingBuilder<IN, OUT> using(UnaryFunctor<IN, OUT> functor) {
      return new WindowingBuilder<>(name, input, functor);
    }
  }

  public static class WindowingBuilder<IN, OUT> {
    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunctor<IN, OUT> functor;

    WindowingBuilder(String name, Dataset<IN> input, UnaryFunctor<IN, OUT> functor) {
      this.name = name;
      this.input = input;
      this.functor = functor;
    }

    public Dataset<OUT> output() {
      return new OutputBuilder<>(this, BatchWindowing.get()).output();
    }

    public <WLABEL, W extends Window<?, WLABEL>> OutputBuilder<IN, OUT, WLABEL, W>
    windowBy(Windowing<IN,?, WLABEL, W> windowing)
    {
      return new OutputBuilder<>(this, windowing);
    }
  }

  public static class OutputBuilder<IN, OUT, WLABEL, W extends Window<?, WLABEL>> {
    private final WindowingBuilder<IN, OUT> prev;
    private final Windowing<IN, ?, WLABEL, W> windowing;

    OutputBuilder(WindowingBuilder<IN, OUT> prev,
                  Windowing<IN, ?, WLABEL, W> windowing)
    {
      this.prev = Objects.requireNonNull(prev);
      this.windowing = Objects.requireNonNull(windowing);
    }

    public Dataset<OUT> output() {
      return outputImpl(false);
    }

    public Dataset<Pair<WLABEL, OUT>> outputWindowed() {
      return outputImpl(true);
    }

    public Dataset outputImpl(boolean windowedOutput) {
      Flow flow = prev.input.getFlow();
      MapWindow<IN, OUT, WLABEL, W, OUT> mapWindow = new MapWindow<>(
          prev.name, flow, windowing, prev.input, prev.functor, windowedOutput);
      flow.add(mapWindow);
      return mapWindow.output();
    }
  }

  public static <IN> UsingBuilder<IN> of(Dataset<IN> input) {
    return new UsingBuilder<>("MapWindow", input);
  }

  public static OfBuilder named(String name) {
    return new OfBuilder(name);
  }

  private final Dataset<IN> input;
  private final Dataset<VOUT> output;
  private final UnaryFunctor<IN, OUT> functor;
  private final boolean windowedOutput;

  MapWindow(String name, Flow flow, Windowing<IN, ?, WLABEL, W> windowing,
            Dataset<IN> input, UnaryFunctor<IN, OUT> functor,
            boolean windowedOutput)
  {
    super(name, flow, windowing);
    this.input = input;
    this.functor = functor;
    this.output = createOutput(input);
    this.windowedOutput = windowedOutput;
  }

  @Override
  public Collection<Dataset<IN>> listInputs() {
    return Collections.singletonList(input);
  }

  @Override
  public Dataset<VOUT> output() {
    return output;
  }

  public boolean isWindowedOutput() {
    return windowedOutput;
  }
}
