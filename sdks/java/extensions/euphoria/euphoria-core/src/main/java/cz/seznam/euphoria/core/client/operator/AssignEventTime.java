package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.annotation.operator.Derived;
import cz.seznam.euphoria.core.annotation.operator.StateComplexity;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.graph.DAG;

import java.util.Objects;

/** A convenient alias for:
 *
 * <pre>{@code
 *   Dataset<T> input = ...;
 *   Dataset<T> withStamps = FlatMap.of(input)
 *      .using(t -> t)
 *      .eventTimeBy(evt-time-fn)
 *      .output();
 * }</pre>
 */
@Derived(
    state = StateComplexity.ZERO,
    repartitions = 0
)
public class AssignEventTime<IN> extends ElementWiseOperator<IN, IN> {

  public static class OfBuilder implements Builders.Of {
    private final String name;

    OfBuilder(String name) {
      this.name = name;
    }

    @Override
    public <IN> UsingBuilder<IN> of(Dataset<IN> input) {
      return new UsingBuilder<>(name, Objects.requireNonNull(input));
    }
  }

  public static class UsingBuilder<IN> {
    private final String name;
    private final Dataset<IN> input;

    UsingBuilder(String name, Dataset<IN> input) {
      this.name = name;
      this.input = input;
    }

    /** @see FlatMap.EventTimeBuilder#eventTimeBy(ExtractEventTime) */
    public OutputBuilder<IN> using(ExtractEventTime<IN> fn) {
      return new OutputBuilder<>(name, input, Objects.requireNonNull(fn));
    }
  }

  public static class OutputBuilder<IN> implements Builders.Output<IN> {
    private final String name;
    private final Dataset<IN> input;
    private final ExtractEventTime<IN> eventTimeFn;

    OutputBuilder(String name, Dataset<IN> input, ExtractEventTime<IN> eventTimeFn) {
      this.name = name;
      this.input = input;
      this.eventTimeFn = eventTimeFn;
    }

    @Override
    public Dataset<IN> output() {
      Flow flow = input.getFlow();
      AssignEventTime<IN> op = new AssignEventTime<>(name, flow, input, eventTimeFn);
      flow.add(op);
      return op.output();
    }
  }

  private final ExtractEventTime<IN> eventTimeFn;

  AssignEventTime(String name, Flow flow, Dataset<IN> input,
                  ExtractEventTime<IN> eventTimeFn) {
    super(name, flow, input);
    this.eventTimeFn = eventTimeFn;
  }

  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    return DAG.of(new FlatMap<IN, IN>(
        getName(), getFlow(), input, (i, c) -> c.collect(i), eventTimeFn));
  }

  /** @see FlatMap#getEventTimeExtractor() */
  public ExtractEventTime<IN> getEventTimeExtractor() {
    return eventTimeFn;
  }

  /**
   * Starts building a named {@link AssignEventTime} operator.
   *
   * @param name a user provided name of the new operator to build
   *
   * @return a builder to complete the setup of the new {@link AssignEventTime}
   *          operator
   */
  public static OfBuilder named(String name) {
    return new OfBuilder(Objects.requireNonNull(name));
  }

  /**
   * Starts building a nameless {@link AssignEventTime} operator to (re-)assign
   * event time the given input dataset's elements.
   *
   * @param <IN> the type of elements of the input dataset
   *
   * @param input the input data set to be processed
   *
   * @return a builder to complete the setup of the new {@link AssignEventTime}
   *          operator
   *
   * @see #named(String)
   * @see OfBuilder#of(Dataset)
   */
  public static <IN> UsingBuilder<IN> of(Dataset<IN> input) {
    return new UsingBuilder<>("AssignEventTime", input);
  }
}
