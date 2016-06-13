package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryPredicate;
import cz.seznam.euphoria.core.client.graph.DAG;

import java.util.Objects;

/**
 * Operator performing a filter operation.
 */
public class Filter<IN> extends ElementWiseOperator<IN, IN> {

  public static class Builder1 {
    private final String name;

    Builder1(String name) {
      this.name = name;
    }

    public <IN> Builder2<IN> of(Dataset<IN> input) {
      return new Builder2<>(name, input);
    }
  }

  public interface OutputBuilder<IN> {
    Dataset<IN> output();
  }

  public interface ByBuilder<IN> {
    OutputBuilder<IN> by(UnaryPredicate<IN> predicate);
  }

  public static class Builder2<IN> implements
          ByBuilder<IN>,
          OutputBuilder<IN>,
      cz.seznam.euphoria.core.client.operator.OutputBuilder<IN>
  {
    private final String name;
    private final Dataset<IN> input;
    private UnaryPredicate<IN> predicate;

    Builder2(String name, Dataset<IN> input) {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
    }

    @Override
    public OutputBuilder<IN> by(UnaryPredicate<IN> predicate) {
      this.predicate = Objects.requireNonNull(predicate);
      return this;
    }

    @Override
    public Dataset<IN> output() {
      Flow flow = input.getFlow();
      Filter<IN> filter = new Filter<>(name, flow, input, predicate);
      flow.add(filter);

      return filter.output();
    }
  }

  public static Builder1 named(String name) {
    return new Builder1(name);
  }

  public static <IN> ByBuilder<IN> of(Dataset<IN> input) {
    return new Builder2<>("Filter", input);
  }

  final UnaryPredicate<IN> predicate;

  Filter(String name, Flow flow, Dataset<IN> input, UnaryPredicate<IN> predicate) {
    super(name, flow, input);
    this.predicate = predicate;
  }

  
  /** This operator can be implemented using FlatMap. */
  @Override
  public DAG<Operator<?, ?>> getBasicOps() {
    return DAG.of(new FlatMap<IN, IN>(getName(), getFlow(), input,
        (elem, collector) -> {
          if (predicate.apply(elem)) {
            collector.collect(elem);
          }
        }));
  }



}
