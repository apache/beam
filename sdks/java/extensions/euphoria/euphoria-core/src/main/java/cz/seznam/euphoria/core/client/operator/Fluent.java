package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.dataset.Partitioner;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.executor.Executor;
import cz.seznam.euphoria.core.util.Settings;

import static java.util.Objects.requireNonNull;

public class Fluent {

  public static class Flow {
    private final cz.seznam.euphoria.core.client.flow.Flow wrap;

    private Flow(cz.seznam.euphoria.core.client.flow.Flow wrap) {
      this.wrap = requireNonNull(wrap);
    }

    public <T> Dataset<T> read(DataSource<T> src) {
      return new Dataset<>(wrap.createInput(src));
    }
  }

  public static class Dataset<T> {
    private final cz.seznam.euphoria.core.client.dataset.Dataset<T> wrap;

    private Dataset(cz.seznam.euphoria.core.client.dataset.Dataset<T> wrap) {
      this.wrap = requireNonNull(wrap);
    }

    public cz.seznam.euphoria.core.client.dataset.Dataset<T> unwrap() {
      return this.wrap;
    }

    public <S> Dataset<S>
    apply(UnaryFunction<cz.seznam.euphoria.core.client.dataset.Dataset<T>,
        OutputProvider<S>> output)
    {
      return new Dataset<>(requireNonNull(output.apply(this.wrap)).output());
    }

    public Dataset<T> repartition(Partitioner<T> partitioner) {
      return new Dataset<>(Repartition.of(this.wrap)
          .setPartitioner(requireNonNull(partitioner))
          .output());
    }

    public Dataset<T> repartition(int num) {
      return new Dataset<>(Repartition.of(this.wrap)
          .setNumPartitions(num)
          .output());
    }

    public Dataset<T> repartition(int num, Partitioner<T> partitioner) {
      return new Dataset<>(Repartition.of(this.wrap)
          .setNumPartitions(num)
          .setPartitioner(requireNonNull(partitioner))
          .output());
    }

    public <S> Dataset<S> mapElements(UnaryFunction<T, S> f) {
      return new Dataset<>(MapElements.of(this.wrap).using(requireNonNull(f)).output());
    }

    public <S> Dataset<S> flatMap(UnaryFunctor<T, S> f) {
      return new Dataset<>(FlatMap.of(this.wrap).using(requireNonNull(f)).output());
    }

    public Dataset<T> distinct() {
      return new Dataset<>(Distinct.of(this.wrap).output());
    }

    public Dataset<T> union(Dataset<T> other) {
      return new Dataset<>(Union.of(this.wrap, other.wrap).output());
    }

    public <S extends DataSink<T>> Dataset<T> persist(S dst) {
      this.wrap.persist(dst);
      return this;
    }

    public void execute(Executor exec) {
      exec.waitForCompletion(this.wrap.getFlow());
    }
  }

  public static Flow flow(String name) {
    return new Flow(cz.seznam.euphoria.core.client.flow.Flow.create(name));
  }

  public static Flow flow(String name, Settings settings) {
    return new Flow(cz.seznam.euphoria.core.client.flow.Flow.create(name, settings));
  }

  public static <T> Dataset<T>
  wrap(cz.seznam.euphoria.core.client.dataset.Dataset<T> xs)
  {
    return new Dataset<>(xs);
  }

  private Fluent() {}
}

