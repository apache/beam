
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.GroupedDataset;
import cz.seznam.euphoria.core.client.dataset.Partitioner;
import cz.seznam.euphoria.core.client.dataset.Partitioning;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.DataSource;

import java.util.Collection;
import java.util.Objects;

/**
 * Operator taking input dataset and performs repartition and key extraction.
 */
public class GroupByKey<IN, KEY, VALUE>
    extends ElementWiseOperator<IN, Pair<KEY, VALUE>>
    implements PartitioningAware<KEY>
{

  public static class Builder1 {
    private final String name;

    Builder1(String name) {
      this.name = Objects.requireNonNull(name);
    }

    public <IN> Builder2<IN> of(Dataset<IN> input) {
      return new Builder2<>(name, input);
    }
  }

  public static class Builder2<IN>  {
    private final String name;
    private final Dataset<IN> input;
    Builder2(String name, Dataset<IN> input) {
      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
    }
    public <KEY> Builder3<IN, KEY> keyBy(UnaryFunction<IN, KEY> keyExtractor) {
      return new Builder3<>(name, input, keyExtractor);
    }
  }

  public static class Builder3<IN, KEY>
          extends PartitioningBuilder<KEY, Builder3<IN, KEY>>
  {
    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunction<IN, KEY> keyExtractor;
    Builder3(String name, Dataset<IN> input, UnaryFunction<IN, KEY> keyExtractor) {
      // define default partitioning
      super(new DefaultPartitioning<>(input.getPartitioning().getNumPartitions()));

      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
    }

    public <VALUE> Builder4<IN, KEY, VALUE> valueBy(UnaryFunction<IN, VALUE> valueExtractor) {
      return new Builder4<>(name, input, keyExtractor, valueExtractor, this);
    }

    public GroupedDataset<KEY, IN> output() {
      Flow flow = input.getFlow();
      GroupByKey<IN, KEY, IN> gbk = new GroupByKey<>(name, flow, input,
              keyExtractor, e -> e, getPartitioning());
      flow.add(gbk);

      return gbk.output();
    }
  }

  public static class Builder4<IN, KEY, VALUE>
          extends PartitioningBuilder<KEY, Builder4<IN, KEY, VALUE>>
  {
    private final String name;
    private final Dataset<IN> input;
    private final UnaryFunction<IN, KEY> keyExtractor;
    private final UnaryFunction<IN, VALUE> valueExtractor;

    Builder4(String name,
             Dataset<IN> input,
             UnaryFunction<IN, KEY> keyExtractor,
             UnaryFunction<IN, VALUE> valueExtractor,
             PartitioningBuilder<KEY, ?> partitioning)
    {
      super(partitioning);

      this.name = Objects.requireNonNull(name);
      this.input = Objects.requireNonNull(input);
      this.keyExtractor = Objects.requireNonNull(keyExtractor);
      this.valueExtractor = Objects.requireNonNull(valueExtractor);
    }

    public GroupedDataset<KEY, VALUE> output() {
      Flow flow = input.getFlow();
      GroupByKey<IN, KEY, VALUE> gbk = new GroupByKey<>(name, flow, input,
              keyExtractor, valueExtractor, getPartitioning());
      flow.add(gbk);

      return gbk.output();
    }
  }

  public static <IN> Builder2<IN> of(Dataset<IN> input) {
    return new Builder2<>("GroupByKey", input);
  }

  public static Builder1 named(String name) {
    return new Builder1(name);
  }

  final UnaryFunction<IN, KEY> keyExtractor;
  final UnaryFunction<IN, VALUE> valueExtractor;
  final GroupedDataset<KEY, VALUE> decoratedOutput;
  final Partitioning<KEY> partitioning;


  GroupByKey(String name,
             Flow flow,
             Dataset<IN> input,
             UnaryFunction<IN, KEY> keyExtractor,
             UnaryFunction<IN, VALUE> valueExtractor,
             Partitioning<KEY> partitioning)
  {
    super(name, flow, input);
    this.keyExtractor = keyExtractor;
    this.decoratedOutput = groupedDecorate(this.output);
    this.valueExtractor = valueExtractor;
    this.partitioning = partitioning;
  }

  @Override
  public GroupedDataset<KEY, VALUE> output() {
    return decoratedOutput;
  }

  @Override
  public Partitioning<KEY> getPartitioning() {
    return partitioning;
  }




  @SuppressWarnings("unchecked")
  private GroupedDataset<KEY, VALUE> groupedDecorate(Dataset<Pair<KEY, VALUE>> output) {
    return new GroupedDataset<KEY, VALUE>() {

      @Override
      public Flow getFlow() {
        return output.getFlow();
      }

      @Override
      public DataSource<Pair<KEY, VALUE>> getSource() {
        return null;
      }

      @Override
      public Operator<?, Pair<KEY, VALUE>> getProducer() {
        return output.getProducer();
      }

      @Override
      public Collection<Operator<?, ?>> getConsumers() {
        return getFlow().getConsumersOf(this);
      }

      @Override
      public <X> Partitioning<X> getPartitioning() {
        return output.getPartitioning();
      }

      @Override
      public boolean isBounded() {
        return output.isBounded();
      }

      @Override
      public void persist(DataSink<Pair<KEY, VALUE>> sink) {
        output.persist(sink);
      }

      @Override
      public void checkpoint(DataSink<Pair<KEY, VALUE>> sink) {
        output.checkpoint(sink);
      }

    };
  }


  @Override
  @SuppressWarnings("unchecked")
  public DAG<Operator<?, ?>> getBasicOps() {
    // we can implement this by basically just repartition operator
    int numPartitions = partitioning.getNumPartitions();
    numPartitions = numPartitions > 0
        ? numPartitions : input.getPartitioning().getNumPartitions();
    Partitioner<KEY> partitioner = partitioning.getPartitioner();
    Partitioner<Pair<KEY, VALUE>> repartitionPart
        = e -> partitioner.getPartition(e.getFirst());
    String name = getName() + "::" + "Map";
    MapElements<IN, Pair<KEY, VALUE>> map =
            new MapElements<>(name, input.getFlow(), input,
                    e -> Pair.of(keyExtractor.apply(e), valueExtractor.apply(e)));
    name = getName() + "::" + "Repartition";
    Repartition<Pair<KEY, VALUE>> repartition = new Repartition<>(
            name,
            input.getFlow(),
            map.output(),
            new DefaultPartitioning<>(numPartitions, repartitionPart));
    DAG<Operator<?, ?>> dag = DAG.of(map);
    return dag.add(repartition, map);
  }


}
