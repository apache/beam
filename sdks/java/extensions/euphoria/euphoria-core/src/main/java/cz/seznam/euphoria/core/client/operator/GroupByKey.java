
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.GroupedDataset;
import cz.seznam.euphoria.core.client.dataset.HashPartitioning;
import cz.seznam.euphoria.core.client.dataset.Partitioner;
import cz.seznam.euphoria.core.client.dataset.Partitioning;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.io.DataSink;
import cz.seznam.euphoria.core.client.io.DataSource;
import java.util.Collection;

/**
 * Operator taking input dataset and performs repartition and key extraction.
 */
public class GroupByKey<IN, KEY, VALUE>
    extends ElementWiseOperator<IN, Pair<KEY, VALUE>>
    implements PartitioningAware<KEY> {

  public static class Builder1<IN>  {
    final Dataset<IN> input;
    Builder1(Dataset<IN> input) {
      this.input = input;
    }
    public <KEY> GroupByKey<IN, KEY, IN> keyBy(
        UnaryFunction<IN, KEY> keyExtractor) {
      Flow flow = input.getFlow();
      return flow.add(new GroupByKey<>(input.getFlow(), input,
          keyExtractor, e -> e));
    }
    public <VALUE> Builder2<IN, VALUE> valueBy(UnaryFunction<IN, VALUE> valueExtractor) {
      return new Builder2<>(input, valueExtractor);
    }
  }
  public static class Builder2<IN, VALUE> {
    final Dataset<IN> input;
    final UnaryFunction<IN, VALUE> valueExtractor;
    Builder2(Dataset<IN> input, UnaryFunction<IN, VALUE> valueExtractor) {
      this.input = input;
      this.valueExtractor = valueExtractor;
    }
    public <KEY> GroupByKey<IN, KEY, VALUE> keyBy(
        UnaryFunction<IN, KEY> keyExtractor) {
      Flow flow = input.getFlow();
      return flow.add(new GroupByKey<>(input.getFlow(), input,
          keyExtractor, valueExtractor));
    }
  }

  public static <IN> Builder1<IN> of(Dataset<IN> input) {
    return new Builder1<>(input);
  }

  private final UnaryFunction<IN, KEY> keyExtractor;
  private final UnaryFunction<IN, VALUE> valueExtractor;
  private final GroupedDataset<KEY, VALUE> decoratedOutput;

  private Partitioning<KEY> partitioning = new HashPartitioning<>(
      input.getPartitioning().getNumPartitions());
  // values extractor - by default identity
  

  public GroupByKey(Flow flow, Dataset<IN> input,
      UnaryFunction<IN, KEY> keyExtractor,
      UnaryFunction<IN, VALUE> valueExtractor) {
    super("GroupByKey", flow, input);
    this.keyExtractor = keyExtractor;
    this.decoratedOutput = groupedDecorate(this.output);
    this.valueExtractor = valueExtractor;
  }

  @Override
  public GroupedDataset<KEY, VALUE> output() {
    return decoratedOutput;
  }

  @Override
  public Partitioning<KEY> getPartitioning() {
    return partitioning;
  }

  public GroupByKey<IN, KEY, VALUE> setPartitioner(Partitioner<KEY> partitioner) {
    final int currentNumPartitions = partitioning.getNumPartitions();
    partitioning = new Partitioning<KEY>() {
      @Override
      public Partitioner<KEY> getPartitioner() {
        return partitioner;
      }
      @Override
      public int getNumPartitions() {
        return currentNumPartitions;
      }
    };
    return this;
  }

  public GroupByKey<IN, KEY, VALUE> setNumPartitions(int numPartitions) {
    final Partitioner<KEY> currentPartitioner = partitioning.getPartitioner();
    partitioning = new Partitioning<KEY>() {
      @Override
      public Partitioner<KEY> getPartitioner() {
        return currentPartitioner;
      }
      @Override
      public int getNumPartitions() {
        return numPartitions;
      }
    };
    return this;
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
    Map<IN, Pair<KEY, VALUE>> map = new Map<>(
        input.getFlow(), input, e -> {
          return Pair.of(keyExtractor.apply(e), valueExtractor.apply(e));
        });
    Repartition<Pair<KEY, VALUE>> repartition = new Repartition<>(
        input.getFlow(), map.output(), repartitionPart, numPartitions);
    DAG<Operator<?, ?>> dag = DAG.of(map);
    return dag.add(repartition, map);
  }


}
