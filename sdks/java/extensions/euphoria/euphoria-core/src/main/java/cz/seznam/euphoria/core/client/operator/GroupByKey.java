
package cz.seznam.euphoria.core.client.operator;

import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.client.dataset.GroupedDataset;
import cz.seznam.euphoria.core.client.dataset.GroupedOutputPCollection;
import cz.seznam.euphoria.core.client.dataset.GroupedOutputPStream;
import cz.seznam.euphoria.core.client.dataset.HashPartitioning;
import cz.seznam.euphoria.core.client.dataset.PCollection;
import cz.seznam.euphoria.core.client.dataset.Partitioner;
import cz.seznam.euphoria.core.client.dataset.Partitioning;
import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.functional.UnaryFunction;
import cz.seznam.euphoria.core.client.graph.DAG;

/**
 * Operator taking input dataset and performs repartition and key extraction.
 */
public class GroupByKey<IN, KEY, VALUE, TYPE extends GroupedDataset<KEY, VALUE>>
    extends ElementWiseOperator<IN, Pair<KEY, VALUE>, TYPE>
    implements PartitioningAware<KEY> {

  public static class Builder1<IN>  {
    final Dataset<IN> input;
    Builder1(Dataset<IN> input) {
      this.input = input;
    }
    public <KEY> GroupByKey<IN, KEY, IN, GroupedDataset<KEY, IN>> keyBy(
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
    public <KEY> GroupByKey<IN, KEY, VALUE, GroupedDataset<KEY, VALUE>> keyBy(
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
  private final TYPE decoratedOutput;

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
  public TYPE output() {
    return decoratedOutput;
  }

  @Override
  public Partitioning<KEY> getPartitioning() {
    return partitioning;
  }

  public GroupByKey<IN, KEY, VALUE, TYPE> setPartitioner(Partitioner<KEY> partitioner) {
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

  public GroupByKey<IN, KEY, VALUE, TYPE> setNumPartitions(int numPartitions) {
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
  private TYPE groupedDecorate(Dataset<Pair<KEY, VALUE>> output) {
    if (output instanceof PCollection) {
      return (TYPE) new GroupedOutputPCollection<KEY, VALUE>(input.getFlow(), (Operator) this) {
        @Override
        @SuppressWarnings("unchecked")
        public <X> Partitioning<X> getPartitioning() {
          return (Partitioning<X>) GroupByKey.this.getPartitioning();
        }
      };
    }
    return (TYPE) new GroupedOutputPStream<KEY, VALUE>(input.getFlow(), (Operator) this) {
      @Override
      public <X> Partitioning<X> getPartitioning() {
        return (Partitioning<X>) GroupByKey.this.getPartitioning();
      }     
    };
  }


  @Override
  @SuppressWarnings("unchecked")
  public DAG<Operator<?, ?, ?>> getBasicOps() {
    // we can implement this by basically just repartition operator
    int numPartitions = partitioning.getNumPartitions();
    numPartitions = numPartitions > 0
        ? numPartitions : input.getPartitioning().getNumPartitions();
    Partitioner<KEY> partitioner = partitioning.getPartitioner();
    Partitioner<Pair<KEY, VALUE>> repartitionPart
        = e -> partitioner.getPartition(e.getFirst());
    Map<IN, Pair<KEY, VALUE>, Dataset<Pair<KEY, VALUE>>> map = new Map<>(
        input.getFlow(), input, e -> {
          return Pair.of(keyExtractor.apply(e), valueExtractor.apply(e));
        });
    Repartition<Pair<KEY, VALUE>, TYPE> repartition = new Repartition<>(
        input.getFlow(), map.output(), repartitionPart, numPartitions);
    return DAG.of(map, repartition);
  }


}
