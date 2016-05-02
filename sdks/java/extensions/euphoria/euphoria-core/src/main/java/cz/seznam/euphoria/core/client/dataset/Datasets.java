
package cz.seznam.euphoria.core.client.dataset;

import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.core.client.operator.PartitioningAware;

/**
 * Various dataset related utils.
 */
public class Datasets {

  /** Create output dataset for given operator. */
  @SuppressWarnings("unchecked")
  public static <IN, OUT> Dataset<OUT> createOutputFor(
      Flow flow, Dataset<IN> input, Operator<IN, OUT> op) {

      return new OutputDataset<OUT>(flow, (Operator) op, input.isBounded()) {

        @Override
        @SuppressWarnings("unchecked")
        public <X> Partitioning<X> getPartitioning()
        {
          if (op instanceof PartitioningAware) {
            // only partitioning aware operators change the partitioning
            PartitioningAware<IN> pa = (PartitioningAware<IN>) op;
            return (Partitioning<X>) pa.getPartitioning();
          }
          return input.getPartitioning();
        }

      };

  }


  /** Create dataset from {@code DataSource}. */
  public static <T> Dataset<T> createInputFromSource(
      Flow flow, DataSource<T> source) {
    
    return new InputDataset<T>(flow, source, source.isBounded()) {

      @Override
      public <X> Partitioning<X> getPartitioning()
      {
        return new Partitioning<X>() {

          @Override
          public Partitioner<X> getPartitioner()
          {
            return new HashPartitioner<>();
          }

          @Override
          public int getNumPartitions()
          {
            return source.getPartitions().size();
          }

        };
      }
    };
  }

}
