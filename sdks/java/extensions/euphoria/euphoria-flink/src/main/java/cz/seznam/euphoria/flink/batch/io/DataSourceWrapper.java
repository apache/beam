package cz.seznam.euphoria.flink.batch.io;

import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.Partition;
import cz.seznam.euphoria.core.client.io.Reader;
import cz.seznam.euphoria.guava.shaded.com.google.common.base.Preconditions;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;

import java.io.IOException;
import java.util.List;

public class DataSourceWrapper<T>
        implements InputFormat<T, PartitionWrapper<T>>,
        ResultTypeQueryable<T>

{
  private final DataSource<T> dataSource;

  /** currently opened reader (if any) */
  private transient Reader<T> reader;

  public DataSourceWrapper(DataSource<T> dataSource) {
    Preconditions.checkArgument(dataSource.isBounded());
    this.dataSource = dataSource;
  }

  @Override
  public void configure(Configuration parameters) {
    // ignore configuration
  }

  @Override
  public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
    return null;
  }

  @Override
  public PartitionWrapper<T>[] createInputSplits(int minNumSplits) throws IOException {
    List<Partition<T>> partitions = dataSource.getPartitions();

    @SuppressWarnings("unchecked")
    PartitionWrapper<T>[] splits = new PartitionWrapper[minNumSplits];
    for (int i = 0; i < minNumSplits; i++) {
      splits[i] = new PartitionWrapper<>(i, partitions.get(i));
    }

    return splits;
  }

  @Override
  public InputSplitAssigner getInputSplitAssigner(PartitionWrapper[] partitions) {
    return new SimpleInputSplitAssigner(partitions);
  }

  @Override
  public void open(PartitionWrapper<T> partition) throws IOException {
    this.reader = partition.getPartition().openReader();
  }

  @Override
  public boolean reachedEnd() throws IOException {
    return !reader.hasNext();
  }

  @Override
  public T nextRecord(T reuse) throws IOException {
    // TODO reader could reuse instance instead allocating a new one
    return reader.next();
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override
  @SuppressWarnings("unchecked")
  public TypeInformation<T> getProducedType() {
    return TypeInformation.of((Class) Object.class);
  }
}
