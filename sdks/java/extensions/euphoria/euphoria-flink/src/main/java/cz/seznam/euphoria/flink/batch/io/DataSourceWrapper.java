package cz.seznam.euphoria.flink.batch.io;

import cz.seznam.euphoria.core.client.dataset.windowing.Batch;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowID;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
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
        implements InputFormat<WindowedElement<Batch.Label, T>,
        PartitionWrapper<T>>,
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
    // TODO euphoria cannot provide the stats at the moment determine whether it is
    // worth building an abstraction such that we could provide the stats to flink
    return null;
  }

  @Override
  public PartitionWrapper<T>[] createInputSplits(int minNumSplits) throws IOException {
    List<Partition<T>> partitions = dataSource.getPartitions();

    @SuppressWarnings("unchecked")
    PartitionWrapper<T>[] splits = new PartitionWrapper[partitions.size()];
    for (int i = 0; i < partitions.size(); i++) {
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
  public WindowedElement<Batch.Label, T> nextRecord(
      WindowedElement<Batch.Label, T> reuse)
      throws IOException
  {
    return new WindowedElement<>(new WindowID<>(Batch.Label.get()), reader.next());
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override
  @SuppressWarnings("unchecked")
  public TypeInformation<T> getProducedType() {
    return TypeInformation.of((Class) WindowedElement.class);
  }
}
