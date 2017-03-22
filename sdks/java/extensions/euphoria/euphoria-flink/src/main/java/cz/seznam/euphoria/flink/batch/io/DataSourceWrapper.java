/**
 * Copyright 2016 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.flink.batch.io;

import com.google.common.base.Preconditions;
import cz.seznam.euphoria.core.client.dataset.windowing.Batch;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.Partition;
import cz.seznam.euphoria.core.client.io.Reader;
import cz.seznam.euphoria.flink.FlinkElement;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.core.io.LocatableInputSplit;

import java.io.IOException;
import java.util.List;
import java.util.function.BiFunction;

public class DataSourceWrapper<T>
        implements InputFormat<FlinkElement<Batch.BatchWindow, T>,
        PartitionWrapper<T>>,
        ResultTypeQueryable<T> {

  private final DataSource<T> dataSource;
  private final BiFunction<LocatableInputSplit[], Integer, InputSplitAssigner> splitAssignerFactory;

  /** currently opened reader (if any) */
  private transient Reader<T> reader;
  
  public DataSourceWrapper(DataSource<T> dataSource, 
                           BiFunction<LocatableInputSplit[], Integer, InputSplitAssigner> splitAssignerFactory) {
    Preconditions.checkArgument(dataSource.isBounded());
    this.dataSource = dataSource;
    this.splitAssignerFactory = splitAssignerFactory;
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
    return splitAssignerFactory.apply(partitions, dataSource.getPartitions().size());
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
  public FlinkElement<Batch.BatchWindow, T> nextRecord(
          FlinkElement<Batch.BatchWindow, T> reuse)
      throws IOException {
    return new FlinkElement<>(Batch.BatchWindow.get(), 0L, reader.next());
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override
  @SuppressWarnings("unchecked")
  public TypeInformation<T> getProducedType() {
    return TypeInformation.of((Class) FlinkElement.class);
  }
}
