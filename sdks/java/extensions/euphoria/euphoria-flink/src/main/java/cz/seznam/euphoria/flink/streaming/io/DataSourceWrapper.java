/*
 * Copyright 2016-2018 Seznam.cz, a.s.
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
package cz.seznam.euphoria.flink.streaming.io;

import cz.seznam.euphoria.core.client.dataset.windowing.GlobalWindowing;
import cz.seznam.euphoria.core.client.io.BoundedDataSource;
import cz.seznam.euphoria.core.client.io.CloseableIterator;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.UnboundedDataSource;
import cz.seznam.euphoria.core.client.io.UnboundedPartition;
import cz.seznam.euphoria.core.client.io.UnboundedReader;
import cz.seznam.euphoria.flink.streaming.StreamingElement;
import cz.seznam.euphoria.shadow.com.google.common.collect.Iterables;
import cz.seznam.euphoria.shadow.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class DataSourceWrapper<T>
    extends RichParallelSourceFunction<StreamingElement<GlobalWindowing.Window, T>>
    implements ResultTypeQueryable<StreamingElement<GlobalWindowing.Window, T>>,
        CheckpointedFunction {

  /** The stored (committed) state of the reader. */
  private static class SourceState<OFFSET> implements Serializable {

    /** Number of readers. */
    final int numReaders;

    /** Offset committed. */
    final List<OFFSET> offsets;

    public int numReaders() {
      return numReaders;
    }

    public List<OFFSET> getOffset() {
      return offsets;
    }

    SourceState(int numReaders, List<OFFSET> offsets) {
      this.numReaders = numReaders;
      this.offsets = offsets;
    }

  }

  @SuppressWarnings("rawtypes")
  private final List<UnboundedReader> unboundedReaders = new ArrayList<>();
  private final String storageName;
  private final DataSource<T> dataSource;
  private ListState<SourceState> snapshotState;

  private volatile boolean isRunning = true;

  private volatile transient ThreadPoolExecutor executor;

  public DataSourceWrapper(String storageName, DataSource<T> dataSource) {
    this.storageName = storageName;
    this.dataSource = dataSource;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void run(
      SourceContext<StreamingElement<GlobalWindowing.Window, T>> ctx)
      throws Exception {

    if (dataSource.isBounded()) {
      runBounded(ctx);
    } else {
      runUnbounded(ctx);
    }
  }

  private void runBounded(
      SourceContext<StreamingElement<GlobalWindowing.Window, T>> ctx)
      throws Exception {

    BoundedDataSource<T> boundedSource = dataSource.asBounded();
    StreamingRuntimeContext runtimeContext =
            (StreamingRuntimeContext) getRuntimeContext();

    final int totalSubtasks = runtimeContext.getNumberOfParallelSubtasks();

    List<BoundedDataSource<T>> partitions = boundedSource
        .split(totalSubtasks);

    runInternal(
        ctx,
        partitions.stream().map(p -> {
          try {
            return p.openReader();
          } catch (IOException ex) {
            throw new RuntimeException(ex);
          }
        }).collect(Collectors.toList()),
        e -> ctx.collect(toStreamingElement(e)));
  }

  @SuppressWarnings("unchecked")
  private void runUnbounded(
      SourceContext<StreamingElement<GlobalWindowing.Window, T>> ctx)
      throws Exception {

    // the readers have already been initialized by {@link initializeState}
    runInternal(
        ctx,
        unboundedReaders,
        e -> produceElement(ctx, e));
  }

  @SuppressWarnings("unchecked")
  private void runInternal(
      SourceContext<StreamingElement<GlobalWindowing.Window, T>> ctx,
      List<? extends CloseableIterator> inputs,
      Consumer<T> emit) throws Exception {

    if (inputs.size() == 1) {
      try (CloseableIterator<T> reader = inputs.get(0)) {
        while (isRunning && reader.hasNext()) {
          emit.accept(reader.next());
        }
      }
    } else {
      executor = createThreadPool();
      // start a new thread for each reader
      Deque<Future> tasks = new ArrayDeque<>();
      int pos = 0;
      for (CloseableIterator<T> reader : inputs) {
        final int id = pos++;
        tasks.add(executor.submit(() -> {
          try {
            while (reader.hasNext()) {
              synchronized (ctx) {
                emit.accept(reader.next());
              }
            }
            return null;
          } finally {
            reader.close();
          }
        }));
      }
      // wait for all task to finish
      while (isRunning && !tasks.isEmpty()) {
        try {
          tasks.peek().get();
          tasks.poll();
        } catch (InterruptedException e) {
          if (!isRunning) {
            // restore the interrupted state, and fall through the loop
            Thread.currentThread().interrupt();
          }
        }
      }
    }

  }

  /**
   * Produce element to output from given reader.
   */
  private void produceElement(
      SourceContext<StreamingElement<GlobalWindowing.Window, T>> ctx,
      T elem) {

    synchronized (ctx.getCheckpointLock()) {
      ctx.collect(toStreamingElement(elem));
    }
  }

  private void initializeReaders(UnboundedDataSource<T, ?> source) throws IOException {
    StreamingRuntimeContext runtimeContext =
            (StreamingRuntimeContext) getRuntimeContext();

    final int subtaskIndex = runtimeContext.getIndexOfThisSubtask();
    final int totalSubtasks = runtimeContext.getNumberOfParallelSubtasks();

    List<? extends UnboundedPartition<T, ?>> partitions = source.getPartitions();

    // find partitions which this data source is responsible for
    for (int i = 0; i < partitions.size(); i++) {
      if (i % totalSubtasks == subtaskIndex) {
        unboundedReaders.add(partitions.get(i).openReader());
      }
    }

  }


  private StreamingElement<GlobalWindowing.Window, T> toStreamingElement(T elem) {
    // assign ingestion timestamp to elements
    return new StreamingElement<>(GlobalWindowing.Window.get(), elem);
  }

  @Override
  public void cancel() {
    if (executor != null) {
      executor.shutdownNow();
    }
    this.isRunning = false;
  }

  @Override
  @SuppressWarnings("unchecked")
  public TypeInformation<StreamingElement<GlobalWindowing.Window, T>> getProducedType() {
    return TypeInformation.of((Class) StreamingElement.class);
  }

  private ThreadPoolExecutor createThreadPool() {
    return new ThreadPoolExecutor(
        0, Integer.MAX_VALUE,
        60,
        TimeUnit.SECONDS,
        new LinkedBlockingDeque<>(),
        new ThreadFactoryBuilder()
            .setNameFormat("DataSource-%d")
            .setDaemon(true)
            .setUncaughtExceptionHandler((Thread t, Throwable e) -> {
              e.printStackTrace(System.err);
            })
            .build());
  }

  @SuppressWarnings("unchecked")
  @Override
  public void snapshotState(FunctionSnapshotContext fsc) throws Exception {
    if (!dataSource.isBounded()) {
      List<Object> offsets = new ArrayList<>(this.unboundedReaders.size());
      for (UnboundedReader<T, Object> r : unboundedReaders) {
        Object off = r.getCurrentOffset();
        offsets.add(off);
        r.commitOffset(off);
      }
      snapshotState.clear();
      snapshotState.add(new SourceState<>(this.unboundedReaders.size(), offsets));
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void initializeState(FunctionInitializationContext fic) throws Exception {
    if (!dataSource.isBounded()) {
      initializeReaders(this.dataSource.asUnbounded());
      snapshotState = fic.getOperatorStateStore().getSerializableListState(storageName);
      SourceState state = Iterables.getOnlyElement(snapshotState.get(), null);
      if (state != null) {
        int readerId = 0;
        for (UnboundedReader<T, Object> r : this.unboundedReaders) {
          r.reset(state.getOffset().get(readerId++));
        }
      }
    }
  }
}
