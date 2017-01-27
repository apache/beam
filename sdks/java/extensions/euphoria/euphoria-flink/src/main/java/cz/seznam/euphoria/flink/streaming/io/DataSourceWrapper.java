/**
 * Copyright 2016 Seznam a.s.
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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import cz.seznam.euphoria.core.client.dataset.windowing.Batch;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.Partition;
import cz.seznam.euphoria.core.client.io.Reader;
import cz.seznam.euphoria.flink.streaming.StreamingWindowedElement;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class DataSourceWrapper<T>
        extends RichParallelSourceFunction<StreamingWindowedElement<Batch.BatchWindow, T>>
        implements ResultTypeQueryable<StreamingWindowedElement<Batch.BatchWindow, T>>
{
  private final DataSource<T> dataSource;
  private volatile boolean isRunning = true;

  private volatile transient ThreadPoolExecutor executor;

  public DataSourceWrapper(DataSource<T> dataSource) {
    this.dataSource = dataSource;
  }

  @Override
  public void run(SourceContext<StreamingWindowedElement<Batch.BatchWindow, T>> ctx)
      throws Exception
  {
    StreamingRuntimeContext runtimeContext =
            (StreamingRuntimeContext) getRuntimeContext();

    final int subtaskIndex = runtimeContext.getIndexOfThisSubtask();
    final int totalSubtasks = runtimeContext.getNumberOfParallelSubtasks();

    List<Partition<T>> partitions = dataSource.getPartitions();
    List<Reader<T>> openReaders = new ArrayList<>();

    // find partitions which this data source is responsible for
    for (int i = 0; i < partitions.size(); i++) {
      if (i % totalSubtasks == subtaskIndex) {
        openReaders.add(partitions.get(i).openReader());
      }
    }

    if (openReaders.size() == 1) {
      try (Reader<T> reader = openReaders.get(0)) {
        while (isRunning && reader.hasNext()) {
          ctx.collect(toWindowedElement(reader.next()));
        }
      }
    } else {
      // start a new thread for each reader
      executor = createThreadPool();
      Deque<Future> tasks = new ArrayDeque<>();
      for (Reader<T> reader : openReaders) {
        tasks.add(executor.submit(() -> {
          try {
            while (reader.hasNext()) {
              synchronized (ctx) {
                ctx.collect(toWindowedElement(reader.next()));
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

  private StreamingWindowedElement<Batch.BatchWindow, T> toWindowedElement(T elem) {
    // assign ingestion timestamp to elements
    return new StreamingWindowedElement<>(Batch.BatchWindow.get(), System.currentTimeMillis(), elem);
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
  public TypeInformation<StreamingWindowedElement<Batch.BatchWindow, T>> getProducedType() {
    return TypeInformation.of((Class) StreamingWindowedElement.class);
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
}
