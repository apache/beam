package cz.seznam.euphoria.flink.streaming.io;

import cz.seznam.euphoria.core.client.dataset.windowing.Batch;
import cz.seznam.euphoria.core.client.io.DataSource;
import cz.seznam.euphoria.core.client.io.Partition;
import cz.seznam.euphoria.core.client.io.Reader;
import cz.seznam.euphoria.flink.streaming.StreamingWindowedElement;
import cz.seznam.euphoria.guava.shaded.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

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
      // ~ execute the reader in a separate thread, such that
      // we can safely quit without being blocked by the reader
      // ~ here we specialize on the single reader scenario to
      // avoid needlessly synchronizing on the ctx as a lock
      Reader<T> reader = openReaders.get(0);
      executor = createThreadPool();
      Future<?> task = executor.submit(() -> {
        while (isRunning && reader.hasNext()) {
          ctx.collect(toWindowedElement(reader.next()));
        }
      });
      // ~ wait for the reader to finish or quit if request
      try {
        task.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    } else {
      // start a new thread for each reader
      executor = createThreadPool();
      Deque<Future> tasks = new ArrayDeque<>();
      for (Reader<T> reader : openReaders) {
        tasks.add(executor.submit(() -> {
          while (reader.hasNext()) {
            synchronized (ctx) {
              ctx.collect(toWindowedElement(reader.next()));
            }
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
