/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.samza.adapter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.core.serialization.Base64Serializer;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.runners.samza.metrics.FnWithMetricsWrapper;
import org.apache.beam.runners.samza.metrics.SamzaMetricsContainer;
import org.apache.beam.runners.samza.runtime.OpMessage;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.io.UnboundedSource.CheckpointMark;
import org.apache.beam.sdk.io.UnboundedSource.UnboundedReader;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Samza system that supports reading from a Beam {@link UnboundedSource}. The source is split
 * into partitions. Samza creates the job model by assigning partitions to Samza tasks.
 */
public class UnboundedSourceSystem {
  private static final Logger LOG = LoggerFactory.getLogger(UnboundedSourceSystem.class);

  // A dummy message used to force the consumer to wake up immediately and check the
  // lastException field, which will be populated.
  private static final IncomingMessageEnvelope CHECK_LAST_EXCEPTION_ENVELOPE =
      new IncomingMessageEnvelope(null, null, null, null);

  /**
   * For better parallelism in Samza, we need to configure a large split number for {@link
   * UnboundedSource} like Kafka. This will most likely make each split contain a single partition,
   * and be assigned to a Samza task. A large split number is safe since the actual split is bounded
   * by the number of source partitions.
   */
  private static <T, CheckpointMarkT extends CheckpointMark>
      List<UnboundedSource<T, CheckpointMarkT>> split(
          UnboundedSource<T, CheckpointMarkT> source, SamzaPipelineOptions pipelineOptions)
          throws Exception {
    final int numSplits = pipelineOptions.getMaxSourceParallelism();
    if (numSplits > 1) {
      @SuppressWarnings("unchecked")
      final List<UnboundedSource<T, CheckpointMarkT>> splits =
          (List<UnboundedSource<T, CheckpointMarkT>>) source.split(numSplits, pipelineOptions);
      // Need the empty check here because Samza doesn't handle empty partition well
      if (!splits.isEmpty()) {
        return splits;
      }
    }
    return Collections.singletonList(source);
  }

  /** A {@link SystemAdmin} for {@link UnboundedSourceSystem}. */
  public static class Admin<T, CheckpointMarkT extends CheckpointMark> implements SystemAdmin {
    private final UnboundedSource<T, CheckpointMarkT> source;
    private final SamzaPipelineOptions pipelineOptions;

    public Admin(UnboundedSource<T, CheckpointMarkT> source, SamzaPipelineOptions pipelineOptions) {
      this.source = source;
      this.pipelineOptions = pipelineOptions;
    }

    @Override
    public Map<SystemStreamPartition, String> getOffsetsAfter(
        Map<SystemStreamPartition, String> offsets) {
      // BEAM checkpoints the next offset so here we just need to return the map itself
      return offsets;
    }

    @Override
    public Map<String, SystemStreamMetadata> getSystemStreamMetadata(Set<String> streamNames) {
      return streamNames.stream()
          .collect(
              Collectors.toMap(
                  Function.<String>identity(),
                  streamName -> {
                    try {
                      final List<UnboundedSource<T, CheckpointMarkT>> splits =
                          split(source, pipelineOptions);
                      final Map<Partition, SystemStreamPartitionMetadata> partitionMetaData =
                          new HashMap<>();
                      // we assume that the generated splits are stable,
                      // this is necessary so that the mapping of partition to source is correct
                      // in each container.
                      for (int i = 0; i < splits.size(); i++) {
                        partitionMetaData.put(
                            new Partition(i), new SystemStreamPartitionMetadata(null, null, null));
                      }
                      return new SystemStreamMetadata(streamName, partitionMetaData);
                    } catch (Exception e) {
                      throw new SamzaException("Fail to read stream metadata", e);
                    }
                  }));
    }

    @Override
    public Integer offsetComparator(String offset1, String offset2) {
      // BEAM will fetch the exact offset. So we don't need to compare them.
      // Return null indicating it's caught up.
      return null;
    }
  }

  /**
   * A {@link SystemConsumer} for a {@link UnboundedSource}. See {@link UnboundedSourceSystem} for
   * more details.
   */
  public static class Consumer<T, CheckpointMarkT extends CheckpointMark>
      implements SystemConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);

    private static final AtomicInteger NEXT_ID = new AtomicInteger();

    private final Coder<CheckpointMarkT> checkpointMarkCoder;
    private final List<UnboundedSource<T, CheckpointMarkT>> splits;
    private final SamzaPipelineOptions pipelineOptions;
    private final Map<UnboundedReader, SystemStreamPartition> readerToSsp = new HashMap<>();
    private final SamzaMetricsContainer metricsContainer;
    private final String stepName;

    private ReaderTask<T, CheckpointMarkT> readerTask;

    Consumer(
        UnboundedSource<T, CheckpointMarkT> source,
        SamzaPipelineOptions pipelineOptions,
        SamzaMetricsContainer metricsContainer,
        String stepName) {
      try {
        this.splits = split(source, pipelineOptions);
      } catch (Exception e) {
        throw new SamzaException("Fail to split source", e);
      }
      this.checkpointMarkCoder = source.getCheckpointMarkCoder();
      this.pipelineOptions = pipelineOptions;
      this.metricsContainer = metricsContainer;
      this.stepName = stepName;
    }

    @Override
    public void start() {
      if (this.readerToSsp.isEmpty()) {
        throw new IllegalArgumentException(
            "Attempted to call start without assigned system stream partitions");
      }

      final FnWithMetricsWrapper metricsWrapper =
          pipelineOptions.getEnableMetrics()
              ? new FnWithMetricsWrapper(metricsContainer, stepName)
              : null;
      readerTask =
          new ReaderTask<>(
              readerToSsp,
              checkpointMarkCoder,
              pipelineOptions.getSystemBufferSize(),
              pipelineOptions.getWatermarkInterval(),
              metricsWrapper);
      final Thread thread =
          new Thread(readerTask, "unbounded-source-system-consumer-" + NEXT_ID.getAndIncrement());
      thread.start();
    }

    @Override
    public void stop() {
      // NOTE: this is not a blocking shutdown
      readerTask.stop();
    }

    @Override
    public void register(SystemStreamPartition ssp, String offset) {
      CheckpointMarkT checkpoint = null;
      if (StringUtils.isNoneEmpty(offset)) {
        final byte[] offsetBytes = Base64.getDecoder().decode(offset);
        final ByteArrayInputStream bais = new ByteArrayInputStream(offsetBytes);
        try {
          checkpoint = checkpointMarkCoder.decode(bais);
        } catch (Exception e) {
          throw new SamzaException("Error in decode offset", e);
        }
      }

      // Create unbounded reader with checkpoint
      final int partitionId = ssp.getPartition().getPartitionId();
      try {
        final UnboundedReader reader =
            splits.get(partitionId).createReader(pipelineOptions, checkpoint);
        readerToSsp.put(reader, ssp);
      } catch (Exception e) {
        throw new SamzaException("Error while creating source reader for ssp: " + ssp, e);
      }
    }

    @Override
    public Map<SystemStreamPartition, List<IncomingMessageEnvelope>> poll(
        Set<SystemStreamPartition> systemStreamPartitions, long timeout)
        throws InterruptedException {
      assert !readerToSsp.isEmpty(); // start should be called before poll

      final Map<SystemStreamPartition, List<IncomingMessageEnvelope>> envelopes = new HashMap<>();
      for (SystemStreamPartition ssp : systemStreamPartitions) {
        envelopes.put(ssp, readerTask.getNextMessages(ssp, timeout));
      }
      return envelopes;
    }

    private static class ReaderTask<T, CheckpointMarkT extends CheckpointMark> implements Runnable {
      private final Map<UnboundedReader, SystemStreamPartition> readerToSsp;
      private final List<UnboundedReader> readers;
      private final Coder<CheckpointMarkT> checkpointMarkCoder;
      private final Map<SystemStreamPartition, Instant> currentWatermarks = new HashMap<>();
      private final Map<SystemStreamPartition, LinkedBlockingQueue<IncomingMessageEnvelope>> queues;
      private final long watermarkInterval;
      private final Semaphore available;
      private final FnWithMetricsWrapper metricsWrapper;

      private volatile boolean running;
      private volatile Exception lastException;
      private long lastWatermarkTime = 0L;

      private ReaderTask(
          Map<UnboundedReader, SystemStreamPartition> readerToSsp,
          Coder<CheckpointMarkT> checkpointMarkCoder,
          int capacity,
          long watermarkInterval,
          FnWithMetricsWrapper metricsWrapper) {
        this.readerToSsp = readerToSsp;
        this.checkpointMarkCoder = checkpointMarkCoder;
        this.readers = ImmutableList.copyOf(readerToSsp.keySet());
        this.watermarkInterval = watermarkInterval;
        this.available = new Semaphore(capacity);
        this.metricsWrapper = metricsWrapper;

        final Map<SystemStreamPartition, LinkedBlockingQueue<IncomingMessageEnvelope>> qs =
            new HashMap<>();
        readerToSsp.values().forEach(ssp -> qs.put(ssp, new LinkedBlockingQueue<>()));
        this.queues = ImmutableMap.copyOf(qs);
      }

      @Override
      public void run() {
        this.running = true;

        try {
          for (UnboundedReader reader : readers) {
            final boolean hasData = invoke(reader::start);
            if (hasData) {
              available.acquire();
              enqueueMessage(reader);
            }
          }

          while (running) {
            boolean elementAvailable = false;
            for (UnboundedReader reader : readers) {
              final boolean hasData = invoke(reader::advance);
              if (hasData) {
                while (!available.tryAcquire(
                    1,
                    Math.max(lastWatermarkTime + watermarkInterval - System.currentTimeMillis(), 1),
                    TimeUnit.MILLISECONDS)) {
                  updateWatermark();
                }
                enqueueMessage(reader);
                elementAvailable = true;
              }
            }

            updateWatermark();

            if (!elementAvailable) {
              // TODO: make poll interval configurable
              Thread.sleep(50);
            }
          }
        } catch (Exception e) {
          lastException = e;
          running = false;
        } finally {
          readers.forEach(
              reader -> {
                try {
                  reader.close();
                } catch (IOException e) {
                  LOG.error("Reader task failed to close reader", e);
                }
              });
        }

        if (lastException != null) {
          // Force any pollers to wake up
          queues
              .values()
              .forEach(
                  queue -> {
                    queue.clear();
                    queue.add(CHECK_LAST_EXCEPTION_ENVELOPE);
                  });
        }
      }

      private <X> X invoke(FnWithMetricsWrapper.SupplierWithException<X> fn) throws Exception {
        if (metricsWrapper != null) {
          return metricsWrapper.wrap(fn, true);
        } else {
          return fn.get();
        }
      }

      private void updateWatermark() throws InterruptedException {
        final long time = System.currentTimeMillis();
        if (time - lastWatermarkTime > watermarkInterval) {
          for (UnboundedReader reader : readers) {
            final SystemStreamPartition ssp = readerToSsp.get(reader);
            final Instant currentWatermark =
                currentWatermarks.containsKey(ssp)
                    ? currentWatermarks.get(ssp)
                    : BoundedWindow.TIMESTAMP_MIN_VALUE;
            final Instant nextWatermark = reader.getWatermark();
            if (currentWatermark.isBefore(nextWatermark)) {
              currentWatermarks.put(ssp, nextWatermark);
              enqueueWatermark(reader);
            }
          }

          lastWatermarkTime = time;
        }
      }

      private void enqueueWatermark(UnboundedReader reader) throws InterruptedException {
        final SystemStreamPartition ssp = readerToSsp.get(reader);
        final IncomingMessageEnvelope envelope =
            IncomingMessageEnvelope.buildWatermarkEnvelope(ssp, reader.getWatermark().getMillis());

        queues.get(ssp).put(envelope);
      }

      private void enqueueMessage(UnboundedReader reader) throws InterruptedException {
        @SuppressWarnings("unchecked")
        final T value = (T) reader.getCurrent();
        final Instant time = reader.getCurrentTimestamp();
        final SystemStreamPartition ssp = readerToSsp.get(reader);
        final WindowedValue<T> windowedValue =
            WindowedValue.timestampedValueInGlobalWindow(value, time);

        final OpMessage<T> opMessage = OpMessage.ofElement(windowedValue);
        final IncomingMessageEnvelope envelope =
            new IncomingMessageEnvelope(ssp, getOffset(reader), null, opMessage);

        queues.get(ssp).put(envelope);
      }

      void stop() {
        running = false;
      }

      List<IncomingMessageEnvelope> getNextMessages(SystemStreamPartition ssp, long timeoutMillis)
          throws InterruptedException {
        if (lastException != null) {
          throw new RuntimeException(lastException);
        }

        final List<IncomingMessageEnvelope> envelopes = new ArrayList<>();
        final BlockingQueue<IncomingMessageEnvelope> queue = queues.get(ssp);
        final IncomingMessageEnvelope envelope = queue.poll(timeoutMillis, TimeUnit.MILLISECONDS);

        if (envelope != null) {
          envelopes.add(envelope);
          queue.drainTo(envelopes);
        }

        final int numElements =
            (int) envelopes.stream().filter(ev -> (ev.getMessage() instanceof OpMessage)).count();
        available.release(numElements);

        if (lastException != null) {
          throw new RuntimeException(lastException);
        }

        return envelopes;
      }

      private String getOffset(UnboundedReader reader) {
        try {
          final ByteArrayOutputStream baos = new ByteArrayOutputStream();
          @SuppressWarnings("unchecked")
          final CheckpointMarkT checkpointMark =
              (CheckpointMarkT) invoke(reader::getCheckpointMark);
          checkpointMarkCoder.encode(checkpointMark, baos);
          return Base64.getEncoder().encodeToString(baos.toByteArray());
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  /**
   * A {@link SystemFactory} that produces a {@link UnboundedSourceSystem} for a particular {@link
   * UnboundedSource} registered in {@link Config}.
   */
  public static class Factory<T, CheckpointMarkT extends CheckpointMark> implements SystemFactory {
    @Override
    public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
      final String streamPrefix = "systems." + systemName;
      final Config scopedConfig = config.subset(streamPrefix + ".", true);
      return new Consumer<T, CheckpointMarkT>(
          getUnboundedSource(scopedConfig),
          getPipelineOptions(config),
          new SamzaMetricsContainer((MetricsRegistryMap) registry),
          scopedConfig.get("stepName"));
    }

    @Override
    public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
      LOG.info("System " + systemName + " does not have producer.");
      return null;
    }

    @Override
    public SystemAdmin getAdmin(String systemName, Config config) {
      final Config scopedConfig = config.subset("systems." + systemName + ".", true);
      return new Admin<T, CheckpointMarkT>(
          getUnboundedSource(scopedConfig), getPipelineOptions(config));
    }

    private static <T, CheckpointMarkT extends CheckpointMark>
        UnboundedSource<T, CheckpointMarkT> getUnboundedSource(Config config) {
      @SuppressWarnings("unchecked")
      final UnboundedSource<T, CheckpointMarkT> source =
          Base64Serializer.deserializeUnchecked(config.get("source"), UnboundedSource.class);
      return source;
    }

    @SuppressWarnings("unchecked")
    private static <T> Coder<WindowedValue<T>> getCoder(Config config) {
      return Base64Serializer.deserializeUnchecked(config.get("coder"), Coder.class);
    }

    private static SamzaPipelineOptions getPipelineOptions(Config config) {
      return Base64Serializer.deserializeUnchecked(
              config.get("beamPipelineOptions"), SerializablePipelineOptions.class)
          .get()
          .as(SamzaPipelineOptions.class);
    }
  }
}
