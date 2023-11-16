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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.core.serialization.Base64Serializer;
import org.apache.beam.runners.samza.SamzaPipelineOptions;
import org.apache.beam.runners.samza.metrics.FnWithMetricsWrapper;
import org.apache.beam.runners.samza.metrics.SamzaMetricsContainer;
import org.apache.beam.runners.samza.runtime.OpMessage;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Samza system that supports reading from a Beam {@link BoundedSource}. The source is treated as
 * though it has a single partition and does not support checkpointing via a changelog stream. If
 * the job is restarted the bounded source will be consumed from the beginning.
 */
// TODO: instrumentation for the consumer
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class BoundedSourceSystem {
  private static final Logger LOG = LoggerFactory.getLogger(BoundedSourceSystem.class);

  private static <T> List<BoundedSource<T>> split(
      BoundedSource<T> source, SamzaPipelineOptions pipelineOptions) throws Exception {
    final int numSplits = pipelineOptions.getMaxSourceParallelism();
    if (numSplits > 1) {
      final long estimatedSize = source.getEstimatedSizeBytes(pipelineOptions);
      // calculate the size of each split, rounded up to the ceiling.
      final long bundleSize = (estimatedSize + numSplits - 1) / numSplits;
      @SuppressWarnings("unchecked")
      final List<BoundedSource<T>> splits =
          (List<BoundedSource<T>>) source.split(bundleSize, pipelineOptions);
      // Need the empty check here because Samza doesn't handle empty partition well
      if (!splits.isEmpty()) {
        return splits;
      }
    }
    return Collections.singletonList(source);
  }

  /** A {@link SystemAdmin} for {@link BoundedSourceSystem}. */
  public static class Admin<T> implements SystemAdmin {
    private final BoundedSource<T> source;
    private final SamzaPipelineOptions pipelineOptions;

    public Admin(BoundedSource<T> source, SamzaPipelineOptions pipelineOptions) {
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
                      List<BoundedSource<T>> splits = split(source, pipelineOptions);
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
      if (offset1 == null) {
        return offset2 == null ? 0 : -1;
      }

      if (offset2 == null) {
        return 1;
      }

      return Long.valueOf(offset1).compareTo(Long.valueOf(offset2));
    }
  }

  /**
   * A {@link SystemConsumer} for a {@link BoundedSource}. See {@link BoundedSourceSystem} for more
   * details.
   */
  public static class Consumer<T> implements SystemConsumer {
    private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);
    private static final AtomicInteger NEXT_ID = new AtomicInteger();

    private final List<BoundedSource<T>> splits;
    private final SamzaPipelineOptions pipelineOptions;
    private final Map<BoundedReader<T>, SystemStreamPartition> readerToSsp = new HashMap<>();
    private final SamzaMetricsContainer metricsContainer;
    private final String stepName;

    private ReaderTask<T> readerTask;

    Consumer(
        BoundedSource<T> source,
        SamzaPipelineOptions pipelineOptions,
        SamzaMetricsContainer metricsContainer,
        String stepName) {
      try {
        splits = split(source, pipelineOptions);
      } catch (Exception e) {
        throw new SamzaException("Fail to split source", e);
      }
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

      final int capacity = pipelineOptions.getSystemBufferSize();
      final FnWithMetricsWrapper metricsWrapper =
          pipelineOptions.getEnableMetrics()
              ? new FnWithMetricsWrapper(metricsContainer, stepName)
              : null;
      readerTask = new ReaderTask<>(readerToSsp, capacity, metricsWrapper);
      final Thread thread =
          new Thread(readerTask, "bounded-source-system-consumer-" + NEXT_ID.getAndIncrement());
      thread.start();
    }

    @Override
    public void stop() {
      // NOTE: this is not a blocking shutdown
      if (readerTask != null) {
        readerTask.stop();
      }
    }

    @Override
    public void register(SystemStreamPartition ssp, String offset) {
      final int partitionId = ssp.getPartition().getPartitionId();
      try {
        final BoundedReader<T> reader = splits.get(partitionId).createReader(pipelineOptions);
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

    private static class ReaderTask<T> implements Runnable {
      private final Map<BoundedReader<T>, SystemStreamPartition> readerToSsp;
      private final Map<SystemStreamPartition, LinkedBlockingQueue<IncomingMessageEnvelope>> queues;
      private final Semaphore available;
      private final FnWithMetricsWrapper metricsWrapper;

      // NOTE: we do not support recovery with a bounded source (we restart from the beginning),
      // so we do not need to have a way to tie an offset to a position in the bounded source.
      private long offset;
      private volatile Thread readerThread;
      private volatile boolean stopInvoked = false;
      private volatile Exception lastException;

      private ReaderTask(
          Map<BoundedReader<T>, SystemStreamPartition> readerToSsp,
          int capacity,
          FnWithMetricsWrapper metricsWrapper) {
        this.readerToSsp = readerToSsp;
        this.available = new Semaphore(capacity);
        this.metricsWrapper = metricsWrapper;

        final Map<SystemStreamPartition, LinkedBlockingQueue<IncomingMessageEnvelope>> qs =
            new HashMap<>();
        readerToSsp.values().forEach(ssp -> qs.put(ssp, new LinkedBlockingQueue<>()));
        this.queues = ImmutableMap.copyOf(qs);
      }

      @Override
      public void run() {
        readerThread = Thread.currentThread();

        final Set<BoundedReader<T>> availableReaders = new HashSet<>(readerToSsp.keySet());
        try {
          for (BoundedReader<T> reader : readerToSsp.keySet()) {
            boolean hasData = invoke(reader::start);
            if (hasData) {
              enqueueMessage(reader);
            } else {
              enqueueMaxWatermarkAndEndOfStream(reader);
              reader.close();
              availableReaders.remove(reader);
            }
          }

          while (!stopInvoked && !availableReaders.isEmpty()) {
            final Iterator<BoundedReader<T>> iter = availableReaders.iterator();
            while (iter.hasNext()) {
              final BoundedReader<T> reader = iter.next();
              final boolean hasData = invoke(reader::advance);
              if (hasData) {
                enqueueMessage(reader);
              } else {
                enqueueMaxWatermarkAndEndOfStream(reader);
                reader.close();
                iter.remove();
              }
            }
          }
        } catch (InterruptedException e) {
          // We use an interrupt to wake the reader from a blocking read under normal termination,
          // so ignore it here.
        } catch (Exception e) {
          setError(e);
        } finally {
          availableReaders.forEach(
              reader -> {
                try {
                  reader.close();
                } catch (IOException e) {
                  LOG.error(
                      "Reader task failed to close reader for ssp {}", readerToSsp.get(reader), e);
                }
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

      private void enqueueMessage(BoundedReader<T> reader) throws InterruptedException {
        final T value = reader.getCurrent();
        final WindowedValue<T> windowedValue =
            WindowedValue.timestampedValueInGlobalWindow(value, reader.getCurrentTimestamp());
        final SystemStreamPartition ssp = readerToSsp.get(reader);
        final IncomingMessageEnvelope envelope =
            new IncomingMessageEnvelope(
                ssp, Long.toString(offset++), null, OpMessage.ofElement(windowedValue));

        available.acquire();
        queues.get(ssp).put(envelope);
      }

      private void enqueueMaxWatermarkAndEndOfStream(BoundedReader<T> reader) {
        final SystemStreamPartition ssp = readerToSsp.get(reader);
        // Send the max watermark to force completion of any open windows.
        final IncomingMessageEnvelope watermarkEnvelope =
            IncomingMessageEnvelope.buildWatermarkEnvelope(
                ssp, BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis());
        enqueueUninterruptibly(watermarkEnvelope);

        final IncomingMessageEnvelope endOfStreamEnvelope =
            IncomingMessageEnvelope.buildEndOfStreamEnvelope(ssp);
        enqueueUninterruptibly(endOfStreamEnvelope);
      }

      private void stop() {
        stopInvoked = true;

        final Thread readerThread = this.readerThread;
        if (readerThread != null) {
          readerThread.interrupt();
        }
      }

      private List<IncomingMessageEnvelope> getNextMessages(
          SystemStreamPartition ssp, long timeoutMillis) throws InterruptedException {
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

        available.release(envelopes.size());

        if (lastException != null) {
          throw new RuntimeException(lastException);
        }

        return envelopes;
      }

      private void setError(Exception exception) {
        this.lastException = exception;
        // A dummy message used to force the consumer to wake up immediately and check the
        // lastException field, which will be populated.
        readerToSsp
            .values()
            .forEach(
                ssp -> {
                  final IncomingMessageEnvelope checkLastExceptionEvelope =
                      new IncomingMessageEnvelope(ssp, null, null, null);
                  enqueueUninterruptibly(checkLastExceptionEvelope);
                });
      }

      private void enqueueUninterruptibly(IncomingMessageEnvelope envelope) {
        final BlockingQueue<IncomingMessageEnvelope> queue =
            queues.get(envelope.getSystemStreamPartition());
        while (true) {
          try {
            queue.put(envelope);
            return;
          } catch (InterruptedException e) {
            // Some events require that we post an envelope to the queue even if the interrupt
            // flag was set (i.e. during a call to stop) to ensure that the consumer properly
            // shuts down. Consequently, if we receive an interrupt here we ignore it and retry
            // the put operation.
          }
        }
      }
    }
  }

  /**
   * A {@link SystemFactory} that produces a {@link BoundedSourceSystem} for a particular {@link
   * BoundedSource} registered in {@link Config}.
   */
  public static class Factory<T> implements SystemFactory {
    @Override
    public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
      final String streamPrefix = "systems." + systemName;
      final Config scopedConfig = config.subset(streamPrefix + ".", true);

      return new Consumer<T>(
          getBoundedSource(scopedConfig),
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
      return new Admin<T>(getBoundedSource(scopedConfig), getPipelineOptions(config));
    }

    private static <T> BoundedSource<T> getBoundedSource(Config config) {
      @SuppressWarnings("unchecked")
      final BoundedSource<T> source =
          Base64Serializer.deserializeUnchecked(config.get("source"), BoundedSource.class);
      return source;
    }

    private static SamzaPipelineOptions getPipelineOptions(Config config) {
      return Base64Serializer.deserializeUnchecked(
              config.get("beamPipelineOptions"), SerializablePipelineOptions.class)
          .get()
          .as(SamzaPipelineOptions.class);
    }
  }
}
