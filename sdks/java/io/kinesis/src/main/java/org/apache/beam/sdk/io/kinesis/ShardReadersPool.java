package org.apache.beam.sdk.io.kinesis;

import com.google.common.base.Function;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal shard iterators pool.
 * It maintains the thread pool for reading Kinesis shards in separate threads.
 * Read records are stored in a blocking queue of limited capacity.
 */
class ShardReadersPool {

  private static final Logger LOG = LoggerFactory.getLogger(ShardReadersPool.class);
  private static final int DEFAULT_CAPACITY_PER_SHARD = 10_000;
  private ExecutorService executorService;
  private BlockingQueue<KinesisRecord> recordsQueue;
  private Map<String, ShardRecordsIterator> shardIteratorsMap;
  private final int queueCapacity;
  private volatile boolean poolOpened = true;

  ShardReadersPool(List<ShardRecordsIterator> iterators) {
    this(iterators, DEFAULT_CAPACITY_PER_SHARD);
  }

  ShardReadersPool(List<ShardRecordsIterator> iterators, int queueCapacity) {
    this.shardIteratorsMap = Maps
        .uniqueIndex(iterators, new Function<ShardRecordsIterator, String>() {

          @Override
          public String apply(ShardRecordsIterator input) {
            return input.getShardId();
          }
        });
    this.queueCapacity = queueCapacity;
  }

  void start() {
    executorService = Executors.newFixedThreadPool(shardIteratorsMap.size());
    recordsQueue = new LinkedBlockingQueue<>(queueCapacity * shardIteratorsMap.size());
    for (final ShardRecordsIterator shardRecordsIterator : shardIteratorsMap.values()) {
      executorService.submit(new Runnable() {

        @Override
        public void run() {
          readLoop(shardRecordsIterator);
        }
      });
    }
  }

  private void readLoop(ShardRecordsIterator shardRecordsIterator) {
    while (poolOpened) {
      try {
        List<KinesisRecord> kinesisRecords = shardRecordsIterator.readNextBatch();
        for (KinesisRecord kinesisRecord : kinesisRecords) {
          recordsQueue.put(kinesisRecord);
        }
      } catch (TransientKinesisException e) {
        LOG.warn("Transient exception occurred.", e);
      } catch (InterruptedException e) {
        LOG.warn("Thread was interrupted, finishing the read loop", e);
        break;
      } catch (Throwable e) {
        LOG.error("Unexpected exception occurred", e);
      }
    }
    LOG.info("Kinesis Shard read loop has finished");
  }

  CustomOptional<KinesisRecord> nextRecord() {
    try {
      KinesisRecord record = recordsQueue.poll(1, TimeUnit.SECONDS);
      if (record == null) {
        return CustomOptional.absent();
      }
      shardIteratorsMap.get(record.getShardId()).ackRecord(record);
      return CustomOptional.of(record);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while waiting for KinesisRecord from the buffer");
      return CustomOptional.absent();
    }
  }

  void stop() {
    LOG.info("Closing shard iterators pool");
    poolOpened = false;
    executorService.shutdownNow();
    boolean isShutdown = false;
    while (!isShutdown) {
      try {
        isShutdown = executorService.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        LOG.error("Interrupted while waiting for the executor service to shutdown");
        throw new RuntimeException(e);
      }
      LOG.warn("Executor service is taking long time to shutdown, will retry");
    }
  }

  boolean allShardsUpToDate() {
    boolean shardsUpToDate = true;
    for (ShardRecordsIterator shardRecordsIterator : shardIteratorsMap.values()) {
      shardsUpToDate &= shardRecordsIterator.isUpToDate();
    }
    return shardsUpToDate;
  }

  Collection<ShardRecordsIterator> getShardRecordsIterators() {
    return shardIteratorsMap.values();
  }

}
