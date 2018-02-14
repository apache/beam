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
package org.apache.beam.sdk.io.kinesis;

import static com.google.common.collect.Lists.newArrayList;

import com.amazonaws.services.kinesis.producer.IKinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.Metric;
import com.amazonaws.services.kinesis.producer.UserRecord;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.joda.time.DateTime;

/**
 * Simple mock implementation of {@link IKinesisProducer} for testing.
 */
public class KinesisProducerMock implements IKinesisProducer {

  private boolean isFailedFlush = false;

  private List<UserRecord> addedRecords = newArrayList();

  private KinesisServiceMock kinesisService = KinesisServiceMock.getInstance();

  public KinesisProducerMock(){}

  public KinesisProducerMock(KinesisProducerConfiguration config, boolean isFailedFlush) {
    this.isFailedFlush = isFailedFlush;
  }

  @Override public ListenableFuture<UserRecordResult> addUserRecord(String stream,
      String partitionKey, ByteBuffer data) {
    throw new RuntimeException("Not implemented");
  }

  @Override public ListenableFuture<UserRecordResult> addUserRecord(UserRecord userRecord) {
    throw new RuntimeException("Not implemented");
  }

  @Override public ListenableFuture<UserRecordResult> addUserRecord(String stream,
      String partitionKey, String explicitHashKey, ByteBuffer data) {
    SettableFuture<UserRecordResult> f = SettableFuture.create();
    if (kinesisService.getExistedStream().equals(stream)) {
      addedRecords.add(new UserRecord(stream, partitionKey, explicitHashKey, data));
    }
    return f;
  }

  @Override
  public int getOutstandingRecordsCount() {
    return addedRecords.size();
  }

  @Override public List<Metric> getMetrics(String metricName, int windowSeconds)
      throws InterruptedException, ExecutionException {
    throw new RuntimeException("Not implemented");
  }

  @Override public List<Metric> getMetrics(String metricName)
      throws InterruptedException, ExecutionException {
    throw new RuntimeException("Not implemented");
  }

  @Override public List<Metric> getMetrics() throws InterruptedException, ExecutionException {
    throw new RuntimeException("Not implemented");
  }

  @Override public List<Metric> getMetrics(int windowSeconds)
      throws InterruptedException, ExecutionException {
    throw new RuntimeException("Not implemented");
  }

  @Override public void destroy() {
  }

  @Override public void flush(String stream) {
    throw new RuntimeException("Not implemented");
  }

  @Override public void flush() {
    if (isFailedFlush) {
      // don't flush
      return;
    }

    DateTime arrival = DateTime.now();
    for (int i = 0; i < addedRecords.size(); i++) {
      UserRecord record = addedRecords.get(i);
      arrival = arrival.plusSeconds(1);
      kinesisService.addShardedData(record.getData(), arrival);
      addedRecords.remove(i);
    }
  }

  @Override public synchronized void flushSync() {
    if (getOutstandingRecordsCount() > 0) {
      flush();
    }
  }
}
