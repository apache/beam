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
package org.apache.beam.sdk.io.sparkreceiver;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Imitation of Spark {@link Receiver} that implements {@link HasOffset} interface. Used to test
 * {@link SparkReceiverIO#read()}.
 */
public class CustomReceiverWithOffset extends Receiver<String> implements HasOffset {

  private static final Logger LOG = LoggerFactory.getLogger(CustomReceiverWithOffset.class);
  private static final int TIMEOUT_MS = 500;
  public static final int RECORDS_COUNT = 20;

  /*
   Used in test for imitation of reading with exception
  */
  public static boolean shouldFailInTheMiddle = false;

  private Long startOffset;

  CustomReceiverWithOffset() {
    super(StorageLevel.MEMORY_AND_DISK_2());
  }

  @Override
  public void setStartOffset(Long startOffset) {
    if (startOffset != null) {
      this.startOffset = startOffset;
    }
  }

  @Override
  @SuppressWarnings("FutureReturnValueIgnored")
  public void onStart() {
    Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().build()).submit(this::receive);
  }

  @Override
  public void onStop() {}

  @Override
  public Long getEndOffset() {
    return Long.MAX_VALUE;
  }

  private void receive() {
    Long currentOffset = startOffset;
    while (!isStopped()) {
      if (currentOffset < RECORDS_COUNT) {
        if (shouldFailInTheMiddle && currentOffset == RECORDS_COUNT / 2) {
          shouldFailInTheMiddle = false;
          LOG.debug("Expected fail in the middle of reading");
          throw new IllegalStateException("Expected exception");
        }
        store(String.valueOf(currentOffset));
        currentOffset++;
      } else {
        break;
      }
      try {
        TimeUnit.MILLISECONDS.sleep(TIMEOUT_MS);
      } catch (InterruptedException e) {
        LOG.error("Interrupted", e);
      }
    }
  }
}
