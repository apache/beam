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

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Imitation of {@link SparkConsumer} that stores records into static {@link Queue}. Used to test
 * {@link SparkReceiverIO#read()}.
 */
@SuppressWarnings("unchecked")
public class CustomSparkConsumer<V> implements SparkConsumer<V> {

  private static final Logger LOG = LoggerFactory.getLogger(CustomSparkConsumer.class);

  private static final Queue<Object> queue = new ConcurrentLinkedQueue<>();
  private Receiver<V> sparkReceiver;

  @Override
  public V poll() {
    return (V) queue.poll();
  }

  @Override
  public void start(Receiver<V> sparkReceiver) {
    try {
      this.sparkReceiver = sparkReceiver;
      new WrappedSupervisor(
          sparkReceiver,
          new SparkConf(),
          objects -> {
            queue.offer(objects[0]);
            return null;
          });
      sparkReceiver.supervisor().startReceiver();
    } catch (Exception e) {
      LOG.error("Can not init Spark Receiver!", e);
    }
  }

  @Override
  public void stop() {
    queue.clear();
    sparkReceiver.stop("Stopped");
  }

  @Override
  public boolean hasRecords() {
    return !queue.isEmpty();
  }
}
