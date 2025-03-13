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

import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.apache.spark.streaming.receiver.ReceiverSupervisor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test class for {@link ReceiverBuilder}. */
@RunWith(JUnit4.class)
public class ReceiverBuilderTest {

  private static final Logger LOG = LoggerFactory.getLogger(ReceiverBuilderTest.class);

  public static final String TEST_MESSAGE = "testMessage";

  private static class CustomReceiver extends Receiver<String> {

    public CustomReceiver(StorageLevel storageLevel) {
      super(storageLevel);
    }

    @Override
    public void onStart() {
      LOG.info("Receiver onStart()");
    }

    @Override
    public void onStop() {
      LOG.info("Receiver onStop()");
    }
  }

  /**
   * If this test passed, then object for Custom {@link
   * org.apache.spark.streaming.receiver.Receiver} was created successfully, and the corresponding
   * {@link ReceiverSupervisor} was wrapped into {@link WrappedSupervisor}.
   */
  @Test
  public void testCreatingCustomSparkReceiver() {
    try {

      AtomicBoolean customStoreConsumerWasUsed = new AtomicBoolean(false);
      ReceiverBuilder<String, CustomReceiver> receiverBuilder =
          new ReceiverBuilder<>(CustomReceiver.class);
      Receiver<String> receiver =
          receiverBuilder.withConstructorArgs(StorageLevel.DISK_ONLY()).build();
      new WrappedSupervisor(
          receiver,
          new SparkConf(),
          args -> {
            customStoreConsumerWasUsed.set(true);
            return null;
          });

      receiver.onStart();
      assertTrue(receiver.supervisor() instanceof WrappedSupervisor);

      receiver.store(TEST_MESSAGE);
      assertTrue(customStoreConsumerWasUsed.get());
    } catch (Exception e) {
      LOG.error("Can not get receiver", e);
    }
  }
}
