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
package org.apache.beam.sdk.io.solace.broker;

import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.io.solace.data.Solace;
import org.apache.beam.sdk.io.solace.data.Solace.PublishResult;
import org.apache.beam.sdk.io.solace.write.PublishResultsReceiver;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is required to handle callbacks from Solace, to find out if messages were actually
 * published or there were any kind of error.
 *
 * <p>This class is also used to calculate the latency of the publication. The correlation key
 * contains the original timestamp of when the message was sent from the pipeline to Solace. The
 * comparison of that value with the clock now, using a monotonic clock, is understood as the
 * latency of the publication
 */
public final class PublishResultHandler implements JCSMPStreamingPublishCorrelatingEventHandler {

  private static final Logger LOG = LoggerFactory.getLogger(PublishResultHandler.class);

  @Override
  public void handleErrorEx(Object key, JCSMPException cause, long timestamp) {
    processKey(key, false, cause);
  }

  @Override
  public void responseReceivedEx(Object key) {
    processKey(key, true, null);
  }

  private void processKey(Object key, boolean isPublished, @Nullable JCSMPException cause) {
    PublishResult.Builder resultBuilder = PublishResult.builder();
    String messageId;
    if (key == null) {
      messageId = "";
    } else if (key instanceof Solace.CorrelationKey) {
      messageId = ((Solace.CorrelationKey) key).getMessageId();
      long latencyMillis = calculateLatency((Solace.CorrelationKey) key);
      resultBuilder = resultBuilder.setLatencyMilliseconds(latencyMillis);
    } else {
      messageId = key.toString();
    }

    resultBuilder = resultBuilder.setMessageId(messageId).setPublished(isPublished);
    if (!isPublished) {
      if (cause != null) {
        resultBuilder = resultBuilder.setError(cause.getMessage());
      } else {
        resultBuilder = resultBuilder.setError("NULL - Not set by Solace");
      }
    } else if (cause != null) {
      LOG.warn(
          "Message with id {} is published but exception is populated. Ignoring exception",
          messageId);
    }

    PublishResult publishResult = resultBuilder.build();
    // Static reference, it receives all callbacks from all publications
    // from all threads
    PublishResultsReceiver.addResult(publishResult);
  }

  private static long calculateLatency(Solace.CorrelationKey key) {
    long currentMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime());
    long publishMillis = key.getPublishMonotonicMillis();
    return currentMillis - publishMillis;
  }
}
