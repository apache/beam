/*
 * Copyright 2019 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.beam.sdk.io.cdap.hubspot.source.streaming;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.beam.sdk.io.cdap.hubspot.common.HubspotHelper;
import org.apache.beam.sdk.io.cdap.hubspot.common.HubspotPage;
import org.apache.beam.sdk.io.cdap.hubspot.common.HubspotPagesIterator;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of Spark receiver to receive Salesforce push topic events.
 */
@SuppressWarnings("FutureReturnValueIgnored")
public class HubspotReceiver extends Receiver<String> {
  private static final Logger LOG = LoggerFactory.getLogger(HubspotReceiver.class);
  private static final String RECEIVER_THREAD_NAME = "hubspot_api_listener";
  private final HubspotStreamingSourceConfig config;

  HubspotReceiver(HubspotStreamingSourceConfig config) throws IOException {
    super(StorageLevel.MEMORY_AND_DISK_2());
    this.config = config;
  }

  @Override
  public void onStart() {
    ThreadFactory namedThreadFactory = new ThreadFactoryBuilder()
      .setNameFormat(RECEIVER_THREAD_NAME + "-%d")
      .build();

    Executors.newSingleThreadExecutor(namedThreadFactory).submit(this::receive);
  }

  @Override
  public void onStop() {
    // There is nothing we can do here as the thread calling receive()
    // is designed to stop by itself if isStopped() returns false
  }

  private void receive() {
    try {
      HubspotPagesIterator hubspotPagesIterator = new HubspotPagesIterator(config);

      while (!isStopped()) {
        if (hubspotPagesIterator.hasNext()) {
          store(hubspotPagesIterator.next().toString());
        } else {
          Integer minutesToSleep = config.getPullFrequency().getMinutesValue();
          LOG.debug(String.format("Waiting for '%d' minutes to pull.", minutesToSleep));
          Thread.sleep(TimeUnit.MINUTES.toMillis(minutesToSleep));

          // reload current page
          HubspotPage currentPage = new HubspotHelper().getHubspotPage(config,
                                                                       hubspotPagesIterator.getCurrentPageOffset());
          int iteratorPosition = hubspotPagesIterator.getIteratorPosition();

          hubspotPagesIterator = new HubspotPagesIterator(config, currentPage,
                                                          hubspotPagesIterator.getCurrentPageOffset());
          hubspotPagesIterator.setIteratorPosition(iteratorPosition);
        }
      }
    } catch (Exception e) {
      String errorMessage = "Exception while receiving messages from hubspot";
      /* TO DO  https://issues.cask.co/browse/PLUGIN-357
      The receiver will get terminated on error and stop receiving messages.
      Retry Logic needs to be implemented.
      */
      // Since it's top level method of thread, we need to log the exception or it will be unseen
      LOG.error(errorMessage, e);
      throw new RuntimeException(errorMessage, e);
    }
  }
}
