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

import com.google.gson.JsonElement;
import io.cdap.plugin.hubspot.common.HubspotHelper;
import io.cdap.plugin.hubspot.common.HubspotPage;
import io.cdap.plugin.hubspot.common.SourceHubspotConfig;
import io.cdap.plugin.hubspot.source.streaming.HubspotStreamingSourceConfig;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("FutureReturnValueIgnored")
public class HubspotCustomReceiver extends Receiver<String> implements HasOffset {

  private static final Logger LOG = LoggerFactory.getLogger(HubspotCustomReceiver.class);
  private static final String RECEIVER_THREAD_NAME = "hubspot_api_listener";
  private final HubspotStreamingSourceConfig config;
  private String startOffset = null;
  private final AtomicBoolean isStopped = new AtomicBoolean(false);
  private Long endOffset = Long.MAX_VALUE;

  HubspotCustomReceiver(HubspotStreamingSourceConfig config) throws IOException {
    super(StorageLevel.MEMORY_AND_DISK_2());
    this.config = config;
  }

  /** @param startOffset inclusive start offset from which the reading should be started. */
  @Override
  public void setStartOffset(Long startOffset) {
    if (startOffset != null) {
      this.startOffset = String.valueOf(startOffset == 0L ? 0 : startOffset - 1);
    }
  }

  public HubspotStreamingSourceConfig getConfig() {
    return config;
  }

  public Long getOffset() {
    return startOffset != null ? Long.valueOf(startOffset) : null;
  }

  @Override
  public void onStart() {
    ThreadFactory namedThreadFactory =
        new ThreadFactoryBuilder().setNameFormat(RECEIVER_THREAD_NAME + "-%d").build();

    Executors.newSingleThreadExecutor(namedThreadFactory).submit(this::receive);
  }

  @Override
  public void onStop() {
    // There is nothing we can do here as the thread calling receive()
    // is designed to stop by itself if isStopped() returns false
    isStopped.set(true);
  }

  @Override
  public boolean isStopped() {
    return isStopped.get();
  }

  /** @return exclusive end offset to which the reading from current page will occur. */
  @Override
  public Long getEndOffset() {
    return endOffset;
  }

  private void receive() {
    try {
      LOG.info("OFFSET = {}", startOffset);
      HubspotPagesIterator hubspotPagesIterator = new HubspotPagesIterator(config, startOffset);

      while (!isStopped.get()) {
        this.endOffset = Long.parseLong(hubspotPagesIterator.currentPage.getOffset());
        if (hubspotPagesIterator.hasNext()) {
          if (!isStopped.get()) {
            store(hubspotPagesIterator.next().toString());
          }
        } else {
          Integer minutesToSleep = config.getPullFrequency().getMinutesValue();
          LOG.debug(String.format("Waiting for '%d' minutes to pull.", minutesToSleep));
          Thread.sleep(TimeUnit.MINUTES.toMillis(1));

          // reload current page
          HubspotPage currentPage =
              new HubspotHelper()
                  .getHubspotPage(config, hubspotPagesIterator.getCurrentPageOffset());
          int iteratorPosition = hubspotPagesIterator.getIteratorPosition();

          hubspotPagesIterator =
              new HubspotPagesIterator(
                  config, currentPage, hubspotPagesIterator.getCurrentPageOffset());
          hubspotPagesIterator.setIteratorPosition(iteratorPosition);
        }
      }
    } catch (Exception e) {
      String errorMessage = "Exception while receiving messages from hubspot";
      // Since it's top level method of thread, we need to log the exception or it will be unseen
      LOG.error(errorMessage, e);
      throw new RuntimeException(errorMessage, e);
    }
  }

  public static class HubspotPagesIterator implements Iterator<JsonElement> {
    private HubspotPage currentPage;
    private Iterator<JsonElement> currentPageIterator;
    private int iteratorPosition = 0;
    private String currentPageOffset;

    /**
     * Constructor for HubspotPagesIterator object.
     *
     * @param config the source hub spot config
     * @param currentPage the current page
     * @param currentPageOffset the current page offset
     */
    public HubspotPagesIterator(
        SourceHubspotConfig config, HubspotPage currentPage, String currentPageOffset) {
      this.currentPage = currentPage;
      this.currentPageIterator = currentPage.getIterator();
      this.currentPageOffset = currentPageOffset;
    }

    public HubspotPagesIterator(SourceHubspotConfig config, String offset) throws IOException {
      this(config, new HubspotHelper().getHubspotPage(config, offset), offset);
    }

    /**
     * Here if require, it will be switched the page.
     *
     * @throws IOException on issues with data reading
     */
    public void switchPageIfNeeded() throws IOException {
      if (!currentPageIterator.hasNext()) {
        // switch page
        HubspotPage nextPage = currentPage.nextPage();

        if (nextPage != null) {
          iteratorPosition = 0;
          currentPageOffset = currentPage.getOffset();
          currentPage = nextPage;
          currentPageIterator = currentPage.getIterator();
        } else {
          currentPageIterator = null;
        }
      }
    }

    @Override
    public boolean hasNext() {
      try {
        switchPageIfNeeded();
      } catch (IOException e) {
        throw new RuntimeException("Failed to switch to next page", e);
      }
      return (currentPageIterator != null);
    }

    @Override
    public JsonElement next() {
      iteratorPosition++;
      return currentPageIterator.next();
    }

    public String getCurrentPageOffset() {
      return currentPageOffset;
    }

    public int getIteratorPosition() {
      return iteratorPosition;
    }

    /**
     * Here, just set the position of iteration.
     *
     * @param iteratorPosition the iterator position
     */
    public void setIteratorPosition(int iteratorPosition) {
      this.currentPageIterator = currentPage.getIterator();

      for (int i = 0; i < iteratorPosition; i++) {
        if (currentPageIterator.hasNext()) {
          next();
        } else {
          break;
        }
      }
    }
  }
}
