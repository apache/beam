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
package org.apache.beam.sdk.io.cdap.hubspot.common;

import com.google.gson.JsonElement;
import java.io.IOException;
import java.util.Iterator;

/** Iterates over all records in all pages. */
public class HubspotPagesIterator implements Iterator<JsonElement> {
  private HubspotPage currentPage;
  private Iterator<JsonElement> currentPageIterator;
  private int iteratorPosition = 0;
  private String currentPageOffset = null;

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

  public HubspotPagesIterator(SourceHubspotConfig config) throws IOException {
    this(config, new HubspotHelper().getHubspotPage(config, null), null);
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
