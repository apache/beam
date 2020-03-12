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
package org.apache.beam.sdk.io.gcp.healthcare;

import com.google.api.services.healthcare.v1alpha2.model.ListMessagesResponse;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import javax.annotation.Nullable;

/** The type HL7v2 message id pages. */
public class HL7v2MessageIDPages implements Iterable<List<String>> {

  private final String hl7v2Store;
  private final String filter;
  private transient HealthcareApiClient client;

  /**
   * Instantiates a new HL7v2 message id pages.
   *
   * @param client the client
   * @param hl7v2Store the HL7v2 store
   */
  HL7v2MessageIDPages(HealthcareApiClient client, String hl7v2Store) {
    this.client = client;
    this.hl7v2Store = hl7v2Store;
    this.filter = null;
  }

  /**
   * Instantiates a new HL7v2 message id pages.
   *
   * @param client the client
   * @param hl7v2Store the HL7v2 store
   * @param filter the filter
   */
  HL7v2MessageIDPages(HealthcareApiClient client, String hl7v2Store, @Nullable String filter) {
    this.client = client;
    this.hl7v2Store = hl7v2Store;
    this.filter = filter;
  }

  /**
   * Make list request list messages response.
   *
   * @param client the client
   * @param hl7v2Store the hl 7 v 2 store
   * @param filter the filter
   * @param pageToken the page token
   * @return the list messages response
   * @throws IOException the io exception
   */
  public static ListMessagesResponse makeListRequest(
      HealthcareApiClient client,
      String hl7v2Store,
      @Nullable String filter,
      @Nullable String pageToken)
      throws IOException {
    return client.makeHL7v2ListRequest(hl7v2Store, filter, pageToken);
  }

  @Override
  public Iterator<List<String>> iterator() {
    return new HL7v2MessageIDPagesIterator(this.client, this.hl7v2Store, this.filter);
  }

  /** The type Hl7v2 message id pages iterator. */
  public static class HL7v2MessageIDPagesIterator implements Iterator<List<String>> {

    private final String hl7v2Store;
    private final String filter;
    private HealthcareApiClient client;
    private String pageToken;
    private boolean isFirstRequest;

    /**
     * Instantiates a new Hl 7 v 2 message id pages iterator.
     *
     * @param client the client
     * @param hl7v2Store the hl 7 v 2 store
     * @param filter the filter
     */
    HL7v2MessageIDPagesIterator(
        HealthcareApiClient client, String hl7v2Store, @Nullable String filter) {
      this.client = client;
      this.hl7v2Store = hl7v2Store;
      this.filter = filter;
      this.pageToken = null;
      this.isFirstRequest = true;
    }

    @Override
    public boolean hasNext() throws NoSuchElementException {
      if (isFirstRequest) {
        try {
          List<String> msgs = makeListRequest(client, hl7v2Store, filter, pageToken).getMessages();
          if (msgs == null) {
            return false;
          } else {
            return !msgs.isEmpty();
          }
        } catch (IOException e) {
          throw new NoSuchElementException(
              String.format(
                  "Failed to list first page of HL7v2 messages from %s: %s",
                  hl7v2Store, e.getMessage()));
        }
      }
      return this.pageToken != null;
    }

    @Override
    public List<String> next() throws NoSuchElementException {
      try {
        // TODO(jaketf): Parameterize page size for throughput parameterization
        ListMessagesResponse response = makeListRequest(client, hl7v2Store, filter, pageToken);
        this.isFirstRequest = false;
        this.pageToken = response.getNextPageToken();
        return response.getMessages();
      } catch (IOException e) {
        this.pageToken = null;
        throw new NoSuchElementException(
            String.format("Error listing HL7v2 Messages from %s: %s", hl7v2Store, e.getMessage()));
      }
    }
  }
}
