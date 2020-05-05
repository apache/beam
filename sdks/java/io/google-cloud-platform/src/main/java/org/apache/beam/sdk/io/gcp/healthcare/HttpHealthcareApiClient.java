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

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.healthcare.v1beta1.CloudHealthcare;
import com.google.api.services.healthcare.v1beta1.CloudHealthcare.Projects.Locations.Datasets.Hl7V2Stores.Messages;
import com.google.api.services.healthcare.v1beta1.CloudHealthcareScopes;
import com.google.api.services.healthcare.v1beta1.model.CreateMessageRequest;
import com.google.api.services.healthcare.v1beta1.model.Empty;
import com.google.api.services.healthcare.v1beta1.model.Hl7V2Store;
import com.google.api.services.healthcare.v1beta1.model.HttpBody;
import com.google.api.services.healthcare.v1beta1.model.IngestMessageRequest;
import com.google.api.services.healthcare.v1beta1.model.IngestMessageResponse;
import com.google.api.services.healthcare.v1beta1.model.ListMessagesResponse;
import com.google.api.services.healthcare.v1beta1.model.Message;
import com.google.api.services.healthcare.v1beta1.model.SearchResourcesRequest;
import com.google.auth.oauth2.GoogleCredentials;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.gcp.util.RetryHttpRequestInitializer;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A client that talks to the Cloud Healthcare API through HTTP requests. This client is created
 * mainly to encapsulate the unserializable dependencies, since most generated classes are not
 * serializable in the HTTP client.
 */
public class HttpHealthcareApiClient<T> implements HealthcareApiClient, Serializable {

  private transient CloudHealthcare client;
  private static final Logger LOG = LoggerFactory.getLogger(HttpHealthcareApiClient.class);
  /**
   * Instantiates a new Http healthcare api client.
   *
   * @throws IOException the io exception
   */
  public HttpHealthcareApiClient() throws IOException {
    initClient();
  }

  /**
   * Instantiates a new Http healthcare api client.
   *
   * @param client the client
   * @throws IOException the io exception
   */
  public HttpHealthcareApiClient(CloudHealthcare client) throws IOException {
    this.client = client;
    initClient();
  }

  public JsonFactory getJsonFactory() {
    return this.client.getJsonFactory();
  }

  @Override
  public Hl7V2Store createHL7v2Store(String dataset, String name) throws IOException {
    Hl7V2Store store = new Hl7V2Store();
    return client
        .projects()
        .locations()
        .datasets()
        .hl7V2Stores()
        .create(dataset, store)
        .setHl7V2StoreId(name)
        .execute();
  }

  @Override
  public Empty deleteHL7v2Store(String name) throws IOException {
    return client.projects().locations().datasets().hl7V2Stores().delete(name).execute();
  }

  @Override
  public Instant getEarliestHL7v2SendTime(String hl7v2Store, @Nullable String filter)
      throws IOException {
    ListMessagesResponse response =
        client
            .projects()
            .locations()
            .datasets()
            .hl7V2Stores()
            .messages()
            .list(hl7v2Store)
            .setFilter(filter)
            .set("view", "full") // needed to retrieve the value for sendtime
            .setOrderBy("sendTime") // default order is ascending
            // https://cloud.google.com/apis/design/design_patterns#sorting_order
            .setPageSize(1) // Only interested in the earliest sendTime
            .execute();
    if (response.isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "Could not find earliest send time. The filter %s  matched no results on "
                  + "HL7v2 Store: %s",
              filter, hl7v2Store));
    }
    String sendTime = response.getHl7V2Messages().get(0).getSendTime();
    if (Strings.isNullOrEmpty(sendTime)) {
      LOG.warn(
          String.format(
              "Earliest message in %s has null or empty sendTime defaulting to Epoch.",
              hl7v2Store));
      return Instant.ofEpochMilli(0);
    }
    // sendTime is conveniently RFC3339 UTC "Zulu"
    // https://cloud.google.com/healthcare/docs/reference/rest/v1beta1/projects.locations.datasets.hl7V2Stores.messages#Message
    return Instant.parse(sendTime);
  }

  @Override
  public Instant getLatestHL7v2SendTime(String hl7v2Store, @Nullable String filter)
      throws IOException {
    ListMessagesResponse response =
        client
            .projects()
            .locations()
            .datasets()
            .hl7V2Stores()
            .messages()
            .list(hl7v2Store)
            .setFilter(filter)
            .set("view", "full") // needed to retrieve the value for sendTime
            .setOrderBy("sendTime desc")
            // https://cloud.google.com/apis/design/design_patterns#sorting_order
            .setPageSize(1) // Only interested in the earliest sendTime
            .execute();
    if (response.isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "Could not find latest send time. The filter %s  matched no results on "
                  + "HL7v2 Store: %s",
              filter, hl7v2Store));
    }
    String sendTime = response.getHl7V2Messages().get(0).getSendTime();
    if (Strings.isNullOrEmpty(sendTime)) {
      LOG.warn(
          String.format(
              "Latest message in %s has null or empty sendTime defaulting to now.", hl7v2Store));
      return Instant.now();
    }
    // sendTime is conveniently RFC3339 UTC "Zulu"
    // https://cloud.google.com/healthcare/docs/reference/rest/v1beta1/projects.locations.datasets.hl7V2Stores.messages#Message
    return Instant.parse(sendTime);
  }

  @Override
  public ListMessagesResponse makeSendTimeBoundHL7v2ListRequest(
      String hl7v2Store,
      Instant start,
      @Nullable Instant end,
      @Nullable String otherFilter,
      @Nullable String orderBy,
      @Nullable String pageToken)
      throws IOException {
    String filter;
    String sendTimeFilter = "";
    if (start != null) {
      sendTimeFilter += String.format("sendTime >= \"%s\"", start.toString());
      if (end != null) {
        sendTimeFilter += String.format(" AND sendTime < \"%s\"", end.toString());
      }
    }

    // TODO(jaketf) does this cause issues if other filter has predicate on sendTime?
    if (otherFilter != null && !Strings.isNullOrEmpty(sendTimeFilter)) {
      filter = sendTimeFilter + " AND " + otherFilter;
    } else {
      filter = sendTimeFilter;
    }
    return makeHL7v2ListRequest(hl7v2Store, filter, orderBy, pageToken);
  }

  @Override
  public ListMessagesResponse makeHL7v2ListRequest(
      String hl7v2Store,
      @Nullable String filter,
      @Nullable String orderBy,
      @Nullable String pageToken)
      throws IOException {

    Messages.List baseRequest =
        client
            .projects()
            .locations()
            .datasets()
            .hl7V2Stores()
            .messages()
            .list(hl7v2Store)
            .set("view", "full")
            .setFilter(filter)
            .setPageSize(1000)
            .setPageToken(pageToken);
    if (orderBy == null) {
      return baseRequest.execute();
    } else {
      return baseRequest.setOrderBy(orderBy).execute();
    }
  }

  /**
   * Gets HL7v2 message.
   *
   * @param msgName the msg name
   * @return the message
   * @throws IOException the io exception
   * @throws ParseException the parse exception
   */
  @Override
  public Message getHL7v2Message(String msgName) throws IOException {
    Message msg =
        client.projects().locations().datasets().hl7V2Stores().messages().get(msgName).execute();
    if (msg == null) {
      throw new IOException(String.format("Couldn't find message: %s.", msgName));
    }
    return msg;
  }

  @Override
  public Empty deleteHL7v2Message(String msgName) throws IOException {
    return client
        .projects()
        .locations()
        .datasets()
        .hl7V2Stores()
        .messages()
        .delete(msgName)
        .execute();
  }

  /**
   * Gets HL7v2 store.
   *
   * @param storeName the store name
   * @return the HL7v2 store
   * @throws IOException the io exception
   */
  @Override
  public Hl7V2Store getHL7v2Store(String storeName) throws IOException {
    return client.projects().locations().datasets().hl7V2Stores().get(storeName).execute();
  }

  @Override
  public IngestMessageResponse ingestHL7v2Message(String hl7v2Store, Message msg)
      throws IOException {
    IngestMessageRequest ingestMessageRequest = new IngestMessageRequest();
    ingestMessageRequest.setMessage(msg);
    return client
        .projects()
        .locations()
        .datasets()
        .hl7V2Stores()
        .messages()
        .ingest(hl7v2Store, ingestMessageRequest)
        .execute();
  }

  @Override
  public HttpBody fhirSearch(String fhirStore, SearchResourcesRequest query) throws IOException {
    return client
        .projects()
        .locations()
        .datasets()
        .fhirStores()
        .fhir()
        .search(fhirStore, query)
        .execute();
  }

  @Override
  public Message createHL7v2Message(String hl7v2Store, Message msg) throws IOException {
    CreateMessageRequest createMessageRequest = new CreateMessageRequest();
    createMessageRequest.setMessage(msg);
    return client
        .projects()
        .locations()
        .datasets()
        .hl7V2Stores()
        .messages()
        .create(hl7v2Store, createMessageRequest)
        .execute();
  }

  @Override
  public HttpBody createFhirResource(String fhirStore, String type, HttpBody body)
      throws IOException {
    return client
        .projects()
        .locations()
        .datasets()
        .fhirStores()
        .fhir()
        .create(fhirStore, type, body)
        .execute();
  }

  @Override
  public HttpBody executeFhirBundle(String fhirStore, HttpBody bundle) throws IOException {
    return client
        .projects()
        .locations()
        .datasets()
        .fhirStores()
        .fhir()
        .executeBundle(fhirStore, bundle)
        .execute();
  }

  @Override
  public HttpBody listFHIRResourceForPatient(String fhirStore, String patient) throws IOException {
    return client
        .projects()
        .locations()
        .datasets()
        .fhirStores()
        .fhir()
        .patientEverything(patient)
        .execute();
  }

  @Override
  public HttpBody readFHIRResource(String fhirStore, String resource) throws IOException {
    return client.projects().locations().datasets().fhirStores().fhir().read(resource).execute();
  }

  private static class AuthenticatedRetryInitializer extends RetryHttpRequestInitializer {
    GoogleCredentials credentials;

    public AuthenticatedRetryInitializer(GoogleCredentials credentials) {
      super();
      this.credentials = credentials;
    }

    @Override
    public void initialize(HttpRequest request) throws IOException {
      super.initialize(request);
      if (!credentials.hasRequestMetadata()) {
        return;
      }
      HttpHeaders requestHeaders = request.getHeaders();
      requestHeaders.setUserAgent("apache-beam-hl7v2-io");
      URI uri = null;
      if (request.getUrl() != null) {
        uri = request.getUrl().toURI();
      }
      Map<String, List<String>> credentialHeaders = credentials.getRequestMetadata(uri);
      if (credentialHeaders == null) {
        return;
      }
      for (Map.Entry<String, List<String>> entry : credentialHeaders.entrySet()) {
        String headerName = entry.getKey();
        List<String> requestValues = new ArrayList<>(entry.getValue());
        requestHeaders.put(headerName, requestValues);
      }
    }
  }

  private void initClient() throws IOException {
    // Create a HttpRequestInitializer, which will provide a baseline configuration to all requests.
    // HttpRequestInitializer requestInitializer = new RetryHttpRequestInitializer();
    // GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
    HttpRequestInitializer requestInitializer =
        new AuthenticatedRetryInitializer(
            GoogleCredentials.getApplicationDefault()
                .createScoped(CloudHealthcareScopes.CLOUD_PLATFORM));

    client =
        new CloudHealthcare.Builder(
                new NetHttpTransport(), new JacksonFactory(), requestInitializer)
            .setApplicationName("apache-beam-hl7v2-io")
            .build();
  }

  public static class HL7v2MessagePages implements Iterable<List<HL7v2Message>> {

    private final String hl7v2Store;
    private final String filter;
    private final String orderBy;
    private final Instant start;
    private final Instant end;
    private transient HealthcareApiClient client;

    /**
     * Instantiates a new HL7v2 message id pages.
     *
     * @param client the client
     * @param hl7v2Store the HL7v2 store
     */
    HL7v2MessagePages(
        HealthcareApiClient client,
        String hl7v2Store,
        @Nullable Instant start,
        @Nullable Instant end) {
      this.client = client;
      this.hl7v2Store = hl7v2Store;
      this.filter = null;
      this.orderBy = null;
      this.start = start;
      this.end = end;
    }

    /**
     * Instantiates a new HL7v2 message id pages.
     *
     * @param client the client
     * @param hl7v2Store the HL7v2 store
     * @param filter the filter
     */
    HL7v2MessagePages(
        HealthcareApiClient client,
        String hl7v2Store,
        @Nullable Instant start,
        @Nullable Instant end,
        @Nullable String filter,
        @Nullable String orderBy) {
      this.client = client;
      this.hl7v2Store = hl7v2Store;
      this.filter = filter;
      this.orderBy = orderBy;
      this.start = start;
      this.end = end;
    }

    public Instant getStart() {
      return this.start;
    }

    public Instant getEnd() {
      return this.end;
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
        @Nullable Instant start,
        @Nullable Instant end,
        @Nullable String filter,
        @Nullable String orderBy,
        @Nullable String pageToken)
        throws IOException {
      return client.makeSendTimeBoundHL7v2ListRequest(
          hl7v2Store, start, end, filter, orderBy, pageToken);
    }

    @Override
    public Iterator<List<HL7v2Message>> iterator() {
      return new HL7v2MessagePagesIterator(
          this.client, this.hl7v2Store, this.start, this.end, this.filter, this.orderBy);
    }

    /** The type Hl7v2 message id pages iterator. */
    public static class HL7v2MessagePagesIterator implements Iterator<List<HL7v2Message>> {

      private final String hl7v2Store;
      private final String filter;
      private final String orderBy;
      private final Instant start;
      private final Instant end;
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
      HL7v2MessagePagesIterator(
          HealthcareApiClient client,
          String hl7v2Store,
          @Nullable Instant start,
          @Nullable Instant end,
          @Nullable String filter,
          @Nullable String orderBy) {
        this.client = client;
        this.hl7v2Store = hl7v2Store;
        this.start = start;
        this.end = end;
        this.filter = filter;
        this.orderBy = orderBy;
        this.pageToken = null;
        this.isFirstRequest = true;
      }

      @Override
      public boolean hasNext() throws NoSuchElementException {
        if (isFirstRequest) {
          try {
            ListMessagesResponse response =
                makeListRequest(client, hl7v2Store, start, end, filter, orderBy, pageToken);
            List<Message> msgs = response.getHl7V2Messages();
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
      public List<HL7v2Message> next() throws NoSuchElementException {
        try {
          ListMessagesResponse response =
              makeListRequest(client, hl7v2Store, start, end, filter, orderBy, pageToken);
          this.isFirstRequest = false;
          this.pageToken = response.getNextPageToken();
          List<Message> msgs = response.getHl7V2Messages();

          return msgs.stream().map(HL7v2Message::fromModel).collect(Collectors.toList());
        } catch (IOException e) {
          this.pageToken = null;
          throw new NoSuchElementException(
              String.format(
                  "Error listing HL7v2 Messages from %s: %s", hl7v2Store, e.getMessage()));
        }
      }
    }
  }
}
