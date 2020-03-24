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

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.healthcare.v1alpha2.CloudHealthcare;
import com.google.api.services.healthcare.v1alpha2.CloudHealthcare.Projects.Locations.Datasets.Hl7V2Stores.Messages;
import com.google.api.services.healthcare.v1alpha2.model.CreateMessageRequest;
import com.google.api.services.healthcare.v1alpha2.model.Empty;
import com.google.api.services.healthcare.v1alpha2.model.Hl7V2Store;
import com.google.api.services.healthcare.v1alpha2.model.HttpBody;
import com.google.api.services.healthcare.v1alpha2.model.IngestMessageRequest;
import com.google.api.services.healthcare.v1alpha2.model.IngestMessageResponse;
import com.google.api.services.healthcare.v1alpha2.model.ListMessagesResponse;
import com.google.api.services.healthcare.v1alpha2.model.Message;
import com.google.api.services.healthcare.v1alpha2.model.SearchResourcesRequest;
import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.gcp.util.RetryHttpRequestInitializer;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;

/**
 * A client that talks to the Cloud Healthcare API through HTTP requests. This client is created
 * mainly to encapsulate the unserializable dependencies, since most generated classes are not
 * serializable in the HTTP client.
 */
public class HttpHealthcareApiClient<T> implements HealthcareApiClient, Serializable {

  private transient CloudHealthcare client;

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

  @VisibleForTesting
  static <T, X extends Collection<T>> Stream<T> flattenIteratorCollectionsToStream(
      Iterator<X> iterator) {
    Spliterator<Collection<T>> spliterator = Spliterators.spliteratorUnknownSize(iterator, 0);
    return StreamSupport.stream(spliterator, false).flatMap(Collection::stream);
  }

  @Override
  public ListMessagesResponse makeHL7v2ListRequest(
      String hl7v2Store, @Nullable String filter, @Nullable String pageToken) throws IOException {

    Messages.List baseRequest =
        client
            .projects()
            .locations()
            .datasets()
            .hl7V2Stores()
            .messages()
            .list(hl7v2Store)
            .setPageToken(pageToken);

    if (Strings.isNullOrEmpty(filter)) {
      return baseRequest.execute();
    } else {
      return baseRequest.setFilter(filter).execute();
    }
  }

  /**
   * Gets message id page iterator.
   *
   * @param hl7v2Store the HL7v2 store
   * @return the message id page iterator
   * @throws IOException the io exception
   */
  @Override
  public Stream<String> getHL7v2MessageIDStream(String hl7v2Store) throws IOException {
    return getHL7v2MessageIDStream(hl7v2Store, null);
  }

  /**
   * Get a {@link Stream} of message IDs from flattening the pages of a new {@link
   * HL7v2MessageIDPages}.
   *
   * @param hl7v2Store the HL7v2 store
   * @param filter the filter
   * @return the message id Stream
   * @throws IOException the io exception
   */
  @Override
  public Stream<String> getHL7v2MessageIDStream(String hl7v2Store, @Nullable String filter)
      throws IOException {
    Iterator<List<String>> iterator = new HL7v2MessageIDPages(this, hl7v2Store, filter).iterator();
    return flattenIteratorCollectionsToStream(iterator);
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

  private void initClient() throws IOException {
    // Create a HttpRequestInitializer, which will provide a baseline configuration to all requests.
    HttpRequestInitializer requestInitializer = new RetryHttpRequestInitializer();
    // GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
    // HttpRequestInitializer requestInitializer = new HttpCredentialsAdapter(GoogleCredentials.getApplicationDefault());

    client =
        new CloudHealthcare.Builder(new NetHttpTransport(), new GsonFactory(), requestInitializer)
            .setApplicationName("apache-beam-hl7-io")
            .build();
  }

  public static class HL7v2MessageIDPages implements Iterable<List<String>> {

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
            ListMessagesResponse response = makeListRequest(client, hl7v2Store, filter, pageToken);
            System.out.println(String.format(".getMessages() -> %s", response.getMessages()));
            System.out.println(String.format(".get('hl7V2Messages') -> %s",
                response.get("hl7V2Messages").toString()));
            System.out.println(String.format(".getHl7V2Messages() -> %s",
                response.get("hl7V2Messages").toString()));
            List<String> msgs = (List<String>) response.getMessages();
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
          ListMessagesResponse response = makeListRequest(client, hl7v2Store, filter, pageToken);
          this.isFirstRequest = false;
          this.pageToken = response.getNextPageToken();
          return response.getMessages();
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
