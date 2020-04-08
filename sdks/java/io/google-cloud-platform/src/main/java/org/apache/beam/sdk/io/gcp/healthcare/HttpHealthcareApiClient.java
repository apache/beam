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
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.healthcare.v1beta1.CloudHealthcare;
import com.google.api.services.healthcare.v1beta1.CloudHealthcare.Projects.Locations.Datasets.Hl7V2Stores.Messages;
import com.google.api.services.healthcare.v1beta1.CloudHealthcareScopes;
import com.google.api.services.healthcare.v1beta1.model.CreateMessageRequest;
import com.google.api.services.healthcare.v1beta1.model.Empty;
import com.google.api.services.healthcare.v1beta1.model.FhirStore;
import com.google.api.services.healthcare.v1beta1.model.GoogleCloudHealthcareV1beta1FhirRestGcsSource;
import com.google.api.services.healthcare.v1beta1.model.Hl7V2Store;
import com.google.api.services.healthcare.v1beta1.model.HttpBody;
import com.google.api.services.healthcare.v1beta1.model.ImportResourcesRequest;
import com.google.api.services.healthcare.v1beta1.model.IngestMessageRequest;
import com.google.api.services.healthcare.v1beta1.model.IngestMessageResponse;
import com.google.api.services.healthcare.v1beta1.model.ListMessagesResponse;
import com.google.api.services.healthcare.v1beta1.model.Message;
import com.google.api.services.healthcare.v1beta1.model.Operation;
import com.google.api.services.healthcare.v1beta1.model.SearchResourcesRequest;
import com.google.api.services.storage.StorageScopes;
import com.google.auth.oauth2.GoogleCredentials;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.gcp.util.RetryHttpRequestInitializer;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A client that talks to the Cloud Healthcare API through HTTP requests. This client is created
 * mainly to encapsulate the unserializable dependencies, since most generated classes are not
 * serializable in the HTTP client.
 */
public class HttpHealthcareApiClient implements HealthcareApiClient, Serializable {
  private static final String FHIRSTORE_HEADER_CONTENT_TYPE = "application/fhir+json";
  private static final String FHIRSTORE_HEADER_ACCEPT = "application/fhir+json; charset=utf-8";
  private static final String FHIRSTORE_HEADER_ACCEPT_CHARSET = "utf-8";
  private static final Logger LOG = LoggerFactory.getLogger(HttpHealthcareApiClient.class);
  private transient CloudHealthcare client;
  private transient HttpClient httpClient;
  private transient GoogleCredentials credentials;

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
    this.httpClient = HttpClients.createDefault();
    initClient();
  }

  @VisibleForTesting
  static <T, X extends Collection<T>> Stream<T> flattenIteratorCollectionsToStream(
      Iterator<X> iterator) {
    Spliterator<Collection<T>> spliterator = Spliterators.spliteratorUnknownSize(iterator, 0);
    return StreamSupport.stream(spliterator, false).flatMap(Collection::stream);
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
  public FhirStore createFhirStore(String dataset, String name, String version) throws IOException {
    FhirStore store = new FhirStore();
    // TODO add separate integration tests for each FHIR version: https://cloud.google.com/healthcare/docs/reference/rest/v1beta1/projects.locations.datasets.fhirStores#Version
    // Our integration test data is STU3.
    store.setVersion(version);
    store.setDisableReferentialIntegrity(true);
    store.setEnableUpdateCreate(true);
    return client
        .projects()
        .locations()
        .datasets()
        .fhirStores()
        .create(dataset, store)
        .setFhirStoreId(name)
        .execute();
  }

  @Override
  public Empty deleteHL7v2Store(String name) throws IOException {
    return client.projects().locations().datasets().hl7V2Stores().delete(name).execute();
  }

  @Override
  public Empty deleteFhirStore(String name) throws IOException {
    return client.projects().locations().datasets().fhirStores().delete(name).execute();
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
            .set("view", "full")
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
  public Stream<HL7v2Message> getHL7v2MessageStream(String hl7v2Store) throws IOException {
    return getHL7v2MessageStream(hl7v2Store, null);
  }

  /**
   * Get a {@link Stream} of message IDs from flattening the pages of a new {@link
   * HL7v2MessagePages}.
   *
   * @param hl7v2Store the HL7v2 store
   * @param filter the filter
   * @return the message id Stream
   * @throws IOException the io exception
   */
  @Override
  public Stream<HL7v2Message> getHL7v2MessageStream(String hl7v2Store, @Nullable String filter)
      throws IOException {
    Iterator<List<HL7v2Message>> iterator =
        new HL7v2MessagePages(this, hl7v2Store, filter).iterator();
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
  public Operation importFhirResource(
      String fhirStore, String gcsSourcePath, @Nullable String contentStructure)
      throws IOException {
    GoogleCloudHealthcareV1beta1FhirRestGcsSource gcsSrc =
        new GoogleCloudHealthcareV1beta1FhirRestGcsSource();

    gcsSrc.setUri(gcsSourcePath);
    ImportResourcesRequest importRequest = new ImportResourcesRequest();
    importRequest.setGcsSource(gcsSrc).setContentStructure(contentStructure);
    return client
        .projects()
        .locations()
        .datasets()
        .fhirStores()
        .healthcareImport(fhirStore, importRequest)
        .execute();
  }

  @Override
  public Operation pollOperation(Operation operation, Long sleepMs)
      throws InterruptedException, IOException {
    LOG.debug(String.format("started opertation %s. polling until complete.", operation.getName()));
    while (operation.getDone() == null || !operation.getDone()) {
      // Update the status of the operation with another request.
      Thread.sleep(sleepMs); // Pause between requests.
      operation =
          client.projects().locations().datasets().operations().get(operation.getName()).execute();
    }
    return operation;
  }

  @Override
  public HttpBody executeFhirBundle(String fhirStore, HttpBody bundle) throws IOException {
    if (httpClient == null || client == null) {
      initClient();
    }

    credentials.refreshIfExpired();
    StringEntity requestEntity = new StringEntity(bundle.getData(), ContentType.APPLICATION_JSON);
    URI uri;
    try {
      uri =
          new URIBuilder(client.getRootUrl() + "v1beta1/" + fhirStore + "/fhir")
              .setParameter("access_token", credentials.getAccessToken().getTokenValue())
              .build();
    } catch (URISyntaxException e) {
      LOG.error("URL error when making executeBundle request to FHIR API. " + e.getMessage());
      throw new IllegalArgumentException(e);
    }

    HttpUriRequest request =
        RequestBuilder.post()
            .setUri(uri)
            .setEntity(requestEntity)
            .addHeader("Content-Type", FHIRSTORE_HEADER_CONTENT_TYPE)
            .addHeader("Accept-Charset", FHIRSTORE_HEADER_ACCEPT_CHARSET)
            .addHeader("Accept", FHIRSTORE_HEADER_ACCEPT)
            .build();

    HttpResponse response = httpClient.execute(request);
    HttpEntity responseEntity = response.getEntity();
    String content = EntityUtils.toString(responseEntity);
    // Check 2XX code.
    if (!(response.getStatusLine().getStatusCode() / 100 == 2)) {
      throw new IOException(
          String.format(
              "ExecuteBundle request to FHIR API failed with status code %s: %s",
              String.valueOf(response.getStatusLine().getStatusCode()), content));
    }
    HttpBody responseModel = new HttpBody();
    responseModel.setData(content);
    return responseModel;
  }

  @Override
  public HttpBody readFhirResource(String resourceId) throws IOException {
    return client.projects().locations().datasets().fhirStores().fhir().read(resourceId).execute();
  }

  @Override
  public HttpBody deleteFhirResource(String resourceId) throws IOException {
    return client
        .projects()
        .locations()
        .datasets()
        .fhirStores()
        .fhir()
        .delete(resourceId)
        .execute();
  }

  public static class AuthenticatedRetryInitializer extends RetryHttpRequestInitializer {
    GoogleCredentials credentials;

    public AuthenticatedRetryInitializer(GoogleCredentials credentials) {
      super();
      this.credentials = credentials;
    }

    @Override
    public void initialize(HttpRequest request) throws IOException {
      super.initialize(request);
      HttpHeaders requestHeaders = request.getHeaders();
      requestHeaders.setUserAgent("apache-beam-hl7v2-io");
      if (!credentials.hasRequestMetadata()) {
        return;
      }
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
    credentials = GoogleCredentials.getApplicationDefault();
    // Create a HttpRequestInitializer, which will provide a baseline configuration to all requests.
    HttpRequestInitializer requestInitializer =
        new AuthenticatedRetryInitializer(
            credentials.createScoped(
                CloudHealthcareScopes.CLOUD_PLATFORM, StorageScopes.CLOUD_PLATFORM_READ_ONLY));

    client =
        new CloudHealthcare.Builder(new NetHttpTransport(), new GsonFactory(), requestInitializer)
            .setApplicationName("apache-beam-hl7v2-io")
            .build();
    httpClient =
        HttpClients.custom().setRetryHandler(new DefaultHttpRequestRetryHandler(10, false)).build();
  }

  public static class HL7v2MessagePages implements Iterable<List<HL7v2Message>> {

    private final String hl7v2Store;
    private final String filter;
    private transient HealthcareApiClient client;

    /**
     * Instantiates a new HL7v2 message id pages.
     *
     * @param client the client
     * @param hl7v2Store the HL7v2 store
     */
    HL7v2MessagePages(HealthcareApiClient client, String hl7v2Store) {
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
    HL7v2MessagePages(HealthcareApiClient client, String hl7v2Store, @Nullable String filter) {
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
    public Iterator<List<HL7v2Message>> iterator() {
      return new HL7v2MessagePagesIterator(this.client, this.hl7v2Store, this.filter);
    }

    /** The type Hl7v2 message id pages iterator. */
    public static class HL7v2MessagePagesIterator implements Iterator<List<HL7v2Message>> {

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
      HL7v2MessagePagesIterator(
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
          ListMessagesResponse response = makeListRequest(client, hl7v2Store, filter, pageToken);
          this.isFirstRequest = false;
          this.pageToken = response.getNextPageToken();
          return response.getHl7V2Messages().stream()
              .map(HL7v2Message::fromModel)
              .collect(Collectors.toList());

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
