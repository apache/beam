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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.healthcare.v1.CloudHealthcare;
import com.google.api.services.healthcare.v1.CloudHealthcare.Projects.Locations.Datasets.FhirStores.Fhir.PatientEverything;
import com.google.api.services.healthcare.v1.CloudHealthcare.Projects.Locations.Datasets.Hl7V2Stores.Messages;
import com.google.api.services.healthcare.v1.CloudHealthcareRequest;
import com.google.api.services.healthcare.v1.CloudHealthcareScopes;
import com.google.api.services.healthcare.v1.model.CreateMessageRequest;
import com.google.api.services.healthcare.v1.model.DeidentifyConfig;
import com.google.api.services.healthcare.v1.model.DeidentifyFhirStoreRequest;
import com.google.api.services.healthcare.v1.model.DicomStore;
import com.google.api.services.healthcare.v1.model.Empty;
import com.google.api.services.healthcare.v1.model.ExportResourcesRequest;
import com.google.api.services.healthcare.v1.model.FhirStore;
import com.google.api.services.healthcare.v1.model.GoogleCloudHealthcareV1FhirBigQueryDestination;
import com.google.api.services.healthcare.v1.model.GoogleCloudHealthcareV1FhirGcsDestination;
import com.google.api.services.healthcare.v1.model.GoogleCloudHealthcareV1FhirGcsSource;
import com.google.api.services.healthcare.v1.model.Hl7V2Store;
import com.google.api.services.healthcare.v1.model.HttpBody;
import com.google.api.services.healthcare.v1.model.ImportResourcesRequest;
import com.google.api.services.healthcare.v1.model.IngestMessageRequest;
import com.google.api.services.healthcare.v1.model.IngestMessageResponse;
import com.google.api.services.healthcare.v1.model.ListFhirStoresResponse;
import com.google.api.services.healthcare.v1.model.ListMessagesResponse;
import com.google.api.services.healthcare.v1.model.Message;
import com.google.api.services.healthcare.v1.model.NotificationConfig;
import com.google.api.services.healthcare.v1.model.Operation;
import com.google.api.services.healthcare.v1.model.SchemaConfig;
import com.google.api.services.healthcare.v1.model.SearchResourcesRequest;
import com.google.api.services.storage.StorageScopes;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.gcp.util.RetryHttpRequestInitializer;
import org.apache.beam.sdk.util.ReleaseInfo;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.client.HttpClients;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A client that talks to the Cloud Healthcare API through HTTP requests. This client is created
 * mainly to encapsulate the unserializable dependencies, since most generated classes are not
 * serializable in the HTTP client.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class HttpHealthcareApiClient implements HealthcareApiClient, Serializable {
  private static final String USER_AGENT =
      String.format(
          "apache-beam-io-google-cloud-platform-healthcare/%s",
          ReleaseInfo.getReleaseInfo().getSdkVersion());
  private static final JsonFactory PARSER = new GsonFactory();
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
    return createFhirStore(dataset, name, version, null);
  }

  @Override
  public FhirStore createFhirStore(
      String dataset, String name, String version, @Nullable String pubsubTopic)
      throws IOException {
    FhirStore store = new FhirStore();

    store.setVersion(version);
    store.setDisableReferentialIntegrity(true);
    store.setEnableUpdateCreate(true);
    if (pubsubTopic != null) {
      NotificationConfig notificationConfig = new NotificationConfig();
      notificationConfig.setPubsubTopic(pubsubTopic);
      store.setNotificationConfig(notificationConfig);
    }
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
  public List<FhirStore> listAllFhirStores(String dataset) throws IOException {
    ArrayList<FhirStore> fhirStores = new ArrayList<>();
    String pageToken = "";
    do {
      ListFhirStoresResponse resp =
          client
              .projects()
              .locations()
              .datasets()
              .fhirStores()
              .list(dataset)
              .setPageToken(pageToken)
              .execute();
      for (FhirStore fs : resp.getFhirStores()) {
        fhirStores.add(fs);
      }
      if (resp.getNextPageToken() == null) {
        break;
      }
      pageToken = resp.getNextPageToken();
    } while (!pageToken.equals(""));
    return fhirStores;
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
  public String retrieveDicomStudyMetadata(String dicomWebPath) throws IOException {
    WebPathParser parser = new WebPathParser();
    WebPathParser.DicomWebPath parsedDicomWebPath = parser.parseDicomWebpath(dicomWebPath);

    String searchQuery = String.format("studies/%s/metadata", parsedDicomWebPath.studyId);

    return makeRetrieveStudyMetadataRequest(parsedDicomWebPath.dicomStorePath, searchQuery);
  }

  @Override
  public DicomStore createDicomStore(String dataset, String name) throws IOException {
    return createDicomStore(dataset, name, null);
  }

  @Override
  public Empty deleteDicomStore(String name) throws IOException {
    return client.projects().locations().datasets().dicomStores().delete(name).execute();
  }

  @Override
  public Empty uploadToDicomStore(String webPath, String filePath)
      throws IOException, URISyntaxException {
    byte[] dcmFile = Files.readAllBytes(Paths.get(filePath));
    ByteArrayEntity requestEntity = new ByteArrayEntity(dcmFile);

    String uri = String.format("%sv1/%s/dicomWeb/studies", client.getRootUrl(), webPath);
    URIBuilder uriBuilder =
        new URIBuilder(uri)
            .setParameter("access_token", credentials.getAccessToken().getTokenValue());
    HttpUriRequest request =
        RequestBuilder.post(uriBuilder.build())
            .setEntity(requestEntity)
            .addHeader("Content-Type", "application/dicom")
            .build();
    httpClient.execute(request);
    return new Empty();
  }

  @Override
  public DicomStore createDicomStore(String dataset, String name, @Nullable String pubsubTopic)
      throws IOException {
    DicomStore store = new DicomStore();

    if (pubsubTopic != null) {
      NotificationConfig notificationConfig = new NotificationConfig();
      notificationConfig.setPubsubTopic(pubsubTopic);
      store.setNotificationConfig(notificationConfig);
    }

    return client
        .projects()
        .locations()
        .datasets()
        .dicomStores()
        .create(dataset, store)
        .setDicomStoreId(name)
        .execute();
  }

  private String makeRetrieveStudyMetadataRequest(String dicomStorePath, String searchQuery)
      throws IOException {
    CloudHealthcare.Projects.Locations.Datasets.DicomStores.Studies.RetrieveMetadata request =
        this.client
            .projects()
            .locations()
            .datasets()
            .dicomStores()
            .studies()
            .retrieveMetadata(dicomStorePath, searchQuery);
    com.google.api.client.http.HttpResponse response = request.executeUnparsed();

    return response.parseAsString();
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
    // https://cloud.google.com/healthcare/docs/reference/rest/v1/projects.locations.datasets.hl7V2Stores.messages#Message
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
    // https://cloud.google.com/healthcare/docs/reference/rest/v1/projects.locations.datasets.hl7V2Stores.messages#Message
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
  public Operation importFhirResource(
      String fhirStore, String gcsSourcePath, @Nullable String contentStructure)
      throws IOException {
    GoogleCloudHealthcareV1FhirGcsSource gcsSrc = new GoogleCloudHealthcareV1FhirGcsSource();

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
  public Operation exportFhirResourceToGcs(String fhirStore, String gcsDestinationPrefix)
      throws IOException {
    GoogleCloudHealthcareV1FhirGcsDestination gcsDst =
        new GoogleCloudHealthcareV1FhirGcsDestination();
    gcsDst.setUriPrefix(gcsDestinationPrefix);
    ExportResourcesRequest exportRequest = new ExportResourcesRequest();
    exportRequest.setGcsDestination(gcsDst);
    return client
        .projects()
        .locations()
        .datasets()
        .fhirStores()
        .export(fhirStore, exportRequest)
        .execute();
  }

  @Override
  public Operation exportFhirResourceToBigQuery(String fhirStore, String bigQueryDatasetUri)
      throws IOException {
    final GoogleCloudHealthcareV1FhirBigQueryDestination bigQueryDestination =
        new GoogleCloudHealthcareV1FhirBigQueryDestination();
    bigQueryDestination.setDatasetUri(bigQueryDatasetUri);

    final SchemaConfig schemaConfig = new SchemaConfig();
    schemaConfig.setSchemaType("ANALYTICS");
    bigQueryDestination.setSchemaConfig(schemaConfig);

    final ExportResourcesRequest exportRequest = new ExportResourcesRequest();
    exportRequest.setBigqueryDestination(bigQueryDestination);
    return client
        .projects()
        .locations()
        .datasets()
        .fhirStores()
        .export(fhirStore, exportRequest)
        .execute();
  }

  @Override
  public Operation deidentifyFhirStore(
      String sourcefhirStore, String destinationFhirStore, DeidentifyConfig deidConfig)
      throws IOException {
    DeidentifyFhirStoreRequest deidRequest = new DeidentifyFhirStoreRequest();
    deidRequest.setDestinationStore(destinationFhirStore);
    deidRequest.setConfig(deidConfig);
    return client
        .projects()
        .locations()
        .datasets()
        .fhirStores()
        .deidentify(sourcefhirStore, deidRequest)
        .execute();
  }

  @Override
  public Operation pollOperation(Operation operation, Long sleepMs)
      throws InterruptedException, IOException {
    LOG.debug(String.format("Operation %s started, polling until complete.", operation.getName()));
    while (operation.getDone() == null || !operation.getDone()) {
      // Update the status of the operation with another request.
      Thread.sleep(sleepMs); // Pause between requests.
      operation =
          client.projects().locations().datasets().operations().get(operation.getName()).execute();
    }
    return operation;
  }

  @Override
  public HttpBody executeFhirBundle(String fhirStore, String bundle) throws IOException {
    if (client == null) {
      initClient();
    }
    HttpBody httpBody = PARSER.fromString(bundle, HttpBody.class);

    return client
        .projects()
        .locations()
        .datasets()
        .fhirStores()
        .fhir()
        .executeBundle(fhirStore, httpBody)
        .execute();
  }

  /**
   * Wraps {@link HttpResponse} in an exception with a statusCode field for use with {@link
   * HealthcareIOError}.
   */
  public static class HealthcareHttpException extends Exception {
    private final int statusCode;

    private HealthcareHttpException(int statusCode, String message) {
      super(message);
      this.statusCode = statusCode;
      if (statusCode / 100 == 2) {
        throw new IllegalArgumentException(
            String.format(
                "2xx codes should not be exceptions. Got status code: %s with body: %s",
                statusCode, message));
      }
    }

    /**
     * Creates an exception from a non-OK response.
     *
     * @param statusCode the HTTP status code.
     * @param message the error message.
     * @return the healthcare http exception
     * @throws IOException the io exception
     */
    static HealthcareHttpException of(int statusCode, String message) {
      return new HealthcareHttpException(statusCode, message);
    }

    int getStatusCode() {
      return statusCode;
    }
  }

  @Override
  public HttpBody readFhirResource(String resourceName) throws IOException {
    return client
        .projects()
        .locations()
        .datasets()
        .fhirStores()
        .fhir()
        .read(resourceName)
        .execute();
  }

  @Override
  public HttpBody searchFhirResource(
      String fhirStore,
      String resourceType,
      @Nullable Map<String, Object> parameters,
      String pageToken)
      throws IOException {
    CloudHealthcareRequest<HttpBody> search;
    if (Strings.isNullOrEmpty(resourceType)) {
      search =
          client
              .projects()
              .locations()
              .datasets()
              .fhirStores()
              .fhir()
              .search(fhirStore, new SearchResourcesRequest());
    } else {
      search =
          client
              .projects()
              .locations()
              .datasets()
              .fhirStores()
              .fhir()
              .searchType(fhirStore, resourceType, new SearchResourcesRequest());
    }
    if (parameters != null && !parameters.isEmpty()) {
      parameters.forEach(search::set);
    }
    if (pageToken != null && !pageToken.isEmpty()) {
      search.set("_page_token", URLDecoder.decode(pageToken, "UTF-8"));
    }
    return search.execute();
  }

  @Override
  public HttpBody getPatientEverything(
      String resourceName, @Nullable Map<String, Object> filters, String pageToken)
      throws IOException {
    PatientEverything patientEverything =
        client
            .projects()
            .locations()
            .datasets()
            .fhirStores()
            .fhir()
            .patientEverything(resourceName);
    if (filters != null && !filters.isEmpty()) {
      filters.forEach(patientEverything::set);
    }
    if (pageToken != null && !pageToken.isEmpty()) {
      patientEverything.set("_page_token", URLDecoder.decode(pageToken, "UTF-8"));
    }
    return patientEverything.execute();
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
      requestHeaders.setUserAgent(USER_AGENT);
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
      this(client, hl7v2Store, start, end, null, null);
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
            }
            return !msgs.isEmpty();
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

  /** The type FhirResourcePagesIterator for methods which return paged output. */
  public static class FhirResourcePagesIterator implements Iterator<JsonArray> {

    public enum FhirMethod {
      SEARCH,
      PATIENT_EVERYTHING
    }

    private final FhirMethod fhirMethod;
    private final String fhirStore;
    private final String resourceType;
    private final String resourceName;
    private final Map<String, Object> parameters;

    private final HealthcareApiClient client;
    private final ObjectMapper mapper;
    private String pageToken;
    private boolean isFirstRequest;

    public FhirResourcePagesIterator(
        FhirMethod fhirMethod,
        HealthcareApiClient client,
        String fhirStore,
        String resourceType,
        String resourceName,
        @Nullable Map<String, Object> parameters) {
      this.fhirMethod = fhirMethod;
      this.client = client;
      this.fhirStore = fhirStore;
      this.resourceType = resourceType;
      this.resourceName = resourceName;
      this.parameters = parameters;
      this.pageToken = null;
      this.isFirstRequest = true;
      this.mapper = new ObjectMapper();
    }

    /**
     * Instantiates a new search FHIR resource pages iterator.
     *
     * @param client the client
     * @param fhirStore the Fhir store
     * @param resourceType the Fhir resource type to search for
     * @param parameters the query parameters
     */
    public static FhirResourcePagesIterator ofSearch(
        HealthcareApiClient client,
        String fhirStore,
        String resourceType,
        @Nullable Map<String, Object> parameters) {
      return new FhirResourcePagesIterator(
          FhirMethod.SEARCH, client, fhirStore, resourceType, "", parameters);
    }

    /**
     * Instantiates a new GetPatientEverything FHIR resource pages iterator.
     *
     * @param client the client
     * @param resourceName the FHIR resource name
     * @param parameters the filter parameters
     */
    public static FhirResourcePagesIterator ofPatientEverything(
        HealthcareApiClient client, String resourceName, @Nullable Map<String, Object> parameters) {
      return new FhirResourcePagesIterator(
          FhirMethod.PATIENT_EVERYTHING, client, "", "", resourceName, parameters);
    }

    @Override
    public boolean hasNext() throws NoSuchElementException {
      if (!isFirstRequest) {
        return this.pageToken != null && !this.pageToken.isEmpty();
      }
      try {
        HttpBody response = executeFhirRequest();
        JsonObject jsonResponse =
            JsonParser.parseString(mapper.writeValueAsString(response)).getAsJsonObject();
        JsonArray resources = jsonResponse.getAsJsonArray("entry");
        return resources != null && resources.size() != 0;
      } catch (IOException e) {
        throw new NoSuchElementException(
            String.format(
                "Failed to list first page of FHIR resources from %s: %s",
                fhirStore, e.getMessage()));
      }
    }

    @Override
    public JsonArray next() throws NoSuchElementException {
      try {
        HttpBody response = executeFhirRequest();
        this.isFirstRequest = false;
        JsonObject jsonResponse =
            JsonParser.parseString(mapper.writeValueAsString(response)).getAsJsonObject();
        JsonArray links = jsonResponse.getAsJsonArray("link");
        this.pageToken = parsePageToken(links);
        JsonArray resources = jsonResponse.getAsJsonArray("entry");
        return resources;
      } catch (IOException e) {
        this.pageToken = null;
        throw new NoSuchElementException(
            String.format("Error listing FHIR resources from %s: %s", fhirStore, e.getMessage()));
      }
    }

    private HttpBody executeFhirRequest() throws IOException {
      switch (fhirMethod) {
        case PATIENT_EVERYTHING:
          return client.getPatientEverything(resourceName, parameters, pageToken);
        case SEARCH:
        default:
          return client.searchFhirResource(fhirStore, resourceType, parameters, pageToken);
      }
    }

    private static String parsePageToken(JsonArray links) throws MalformedURLException {
      for (JsonElement e : links) {
        JsonObject link = e.getAsJsonObject();
        if (link.get("relation").getAsString().equalsIgnoreCase("next")) {
          URL url = new URL(link.get("url").getAsString());
          List<String> parameters = Splitter.on("&").splitToList(url.getQuery());
          for (String parameter : parameters) {
            List<String> parts = Splitter.on("=").limit(2).splitToList(parameter);
            if (parts.get(0).equalsIgnoreCase("_page_token")) {
              return parts.get(1);
            }
          }
        }
      }
      return "";
    }
  }
}
