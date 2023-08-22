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
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.healthcare.v1beta1.CloudHealthcare;
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
import com.google.api.services.healthcare.v1beta1.model.FhirStore;
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
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
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
public class HttpHealthcareApiClientv1beta1 implements HealthcareApiClientv1beta1, Serializable {
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
  public HttpHealthcareApiClientv1beta1() throws IOException {
    initClient();
  }

  /**
   * Instantiates a new Http healthcare api client.
   *
   * @param client the client
   * @throws IOException the io exception
   */
  public HttpHealthcareApiClientv1beta1(CloudHealthcare client) throws IOException {
    this.client = client;
    this.httpClient = HttpClients.createDefault();
    initClient();
  }

  public JsonFactory getJsonFactory() {
    return this.client.getJsonFactory();
  }
  
  @Override
  public Empty conditionalDelete(String parent, String type) throws IOException {
    return client.projects().locations().datasets().fhirStores().fhir().conditionalDelete(parent, type).execute();
  }
}