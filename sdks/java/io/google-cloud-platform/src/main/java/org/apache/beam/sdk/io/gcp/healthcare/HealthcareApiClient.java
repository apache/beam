// Copyright 2020 Google LLC.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package org.apache.beam.sdk.io.gcp.healthcare;

import com.google.api.services.healthcare.v1beta1.model.Empty;
import com.google.api.services.healthcare.v1beta1.model.FhirStore;
import com.google.api.services.healthcare.v1beta1.model.Hl7V2Store;
import com.google.api.services.healthcare.v1beta1.model.HttpBody;
import com.google.api.services.healthcare.v1beta1.model.IngestMessageResponse;
import com.google.api.services.healthcare.v1beta1.model.ListMessagesResponse;
import com.google.api.services.healthcare.v1beta1.model.Message;
import com.google.api.services.healthcare.v1beta1.model.Operation;
import com.google.api.services.healthcare.v1beta1.model.ParserConfig;
import java.io.IOException;
import java.text.ParseException;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.healthcare.HttpHealthcareApiClient.HealthcareHttpException;
import org.joda.time.Instant;

/** Defines a client that talks to the Cloud Healthcare API. */
public interface HealthcareApiClient {

  /**
   * Fetches an Hl7v2 message by its name from a Hl7v2 store.
   *
   * @param msgName the msg name
   * @return HL7v2 message
   * @throws IOException the io exception
   * @throws ParseException the parse exception
   */
  Message getHL7v2Message(String msgName) throws IOException, ParseException;

  /**
   * Delete hl 7 v 2 message empty.
   *
   * @param msgName the msg name
   * @return the empty
   * @throws IOException the io exception
   */
  Empty deleteHL7v2Message(String msgName) throws IOException;

  /**
   * Gets HL7v2 store.
   *
   * @param storeName the store name
   * @return the HL7v2 store
   * @throws IOException the io exception
   */
  Hl7V2Store getHL7v2Store(String storeName) throws IOException;

  /**
   * Gets earliest hl 7 v 2 send time.
   *
   * @param hl7v2Store the hl 7 v 2 store
   * @param filter the filter
   * @return the earliest hl 7 v 2 send time
   * @throws IOException the io exception
   */
  Instant getEarliestHL7v2SendTime(String hl7v2Store, @Nullable String filter) throws IOException;

  Instant getLatestHL7v2SendTime(String hl7v2Store, @Nullable String filter) throws IOException;

  /**
   * Make send time bound hl 7 v 2 list request.
   *
   * @param hl7v2Store the hl 7 v 2 store
   * @param start the start
   * @param end the end
   * @param otherFilter the other filter
   * @param orderBy the order by
   * @param pageToken the page token
   * @return the list messages response
   * @throws IOException the io exception
   */
  ListMessagesResponse makeSendTimeBoundHL7v2ListRequest(
      String hl7v2Store,
      Instant start,
      @Nullable Instant end,
      @Nullable String otherFilter,
      @Nullable String orderBy,
      @Nullable String pageToken)
      throws IOException;

  /**
   * Make hl 7 v 2 list request list messages response.
   *
   * @param hl7v2Store the hl 7 v 2 store
   * @param filter the filter
   * @param orderBy the order by
   * @param pageToken the page token
   * @return the list messages response
   * @throws IOException the io exception
   */
  ListMessagesResponse makeHL7v2ListRequest(
      String hl7v2Store,
      @Nullable String filter,
      @Nullable String orderBy,
      @Nullable String pageToken)
      throws IOException;

  /**
   * Ingest hl 7 v 2 message ingest message response.
   *
   * @param hl7v2Store the hl 7 v 2 store
   * @param msg the msg
   * @return the ingest message response
   * @throws IOException the io exception
   */
  IngestMessageResponse ingestHL7v2Message(String hl7v2Store, Message msg) throws IOException;

  /**
   * Create hl 7 v 2 message message.
   *
   * @param hl7v2Store the hl 7 v 2 store
   * @param msg the msg
   * @return the message
   * @throws IOException the io exception
   */
  Message createHL7v2Message(String hl7v2Store, Message msg) throws IOException;

  Operation importFhirResource(
      String fhirStore, String gcsSourcePath, @Nullable String contentStructure) throws IOException;

  Operation exportHl7v2Messages(String hl7v2Store, String start, String end, String gcsPrefix)
      throws IOException, HealthcareHttpException;

  Operation pollOperation(Operation operation, Long sleepMs)
      throws InterruptedException, IOException;

  /**
   * Execute fhir bundle http body.
   *
   * @param fhirStore the fhir store
   * @param bundle the bundle
   * @return the http body
   * @throws IOException the io exception
   */
  HttpBody executeFhirBundle(String fhirStore, String bundle)
      throws IOException, HealthcareHttpException;

  /**
   * Read fhir resource http body.
   *
   * @param resourceId the resource
   * @return the http body
   * @throws IOException the io exception
   */
  HttpBody readFhirResource(String resourceId) throws IOException;

  /**
   * Search fhir resource http body.
   *
   * @param fhirStore the fhir store
   * @param resourceType the resource type
   * @param parameters the parameters
   * @return the http body
   * @throws IOException
   */
  HttpBody searchFhirResource(String fhirStore, String resourceType,
      @Nullable Map<String, Object> parameters) throws IOException;

  /**
   * Create hl 7 v 2 store hl 7 v 2 store.
   *
   * @param dataset the dataset
   * @param name the name
   * @return the hl 7 v 2 store
   * @throws IOException the io exception
   */
  Hl7V2Store createHL7v2Store(String dataset, String name) throws IOException;

  Hl7V2Store createHL7v2Store(String dataset, String name, @Nullable ParserConfig parserConfig,
      @Nullable String pubsubTopic) throws IOException;

  FhirStore createFhirStore(String dataset, String name, String version,
      @Nullable String pubsubTopic) throws IOException;

  FhirStore createFhirStore(String dataset, String name, String version) throws IOException;

  /**
   * Delete hl 7 v 2 store empty.
   *
   * @param store the store
   * @return the empty
   * @throws IOException the io exception
   */
  Empty deleteHL7v2Store(String store) throws IOException;

  Empty deleteFhirStore(String store) throws IOException;


  /**
   * Retrieve metadata of a study in a dicom Store
   * @param fullWebPath the full path of the study
   * @return the metadata of the study as a json string
   * @throws IOException the io exception
   */
  String retrieveStudyMetadata(String fullWebPath) throws IOException;
}
