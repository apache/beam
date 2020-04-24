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

import com.google.api.services.healthcare.v1beta1.model.Empty;
import com.google.api.services.healthcare.v1beta1.model.Hl7V2Store;
import com.google.api.services.healthcare.v1beta1.model.HttpBody;
import com.google.api.services.healthcare.v1beta1.model.IngestMessageResponse;
import com.google.api.services.healthcare.v1beta1.model.ListMessagesResponse;
import com.google.api.services.healthcare.v1beta1.model.Message;
import com.google.api.services.healthcare.v1beta1.model.SearchResourcesRequest;
import java.io.IOException;
import java.text.ParseException;
import javax.annotation.Nullable;

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
   * Make hl 7 v 2 list request list messages response.
   *
   * @param hl7v2Store the hl 7 v 2 store
   * @param filter the filter
   * @param pageToken the page token
   * @return the list messages response
   * @throws IOException the io exception
   */
  ListMessagesResponse makeHL7v2ListRequest(
      String hl7v2Store, @Nullable String filter, @Nullable String pageToken) throws IOException;

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

  /**
   * Create fhir resource http body.
   *
   * @param fhirStore the fhir store
   * @param type the type
   * @param body the body
   * @return the http body
   * @throws IOException the io exception
   */
  HttpBody createFhirResource(String fhirStore, String type, HttpBody body) throws IOException;

  HttpBody fhirSearch(String fhirStore, SearchResourcesRequest query) throws IOException;
  /**
   * Execute fhir bundle http body.
   *
   * @param fhirStore the fhir store
   * @param bundle the bundle
   * @return the http body
   * @throws IOException the io exception
   */
  HttpBody executeFhirBundle(String fhirStore, HttpBody bundle) throws IOException;

  /**
   * List fhir resource for patient http body.
   *
   * @param fhirStore the fhir store
   * @param patient the patient
   * @return the http body
   * @throws IOException the io exception
   */
  HttpBody listFHIRResourceForPatient(String fhirStore, String patient) throws IOException;

  /**
   * Read fhir resource http body.
   *
   * @param fhirStore the fhir store
   * @param resource the resource
   * @return the http body
   * @throws IOException the io exception
   */
  HttpBody readFHIRResource(String fhirStore, String resource) throws IOException;

  Hl7V2Store createHL7v2Store(String dataset, String name) throws IOException;

  Empty deleteHL7v2Store(String store) throws IOException;
}
