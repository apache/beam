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

import com.google.api.services.healthcare.v1.model.DeidentifyConfig;
import com.google.api.services.healthcare.v1.model.DicomStore;
import com.google.api.services.healthcare.v1.model.Empty;
import com.google.api.services.healthcare.v1.model.FhirStore;
import com.google.api.services.healthcare.v1.model.Hl7V2Store;
import com.google.api.services.healthcare.v1.model.HttpBody;
import com.google.api.services.healthcare.v1.model.IngestMessageResponse;
import com.google.api.services.healthcare.v1.model.ListMessagesResponse;
import com.google.api.services.healthcare.v1.model.Message;
import com.google.api.services.healthcare.v1.model.Operation;
import java.io.IOException;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.healthcare.HttpHealthcareApiClient.HealthcareHttpException;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/** Defines a client to communicate with the GCP HCLS API (version v1). */
public interface HealthcareApiClient {

  /**
   * Gets a Hl7v2 message by its name from a Hl7v2 store.
   *
   * @param msgName The message name to be retrieved.
   * @return The HL7v2 message.
   * @throws IOException The IO Exception.
   * @throws ParseException The Parse Exception.
   */
  Message getHL7v2Message(String msgName) throws IOException, ParseException;

  /**
   * Deletes an HL7v2 message.
   *
   * @param msgName The message name to be deleted.
   * @return Empty.
   * @throws IOException The IO Exception.
   */
  Empty deleteHL7v2Message(String msgName) throws IOException;

  /**
   * Gets an HL7v2 store.
   *
   * @param storeName The store name to be retrieved.
   * @return The HL7v2 store.
   * @throws IOException The IO Exception.
   */
  Hl7V2Store getHL7v2Store(String storeName) throws IOException;

  /**
   * Gets the earliest HL7v2 send time.
   *
   * @param hl7v2Store The HL7v2 store.
   * @param filter the filter to be matched on.
   * @return The earliest HL7v2 send time.
   * @throws IOException The IO Exception.
   */
  Instant getEarliestHL7v2SendTime(String hl7v2Store, @Nullable String filter) throws IOException;

  /**
   * Gets the latest HL7v2 send time.
   *
   * @param hl7v2Store The HL7v2 store.
   * @param filter The filter to be matched on.
   * @return The latest HL7v2 send time.
   * @throws IOException The IO Exception.
   */
  Instant getLatestHL7v2SendTime(String hl7v2Store, @Nullable String filter) throws IOException;

  /**
   * Time Bound HL7v2 list request.
   *
   * @param hl7v2Store The HL7v2 store.
   * @param start Start time.
   * @param end End time.
   * @param otherFilter The filter outside of the sendTime.
   * @param orderBy Order by.
   * @param pageToken The page token.
   * @return HTTP List response.
   * @throws IOException The IO Exception.
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
   * @param hl7v2Store The HL7v2 Store.
   * @param filter The Filter.
   * @param orderBy Order by.
   * @param pageToken The Page Token.
   * @return HTTP List response.
   * @throws IOException The IO Exception.
   */
  ListMessagesResponse makeHL7v2ListRequest(
      String hl7v2Store,
      @Nullable String filter,
      @Nullable String orderBy,
      @Nullable String pageToken)
      throws IOException;

  /**
   * Ingest an HL7v2 message.
   *
   * @param hl7v2Store The HL7v2 store of the message.
   * @param msg The message.
   * @return Empty.
   * @throws IOException The IO Exception.
   */
  IngestMessageResponse ingestHL7v2Message(String hl7v2Store, Message msg) throws IOException;

  /**
   * Creates an HL7v2 message.
   *
   * @param hl7v2Store The HL7v2 store to create a message in.
   * @param msg The message to create.
   * @return Empty.
   * @throws IOException The IO Exception.
   */
  Message createHL7v2Message(String hl7v2Store, Message msg) throws IOException;

  /**
   * Deletes an HL7v2 store.
   *
   * @param store The HL7v2 store to be deleted.
   * @return Empty.
   * @throws IOException The IO Exception.
   */
  Empty deleteHL7v2Store(String store) throws IOException;

  /**
   * Importing a FHIR resource from GCS.
   *
   * @param fhirStore the FhirStore to import into.
   * @param gcsSourcePath the GCS Path of resource.
   * @param contentStructure The content structure.
   * @return Empty.
   * @throws IOException the io exception
   */
  Operation importFhirResource(
      String fhirStore, String gcsSourcePath, @Nullable String contentStructure) throws IOException;

  /**
   * Export a FHIR Resource to GCS.
   *
   * @param fhirStore the FhirStore of the resource.
   * @param gcsDestinationPrefix GCS Destination Prefix to export to.
   * @return Empty.
   * @throws IOException The IO Exception.
   */
  Operation exportFhirResourceToGcs(String fhirStore, String gcsDestinationPrefix)
      throws IOException;

  /**
   * Export a FHIR Resource to BigQuery.
   *
   * @param fhirStore the FhirStore of the resource.
   * @param bigQueryDatasetUri The BQ Dataset URI to export to.
   * @return Empty.
   * @throws IOException The IO Exception.
   */
  Operation exportFhirResourceToBigQuery(String fhirStore, String bigQueryDatasetUri)
      throws IOException;

  /**
   * Deidentify a GCP FHIR Store and write the result into a new FHIR Store.
   *
   * @param sourceFhirStore the FhirStore to be deidentified.
   * @param destinationFhirStore the FhirStore that the deidentified data will be written to.
   * @param deidConfig the deidCongig specifying form of deidentification.
   * @return Empty.
   * @throws IOException The IO Exception.
   */
  Operation deidentifyFhirStore(
      String sourceFhirStore, String destinationFhirStore, DeidentifyConfig deidConfig)
      throws IOException;

  /**
   * Poll operation.
   *
   * @param operation to be polled.
   * @param sleepMs length of time to wait between requests.
   * @return HTTP Request (that returns status of operation).
   * @throws IOException The IO Exception.
   */
  Operation pollOperation(Operation operation, Long sleepMs)
      throws InterruptedException, IOException;

  /**
   * Execute fhir bundle http body.
   *
   * @param fhirStore the fhir store
   * @param bundle the bundle
   * @return the http body
   * @throws IOException The IO Exception.
   */
  HttpBody executeFhirBundle(String fhirStore, String bundle)
      throws IOException, HealthcareHttpException;

  /**
   * Read fhir resource http body.
   *
   * @param resourceName the resource name, in format
   *     projects/{p}/locations/{l}/datasets/{d}/fhirStores/{f}/fhir/{resourceType}/{id}
   * @return the http body
   * @throws IOException The IO Exception.
   */
  HttpBody readFhirResource(String resourceName) throws IOException;

  /**
   * Search fhir resource http body.
   *
   * @param fhirStore the fhir store
   * @param resourceType the resource type
   * @param parameters The parameters (in the form of key-value pairs).
   * @return the http body
   * @throws IOException The IO Exception.
   */
  HttpBody searchFhirResource(
      String fhirStore,
      String resourceType,
      @Nullable Map<String, Object> parameters,
      String pageToken)
      throws IOException;

  /**
   * Fhir get patient everything http body.
   *
   * @param resourceName the resource name, in format
   *     projects/{p}/locations/{l}/datasets/{d}/fhirStores/{f}/fhir/{resourceType}/{id}
   * @param filters optional request filters (in key value pairs).
   * @return the http body
   * @throws IOException The IO Exception.
   */
  HttpBody getPatientEverything(
      String resourceName, @Nullable Map<String, Object> filters, String pageToken)
      throws IOException;

  /**
   * Create hl 7 v 2 store hl 7 v 2 store.
   *
   * @param dataset The dataset to create the HL7v2 store in.
   * @param name The name of the store to be created.
   * @return Empty.
   * @throws IOException The IO Exception.
   */
  Hl7V2Store createHL7v2Store(String dataset, String name) throws IOException;

  /**
   * Create FHIR Store with a PubSub topic listener.
   *
   * @param dataset The name of Dataset for the FHIR store to be created in.
   * @param name The name of the FHIR store.
   * @param version The version of the FHIR store (DSTU2, STU3, R4).
   * @param pubsubTopic The pubsub topic listening to the FHIR store.
   * @throws IOException The IO Exception.
   */
  FhirStore createFhirStore(String dataset, String name, String version, String pubsubTopic)
      throws IOException;
  /**
   * Create FHIR Store.
   *
   * @param dataset The name of the Dataset for the FHIR store to be created in.
   * @param name The name of the FHIR store.
   * @param version The version of the FHIR store (DSTU2, STU3, R4).
   * @throws IOException The IO Exception.
   */
  FhirStore createFhirStore(String dataset, String name, String version) throws IOException;

  /**
   * List all FHIR stores in a dataset.
   *
   * @param dataset The dataset, in the format:
   *     projects/project_id/locations/location_id/datasets/dataset_id
   * @return A list of all FHIR stores in the dataset.
   * @throws IOException The IO Exception.
   */
  List<FhirStore> listAllFhirStores(String dataset) throws IOException;

  /**
   * Delete Fhir store.
   *
   * @param store The FHIR store to be deleted.
   * @return Empty.
   * @throws IOException The IO Exception.
   */
  Empty deleteFhirStore(String store) throws IOException;

  /**
   * Retrieve DicomStudyMetadata.
   *
   * @param dicomWebPath The Dicom Web Path to retrieve the metadata from.
   * @return The study metadata.
   * @throws IOException The IO Exception.
   */
  String retrieveDicomStudyMetadata(String dicomWebPath) throws IOException;

  /**
   * Create a DicomStore.
   *
   * @param dataset The dataset that the Dicom Store should be in, in the format:
   *     projects/project_id/locations/location_id/datasets/dataset_id.
   * @param name The name of the Dicom Store to be created.
   * @return Empty.
   * @throws IOException The IO Exception.
   */
  DicomStore createDicomStore(String dataset, String name) throws IOException;

  /**
   * Create a DicomStore with a PubSub listener.
   *
   * @param dataset The dataset that the Dicom Store should be in, in the format:
   *     projects/project_id/locations/location_id/datasets/dataset_id
   * @param name The name of the Dicom Store to be created.
   * @param pubsubTopic Name of PubSub topic connected with the Dicom store.
   * @return Empty.
   * @throws IOException The IO Exception.
   */
  DicomStore createDicomStore(String dataset, String name, String pubsubTopic) throws IOException;

  /**
   * Delete a Dicom Store.
   *
   * @param name The name of the Dicom Store to be deleted.
   * @return Empty.
   * @throws IOException The IO Exception.
   */
  Empty deleteDicomStore(String name) throws IOException;

  /**
   * Upload to a Dicom Store.
   *
   * @param webPath String format of webPath to upload into.
   * @param filePath filePath of file to upload.
   * @return Empty.
   * @throws IOException The IO Exception.
   */
  Empty uploadToDicomStore(String webPath, String filePath) throws IOException, URISyntaxException;
}
