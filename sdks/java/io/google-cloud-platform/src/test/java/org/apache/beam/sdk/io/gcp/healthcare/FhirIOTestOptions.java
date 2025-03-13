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

import static org.apache.beam.sdk.io.gcp.healthcare.FhirIOTestUtil.DEFAULT_TEMP_BUCKET;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation.Required;

public interface FhirIOTestOptions extends HealthcareStoreTestPipelineOptions {

  @Description(
      "FHIR store should match the pattern: projects/PROJECT_ID/locations/LOCATION/datasets/DATASET_ID/fhirStores/HL7V2_STORE_ID")
  @Required
  String getFhirStore();

  void setFhirStore(String value);

  @Description("GCS temp path for import should be of the form gs://bucket/path/")
  @Default.String("gs://" + DEFAULT_TEMP_BUCKET + "/FhirIOWriteIT/temp/")
  String getGcsTempPath();

  void setGcsTempPath(String value);

  @Description("GCS dead letter path for import should be of the form gs://bucket/path/")
  String getGcsDeadLetterPath();

  void setGcsDeadLetterPath(String value);
}
