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

import com.google.api.services.healthcare.v1beta1.model.DeidentifyConfig;
import com.google.api.services.healthcare.v1beta1.model.DicomStore;
import com.google.api.services.healthcare.v1beta1.model.Empty;
import com.google.api.services.healthcare.v1beta1.model.FhirStore;
import com.google.api.services.healthcare.v1beta1.model.Hl7V2Store;
import com.google.api.services.healthcare.v1beta1.model.HttpBody;
import com.google.api.services.healthcare.v1beta1.model.IngestMessageResponse;
import com.google.api.services.healthcare.v1beta1.model.ListMessagesResponse;
import com.google.api.services.healthcare.v1beta1.model.Message;
import com.google.api.services.healthcare.v1beta1.model.Operation;
import java.io.IOException;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.gcp.healthcare.HttpHealthcareApiClient.HealthcareHttpException;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
  /** Defines a client that talks to the Cloud Healthcare API (version v1beta1). */

public interface HealthcareApiClient {
  Empty conditionalDelete(String parent, String type) throws IOException;
}
