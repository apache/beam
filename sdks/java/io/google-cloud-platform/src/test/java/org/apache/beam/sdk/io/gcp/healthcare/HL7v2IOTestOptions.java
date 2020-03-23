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

import javax.annotation.Nullable;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.testing.TestPipelineOptions;

public interface HL7v2IOTestOptions extends TestPipelineOptions {

  @Description(
      "HL7v2 store should match the pattern: projects/PROJECT_ID/locations/LOCATION/datasets/DATASET_ID/hl7V2Stores/HL7V2_STORE_ID/messages/MESSAGE_ID")
  @Required
  String getHl7v2Store();

  void setHl7v2Store(@Nullable String value);
}
