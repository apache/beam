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
package org.apache.beam.sdk.extensions.sql.meta.provider.datacatalog;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/** Pipeline options for Data Catalog table provider. */
public interface DataCatalogPipelineOptions extends PipelineOptions {

  /** DataCatalog endpoint. */
  @Description("Data catalog endpoint.")
  @Validation.Required
  @Default.String("datacatalog.googleapis.com:443")
  String getDataCatalogEndpoint();

  void setDataCatalogEndpoint(String dataCatalogEndpoint);

  /** Whether to truncate timestamps in tables described by Data Catalog. */
  @Description("Truncate sub-millisecond precision timestamps in tables described by Data Catalog")
  @Validation.Required
  @Default.Boolean(false)
  boolean getTruncateTimestamps();

  void setTruncateTimestamps(boolean newValue);
}
