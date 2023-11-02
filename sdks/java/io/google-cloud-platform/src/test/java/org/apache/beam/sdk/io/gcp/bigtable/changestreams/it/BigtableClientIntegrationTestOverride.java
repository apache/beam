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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams.it;

import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminSettings;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.errorprone.annotations.CheckReturnValue;
import java.io.Serializable;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.BigtableClientOverride;

/** Implements BigtableClientOverride to override data and admin endpoints. */
@CheckReturnValue
final class BigtableClientIntegrationTestOverride implements Serializable, BigtableClientOverride {
  private static final long serialVersionUID = 4188505491566837311L;

  // The address of the admin API endpoint.
  private static final String ADMIN_ENDPOINT_ENV_VAR =
      getenv("BIGTABLE_ENV_ADMIN_ENDPOINT", "bigtableadmin.googleapis.com:443");
  // The address of the data API endpoint.
  private static final String DATA_ENDPOINT_ENV_VAR =
      getenv("BIGTABLE_ENV_DATA_ENDPOINT", "bigtable.googleapis.com:443");

  private final String adminEndpoint;
  private final String dataEndpoint;

  @Override
  public String toString() {
    return "BigtableClientIntegrationTestOverride{"
        + "adminEndpoint="
        + adminEndpoint
        + ", dataEndpoint="
        + dataEndpoint
        + "}";
  }

  /** Applies the test environment settings to the builder. */
  @Override
  public void updateInstanceAdminClientSettings(BigtableInstanceAdminSettings.Builder builder) {
    builder.stubSettings().setEndpoint(adminEndpoint);
  }

  /** Applies the test environment settings to the builder. */
  @Override
  public void updateTableAdminClientSettings(BigtableTableAdminSettings.Builder builder) {
    builder.stubSettings().setEndpoint(adminEndpoint);
  }

  /** Applies the test environment settings to the builder. */
  @Override
  public void updateDataClientSettings(BigtableDataSettings.Builder builder) {
    builder.stubSettings().setEndpoint(dataEndpoint);
  }

  /** Returns the value of the environment variable, or default string if not found. */
  private static String getenv(String name, String defaultValue) {
    final String value = System.getenv(name);
    if (value != null) {
      return value;
    }
    return defaultValue;
  }

  BigtableClientIntegrationTestOverride() {
    adminEndpoint = ADMIN_ENDPOINT_ENV_VAR;
    dataEndpoint = DATA_ENDPOINT_ENV_VAR;
  }
}
