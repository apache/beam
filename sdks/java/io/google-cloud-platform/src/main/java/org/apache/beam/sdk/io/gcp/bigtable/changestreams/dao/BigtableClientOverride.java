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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao;

import com.google.cloud.bigtable.admin.v2.BigtableInstanceAdminSettings;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import java.io.IOException;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;

/** Override the configuration of Cloud Bigtable data and admin client. */
@VisibleForTesting
public interface BigtableClientOverride {
  /**
   * Update {@link BigtableInstanceAdminSettings.Builder} with custom configurations.
   *
   * <p>For example, to update the admin api endpoint.
   *
   * @param builder builds the instance admin client
   * @throws IOException when dependency initialization fails
   */
  void updateInstanceAdminClientSettings(BigtableInstanceAdminSettings.Builder builder)
      throws IOException;

  /**
   * Update {@link BigtableTableAdminSettings.Builder} with custom configurations.
   *
   * <p>For example, to update the admin api endpoint.
   *
   * @param builder builds the table admin client
   * @throws IOException when dependency initialization fails
   */
  void updateTableAdminClientSettings(BigtableTableAdminSettings.Builder builder)
      throws IOException;

  /**
   * Update {@link BigtableDataSettings.Builder} with custom configurations.
   *
   * @param builder builds the data client
   * @throws IOException when dependency initialization fails
   */
  void updateDataClientSettings(BigtableDataSettings.Builder builder) throws IOException;
}
