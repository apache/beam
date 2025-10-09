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
package org.apache.beam.sdk.util;

import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import java.io.IOException;

/**
 * A {@link Secret} manager implementation that retrieves secrets from Google Cloud Secret Manager.
 */
public class GcpSecret implements Secret {
  private final String versionName;

  /**
   * Initializes a {@link GcpSecret} object.
   *
   * @param versionName The full version name of the secret in Google Cloud Secret Manager. For
   *     example: projects/<id>/secrets/<secret_name>/versions/1. For more info, see
   *     https://cloud.google.com/python/docs/reference/secretmanager/latest/google.cloud.secretmanager_v1beta1.services.secret_manager_service.SecretManagerServiceClient#google_cloud_secretmanager_v1beta1_services_secret_manager_service_SecretManagerServiceClient_access_secret_version
   */
  public GcpSecret(String versionName) {
    this.versionName = versionName;
  }

  /**
   * Returns the secret as a byte array. Assumes that the current active service account has
   * permissions to read the secret.
   *
   * @return The secret as a byte array.
   */
  @Override
  public byte[] getSecretBytes() {
    try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
      SecretVersionName secretVersionName = SecretVersionName.parse(versionName);
      AccessSecretVersionResponse response = client.accessSecretVersion(secretVersionName);
      return response.getPayload().getData().toByteArray();
    } catch (IOException e) {
      throw new RuntimeException("Failed to retrieve secret bytes", e);
    }
  }
}
