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
package org.apache.beam.it.gcp.kms;

import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.cloud.kms.v1.KeyManagementServiceSettings;
import java.io.IOException;

/** KMS Client Factory class. */
class KMSClientFactory {

  private final CredentialsProvider credentialsProvider;

  KMSClientFactory(CredentialsProvider credentialsProvider) {
    this.credentialsProvider = credentialsProvider;
  }

  /**
   * Returns a KMS client for connection to Google Cloud KMS.
   *
   * @return KMS client.
   */
  KeyManagementServiceClient getKMSClient() {
    KeyManagementServiceSettings.Builder settings = KeyManagementServiceSettings.newBuilder();

    if (credentialsProvider != null) {
      settings.setCredentialsProvider(credentialsProvider);
    }

    try {
      return KeyManagementServiceClient.create(settings.build());
    } catch (IOException e) {
      throw new KMSResourceManagerException("Failed to create KMS client.", e);
    }
  }
}
