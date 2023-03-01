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
package org.apache.beam.sdk.io.azure.options;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface AzureOptions extends PipelineOptions {

  /* Refer to {@link DefaultAWSCredentialsProviderChain} Javadoc for usage help. */

  /**
   * The credential instance that should be used to authenticate against Azure services. The option
   * value must contain a "@type" field and an Azure credentials provider class as the field value.
   *
   * <p>For example, to specify the Azure client id, tenant id, and client secret, specify the
   * following: <code>
   *     {"@type" : "ClientSecretCredential", "azureClientId": "client_id_value",
   *     "azureTenantId": "tenant_id_value", "azureClientSecret": "client_secret_value"}
   * </code>
   */
  @Description(
      "The credential instance that should be used to authenticate "
          + "against Azure services. The option value must contain \"@type\" field "
          + "and an Azure credentials provider class name as the field value. "
          + " For example, to specify the Azure client id, tenant id, and client secret, specify the following: "
          + "{\"@type\" : \"ClientSecretCredential\", \"azureClientId\": \"client_id_value\", "
          + "\"azureTenantId\": \"tenant_id_value\", \"azureClientSecret\": \"client_secret_value\"}")
  @Default.InstanceFactory(AzureUserCredentialsFactory.class)
  TokenCredential getAzureCredentialsProvider();

  void setAzureCredentialsProvider(TokenCredential value);

  /** Attempts to load Azure credentials. */
  class AzureUserCredentialsFactory implements DefaultValueFactory<TokenCredential> {

    @Override
    public TokenCredential create(PipelineOptions options) {
      return new DefaultAzureCredentialBuilder().build();
    }
  }
}
