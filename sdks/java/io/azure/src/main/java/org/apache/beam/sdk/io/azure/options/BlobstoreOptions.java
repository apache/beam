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
import com.azure.core.http.HttpClient;
import com.azure.core.http.HttpPipeline;
import com.azure.core.http.policy.HttpPipelinePolicy;
import com.azure.core.util.Configuration;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.models.CustomerProvidedKey;
import com.azure.storage.common.StorageSharedKeyCredential;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.io.azure.blobstore.DefaultBlobstoreClientBuilderFactory;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.nullness.qual.Nullable;

// TODO: Tag each option with @Default or @Nullable

@Experimental
/** Options used to configure Microsoft Azure Blob Storage. */
public interface BlobstoreOptions extends PipelineOptions {

  @Description(
      "Factory class that should be created and used to create a builder of Azure Blobstore client."
          + "Override the default value if you need a Azure client with custom properties.")
  @Default.Class(DefaultBlobstoreClientBuilderFactory.class)
  Class<? extends BlobstoreClientBuilderFactory> getBlobstoreClientFactoryClass();

  void setBlobstoreClientFactoryClass(
      Class<? extends BlobstoreClientBuilderFactory> blobstoreClientFactoryClass);

  @Description("Adds a pipeline policy to apply on each request sent to the blob service client.")
  @Nullable
  HttpPipelinePolicy getPipelinePolicy();

  void setPipelinePolicy(HttpPipelinePolicy pipelinePolicy);

  /** The client configuration instance that should be used to configure Azure service clients. */
  @Description(
      "The configuration instance used to retrieve environment configuration values "
          + "when building an Azure Blobstore client. Set only those that need custom changes.")
  @Default.InstanceFactory(BlobstoreOptions.ConfigurationFactory.class)
  @Nullable
  Configuration getEnvironmentConfiguration();

  void setEnvironmentConfiguration(Configuration configuration);

  /** Default Azure client configuration. */
  class ConfigurationFactory implements DefaultValueFactory<Configuration> {

    @Override
    public Configuration create(PipelineOptions options) {
      return new Configuration();
    }
  }

  @Description("Sets the connection string to connect to the Azure Blobstore client.")
  String getAzureConnectionString();

  void setAzureConnectionString(String connectionString);

  @Description("Sets a StorageSharedKeyCredential used to authorize requests sent to the service.")
  StorageSharedKeyCredential getSharedKeyCredential();

  void setSharedKeyCredential(StorageSharedKeyCredential sharedKeyCredential);

  @Description("Sets a TokenCredential used to authorize requests sent to the service.")
  TokenCredential getTokenCredential();

  void setTokenCredential(TokenCredential tokenCredential);

  @Description("Sets the SAS token used to authorize requests sent to the service.")
  String getSasToken();

  void setSasToken(String sasToken);

  @Description("Blobstore account name")
  String getAccountName();

  void setAccountName(String name);

  @Description("Azure Blobstore access key")
  String getAccessKey();

  void setAccessKey(String key);

  @Description(
      "Sets the customer provided key that is used to encrypt blob contents on the server.")
  CustomerProvidedKey getCustomerProvidedKey();

  void setCustomerProvidedKey(CustomerProvidedKey customerProvidedKey);

  /** The Azure Blobstore service endpoint used by the Blob service client. */
  @Description("Sets the blob service endpoint, additionally parses it for information (SAS token)")
  @Nullable
  String getBlobServiceEndpoint();

  void setBlobServiceEndpoint(String endpoint);

  @Description(
      "Sets the HttpClient to use for sending a receiving requests to and from the service.")
  @Nullable
  HttpClient getHttpClient();

  void setHttpClient(HttpClient httpClient);

  @Description("Sets the HttpPipeline to use for the service client.")
  @Nullable
  HttpPipeline getHttpPipeline();

  void setHttpPipeline(HttpPipeline httpPipeline);

  /**
   * The credential instance that should be used to authenticate against Azure services. The option
   * value must contain a "@type" field and an Azure credentials provider class as the field value.
   */
  @Description(
      "The credential instance that should be used to authenticate "
          + "against Azure services. The option value must contain \"@type\" field "
          + "and an Azure credentials provider class name as the field value.")
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
