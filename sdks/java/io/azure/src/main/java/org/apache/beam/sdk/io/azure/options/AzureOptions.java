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
import com.azure.core.util.Configuration;
import com.azure.identity.DefaultAzureCredentialBuilder;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface AzureOptions extends PipelineOptions {

  // TODO: Add any other azure options that users should be able to configure
  // TODO: Confirm that Azure options are in this file, Blobstore options in BlobstoreOptions

  /** The Azure service endpoint used by the Azure client. */
  @Description("Azure service endpoint used by the Azure client")
  String getAzureServiceEndpoint();

  void setAzureServiceEndpoint(String value);

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

  /** The client configuration instance that should be used to configure Azure service clients. */
  @Description(
      "The client configuration instance that should be used to configure Azure service clients")
  @Default.InstanceFactory(ConfigurationFactory.class)
  Configuration getClientConfiguration();

  void setClientConfiguration(Configuration configuration);

  /** The client configuration instance that should be used to configure Azure service clients. */
  @Description(
      "The client configuration instance that should be used to configure Azure http client configuration parameters."
          + "Mentioned parameters are the available parameters that can be set. Set only those that need custom changes.")
  @Default.InstanceFactory(ConfigurationFactory.class)
  Configuration getAzureHttpConfiguration();

  void setAzureHttpConfiguration(Configuration configuration);

  /** Default Azure client configuration. */
  class ConfigurationFactory implements DefaultValueFactory<Configuration> {

    @Override
    public Configuration create(PipelineOptions options) {
      return new Configuration();
    }
  }
}
