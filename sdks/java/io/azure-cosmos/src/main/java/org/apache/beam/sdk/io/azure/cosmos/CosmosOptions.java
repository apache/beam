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
package org.apache.beam.sdk.io.azure.cosmos;

import com.azure.cosmos.CosmosClientBuilder;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.io.azure.options.AzureOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;
import org.checkerframework.checker.nullness.qual.Nullable;

@Experimental(Experimental.Kind.SOURCE_SINK)
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public interface CosmosOptions extends AzureOptions {

  @JsonIgnore
  @Description(
      "The Azure Cosmos client builder. "
          + "If no client has been set explicitly, the default is to use the instance factory.")
  @Default.InstanceFactory(CosmosClientBuilderFactory.class)
  CosmosClientBuilder getCosmosClientBuilder();

  void setCosmosClientBuilder(CosmosClientBuilder builder);

  /** The Azure Cosmos service endpoint used by the Cosmos client. */
  @Description(
      "Sets the cosmos service endpoint, additionally parses it for information (SAS token)")
  @Nullable
  String getCosmosServiceEndpoint();

  void setCosmosServiceEndpoint(String endpoint);

  /** The Azure Cosmos key used to perform authentication for accessing resource */
  @Description(
      "Sets the cosmos service endpoint, additionally parses it for information (SAS token)")
  @Nullable
  String getCosmosKey();

  void setCosmosKey(String key);

  /** Create a cosmos client from the pipeline options */
  class CosmosClientBuilderFactory implements DefaultValueFactory<CosmosClientBuilder> {

    @Override
    public CosmosClientBuilder create(PipelineOptions options) {
      CosmosOptions cosmosOptions = options.as(CosmosOptions.class);
      CosmosClientBuilder builder = new CosmosClientBuilder();

      if (cosmosOptions.getAzureCredentialsProvider() != null) {
        builder = builder.credential(cosmosOptions.getAzureCredentialsProvider());
      }

      if (!Strings.isNullOrEmpty(cosmosOptions.getCosmosServiceEndpoint())) {
        builder = builder.endpoint(cosmosOptions.getCosmosServiceEndpoint());
      }

      if (!Strings.isNullOrEmpty(cosmosOptions.getCosmosKey())) {
        builder = builder.key(cosmosOptions.getCosmosKey());
      }

      return builder;
    }
  }
}
