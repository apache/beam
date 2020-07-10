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
package org.apache.beam.sdk.io.azure.blobstore;

import com.azure.storage.blob.BlobServiceClientBuilder;
import org.apache.beam.sdk.io.azure.options.BlobstoreClientBuilderFactory;
import org.apache.beam.sdk.io.azure.options.BlobstoreOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;

/** Construct BlobServiceClientBuilder with default values of Azure client properties. */
public class DefaultBlobstoreClientBuilderFactory implements BlobstoreClientBuilderFactory {

  // TODO: add any other options that should be passed to BlobServiceClientBuilder

  @Override
  public BlobServiceClientBuilder createBuilder(BlobstoreOptions blobstoreOptions) {
    BlobServiceClientBuilder builder = new BlobServiceClientBuilder();

    if (blobstoreOptions.getClientConfiguration() != null) {
      builder = builder.configuration(blobstoreOptions.getClientConfiguration());
    }

    if (blobstoreOptions.getAzureConnectionString() != null) {
      builder.connectionString(blobstoreOptions.getAzureConnectionString());
    }

    if (!Strings.isNullOrEmpty(blobstoreOptions.getAzureServiceEndpoint())) {
      builder = builder.endpoint(blobstoreOptions.getAzureServiceEndpoint());
    }
    return builder;
  }
}
