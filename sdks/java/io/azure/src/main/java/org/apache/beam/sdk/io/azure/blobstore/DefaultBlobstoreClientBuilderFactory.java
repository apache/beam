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
import com.azure.storage.common.StorageSharedKeyCredential;
import org.apache.beam.sdk.io.azure.options.BlobstoreClientBuilderFactory;
import org.apache.beam.sdk.io.azure.options.BlobstoreOptions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;

/** Construct BlobServiceClientBuilder with given values of Azure client properties. */
public class DefaultBlobstoreClientBuilderFactory implements BlobstoreClientBuilderFactory {

  @Override
  public BlobServiceClientBuilder createBuilder(BlobstoreOptions blobstoreOptions) {
    BlobServiceClientBuilder builder = new BlobServiceClientBuilder();

    if (!Strings.isNullOrEmpty(blobstoreOptions.getAzureConnectionString())) {
      builder = builder.connectionString(blobstoreOptions.getAzureConnectionString());
    }

    if (blobstoreOptions.getSharedKeyCredential() != null) {
      builder = builder.credential(blobstoreOptions.getSharedKeyCredential());
    }

    if (blobstoreOptions.getTokenCredential() != null) {
      builder = builder.credential(blobstoreOptions.getTokenCredential());
    }

    if (!Strings.isNullOrEmpty(blobstoreOptions.getSasToken())) {
      builder = builder.sasToken(blobstoreOptions.getSasToken());
    }

    if (!Strings.isNullOrEmpty(blobstoreOptions.getAccountName())
        && !Strings.isNullOrEmpty(blobstoreOptions.getAccessKey())) {
      StorageSharedKeyCredential credential =
          new StorageSharedKeyCredential(
              blobstoreOptions.getAccountName(), blobstoreOptions.getAccessKey());
      builder = builder.credential(credential);
    }

    if (!Strings.isNullOrEmpty(blobstoreOptions.getBlobServiceEndpoint())) {
      builder = builder.endpoint(blobstoreOptions.getBlobServiceEndpoint());
    }

    if (blobstoreOptions.getCustomerProvidedKey() != null) {
      builder = builder.customerProvidedKey(blobstoreOptions.getCustomerProvidedKey());
    }

    if (blobstoreOptions.getEnvironmentConfiguration() != null) {
      builder = builder.configuration(blobstoreOptions.getEnvironmentConfiguration());
    }

    if (blobstoreOptions.getPipelinePolicy() != null) {
      builder = builder.addPolicy(blobstoreOptions.getPipelinePolicy());
    }

    if (blobstoreOptions.getHttpClient() != null) {
      builder = builder.httpClient(blobstoreOptions.getHttpClient());
    }

    if (blobstoreOptions.getHttpPipeline() != null) {
      builder = builder.pipeline(blobstoreOptions.getHttpPipeline());
    }

    return builder;
  }
}
