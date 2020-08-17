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

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.io.azure.blobstore.DefaultBlobstoreClientBuilderFactory;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

@Experimental(Kind.FILESYSTEM)
public interface BlobstoreOptions extends AzureOptions {

  // TODO: Add any other blobstore options that users should be able to configure

  @Description(
      "Factory class that should be created and used to create a builder of Azure Blobstore client."
          + "Override the default value if you need a Azure client with custom properties.")
  @Default.Class(DefaultBlobstoreClientBuilderFactory.class)
  Class<? extends BlobstoreClientBuilderFactory> getBlobstoreClientFactoryClass();

  void setBlobstoreClientFactoryClass(
      Class<? extends BlobstoreClientBuilderFactory> blobstoreClientFactoryClass);

  String getAzureConnectionString();

  void setAzureConnectionString(String connectionString);
}
