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

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Collection;
import java.util.List;
import org.apache.beam.sdk.io.FileSystem;
import org.apache.beam.sdk.io.fs.CreateOptions;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;

class AzureBlobStoreFileSystem extends FileSystem<AzfsResourceId> {

  @Override
  protected String getScheme() {
    return "azfs";
  }

  @Override
  protected List<MatchResult> match(List<String> specs) throws IOException {
    // TODO
    return null;
  }

  @Override
  protected WritableByteChannel create(AzfsResourceId resourceId, CreateOptions createOptions)
      throws IOException {
    // TODO
    return null;
  }

  @Override
  protected ReadableByteChannel open(AzfsResourceId resourceId) throws IOException {
    // TODO
    return null;
  }

  @Override
  protected void copy(List<AzfsResourceId> srcPaths, List<AzfsResourceId> destPaths)
      throws IOException {
    // TODO
  }

  @VisibleForTesting
  void copy(AzfsResourceId sourcePath, AzfsResourceId destinationPath) throws IOException {
    // TODO
  }

  @Override
  protected void rename(List<AzfsResourceId> srcResourceIds, List<AzfsResourceId> destResourceIds)
      throws IOException {
    // TODO
  }

  @Override
  protected void delete(Collection<AzfsResourceId> resourceIds) throws IOException {
    // TODO
  }

  @Override
  protected AzfsResourceId matchNewResource(String singleResourceSpec, boolean isDirectory) {
    // TODO
    return null;
  }
}
