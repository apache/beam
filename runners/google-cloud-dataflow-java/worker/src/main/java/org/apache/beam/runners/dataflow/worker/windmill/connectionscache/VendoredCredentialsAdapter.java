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
package org.apache.beam.runners.dataflow.worker.windmill.connectionscache;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;
import org.apache.beam.vendor.grpc.v1p54p0.com.google.auth.Credentials;
import org.apache.beam.vendor.grpc.v1p54p0.com.google.auth.RequestMetadataCallback;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;

/**
 * Create a wrapper around credentials that delegates to the underlying {@link
 * com.google.auth.Credentials}. Note that this class should override every method that is not final
 * and not static and call the delegate directly.
 *
 * <p>TODO: Replace this with an auto generated proxy which calls the underlying implementation
 * delegate to reduce maintenance burden.
 */
class VendoredCredentialsAdapter extends Credentials {

  private final Credentials credentials;

  VendoredCredentialsAdapter(@Nullable Credentials credentials) {
    this.credentials =
        Preconditions.checkNotNull(
            credentials, "gcpCredentials are required to communicate with Streaming Engine.");
  }

  @Override
  public String getAuthenticationType() {
    return credentials.getAuthenticationType();
  }

  @Override
  public Map<String, List<String>> getRequestMetadata() throws IOException {
    return credentials.getRequestMetadata();
  }

  @Override
  public void getRequestMetadata(URI uri, Executor executor, RequestMetadataCallback callback) {
    credentials.getRequestMetadata(
        uri, executor, new VendoredRequestMetadataCallbackAdapter(callback));
  }

  @Override
  public Map<String, List<String>> getRequestMetadata(URI uri) throws IOException {
    return credentials.getRequestMetadata(uri);
  }

  @Override
  public boolean hasRequestMetadata() {
    return credentials.hasRequestMetadata();
  }

  @Override
  public boolean hasRequestMetadataOnly() {
    return credentials.hasRequestMetadataOnly();
  }

  @Override
  public void refresh() throws IOException {
    credentials.refresh();
  }

  /**
   * Create a wrapper around credentials callback that delegates to the underlying vendored {@link
   * com.google.auth.RequestMetadataCallback}. Note that this class should override every method
   * that is not final and not static and call the delegate directly.
   *
   * <p>TODO: Replace this with an auto generated proxy which calls the underlying implementation
   * delegate to reduce maintenance burden.
   */
  private static class VendoredRequestMetadataCallbackAdapter implements RequestMetadataCallback {

    private final RequestMetadataCallback callback;

    private VendoredRequestMetadataCallbackAdapter(RequestMetadataCallback callback) {
      this.callback = callback;
    }

    @Override
    public void onSuccess(Map<String, List<String>> metadata) {
      callback.onSuccess(metadata);
    }

    @Override
    public void onFailure(Throwable exception) {
      callback.onFailure(exception);
    }
  }
}
