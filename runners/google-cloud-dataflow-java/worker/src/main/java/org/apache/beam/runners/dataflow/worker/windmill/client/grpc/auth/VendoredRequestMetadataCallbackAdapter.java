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
package org.apache.beam.runners.dataflow.worker.windmill.client.grpc.auth;

import java.util.List;
import java.util.Map;

/**
 * Create a wrapper around credentials callback that delegates to the underlying vendored {@link
 * com.google.auth.RequestMetadataCallback}. Note that this class should override every method that
 * is not final and not static and call the delegate directly.
 *
 * <p>TODO: Replace this with an auto generated proxy which calls the underlying implementation
 * delegate to reduce maintenance burden.
 */
public class VendoredRequestMetadataCallbackAdapter
    implements com.google.auth.RequestMetadataCallback {

  private final org.apache.beam.vendor.grpc.v1p54p0.com.google.auth.RequestMetadataCallback
      callback;

  VendoredRequestMetadataCallbackAdapter(
      org.apache.beam.vendor.grpc.v1p54p0.com.google.auth.RequestMetadataCallback callback) {
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
