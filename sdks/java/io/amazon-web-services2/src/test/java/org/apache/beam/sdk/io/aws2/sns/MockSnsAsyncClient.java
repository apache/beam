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
package org.apache.beam.sdk.io.aws2.sns;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

final class MockSnsAsyncClient extends MockSnsAsyncBaseClient {
  private final int statusCode;

  private MockSnsAsyncClient(int statusCode) {
    this.statusCode = statusCode;
  }

  static MockSnsAsyncClient withStatusCode(int statusCode) {
    return new MockSnsAsyncClient(statusCode);
  }

  @Override
  public CompletableFuture<PublishResponse> publish(PublishRequest publishRequest) {
    PublishResponse response = mock(PublishResponse.class);
    SdkHttpResponse metadata = mock(SdkHttpResponse.class);

    when(metadata.statusCode()).thenReturn(statusCode);
    when(response.sdkHttpResponse()).thenReturn(metadata);
    when(response.messageId()).thenReturn(UUID.randomUUID().toString());
    return CompletableFuture.completedFuture(response);
  }
}
