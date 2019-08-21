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

import org.mockito.Mockito;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.GetTopicAttributesRequest;
import software.amazon.awssdk.services.sns.model.GetTopicAttributesResponse;
import software.amazon.awssdk.services.sns.model.InternalErrorException;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

/** Mock class to test a failed publish of a msg. */
public class SnsClientMockErrors implements SnsClient {

  @Override
  public PublishResponse publish(PublishRequest publishRequest) {
    throw InternalErrorException.builder().message("Service unavailable").build();
  }

  @Override
  public GetTopicAttributesResponse getTopicAttributes(
      GetTopicAttributesRequest topicAttributesRequest) {
    GetTopicAttributesResponse response = Mockito.mock(GetTopicAttributesResponse.class);
    SdkHttpResponse metadata = Mockito.mock(SdkHttpResponse.class);

    Mockito.when(metadata.statusCode()).thenReturn(200);
    Mockito.when(response.sdkHttpResponse()).thenReturn(metadata);

    return response;
  }

  @Override
  public String serviceName() {
    return null;
  }

  @Override
  public void close() {}
}
