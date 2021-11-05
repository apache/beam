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

import java.util.UUID;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.GetTopicAttributesRequest;
import software.amazon.awssdk.services.sns.model.GetTopicAttributesResponse;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;

// import static org.mockito.BDDMockito.given;

/** Mock class to test a successful publish of a msg. */
public class SnsClientMockSuccess implements SnsClient {

  @Override
  public PublishResponse publish(PublishRequest publishRequest) {
    return (PublishResponse)
        PublishResponse.builder()
            .messageId(UUID.randomUUID().toString())
            .sdkHttpResponse(SdkHttpResponse.builder().statusCode(200).build())
            .build();
  }

  @Override
  public GetTopicAttributesResponse getTopicAttributes(
      GetTopicAttributesRequest topicAttributesRequest) {
    return (GetTopicAttributesResponse)
        GetTopicAttributesResponse.builder()
            .sdkHttpResponse(SdkHttpResponse.builder().statusCode(200).build())
            .build();
  }

  @Override
  public String serviceName() {
    return null;
  }

  @Override
  public void close() {}
}
