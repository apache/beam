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
package org.apache.beam.sdk.io.aws.sns;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import com.amazonaws.ResponseMetadata;
import com.amazonaws.http.HttpResponse;
import com.amazonaws.http.SdkHttpMetadata;
import com.amazonaws.services.sns.model.PublishResult;
import java.util.UUID;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;

/** Tests for PublishResult coders. */
public class PublishResultCodersTest {

  @Test
  public void testDefaultPublishResultDecodeEncodeEquals() throws Exception {
    CoderProperties.coderDecodeEncodeEqual(
        PublishResultCoders.defaultPublishResult(),
        new PublishResult().withMessageId(UUID.randomUUID().toString()));
  }

  @Test
  public void testFullPublishResultWithoutHeadersDecodeEncodeEquals() throws Exception {
    CoderProperties.coderDecodeEncodeEqual(
        PublishResultCoders.fullPublishResultWithoutHeaders(),
        new PublishResult().withMessageId(UUID.randomUUID().toString()));

    PublishResult value = buildFullPublishResult();
    PublishResult clone =
        CoderUtils.clone(PublishResultCoders.fullPublishResultWithoutHeaders(), value);
    assertThat(
        clone.getSdkResponseMetadata().getRequestId(),
        equalTo(value.getSdkResponseMetadata().getRequestId()));
    assertThat(
        clone.getSdkHttpMetadata().getHttpStatusCode(),
        equalTo(value.getSdkHttpMetadata().getHttpStatusCode()));
    assertThat(clone.getSdkHttpMetadata().getHttpHeaders().isEmpty(), equalTo(true));
  }

  @Test
  public void testFullPublishResultIncludingHeadersDecodeEncodeEquals() throws Exception {
    CoderProperties.coderDecodeEncodeEqual(
        PublishResultCoders.fullPublishResult(),
        new PublishResult().withMessageId(UUID.randomUUID().toString()));

    PublishResult value = buildFullPublishResult();
    PublishResult clone = CoderUtils.clone(PublishResultCoders.fullPublishResult(), value);
    assertThat(
        clone.getSdkResponseMetadata().getRequestId(),
        equalTo(value.getSdkResponseMetadata().getRequestId()));
    assertThat(
        clone.getSdkHttpMetadata().getHttpStatusCode(),
        equalTo(value.getSdkHttpMetadata().getHttpStatusCode()));
    assertThat(
        clone.getSdkHttpMetadata().getHttpHeaders(),
        equalTo(value.getSdkHttpMetadata().getHttpHeaders()));
  }

  private PublishResult buildFullPublishResult() {
    PublishResult publishResult = new PublishResult().withMessageId(UUID.randomUUID().toString());
    publishResult.setSdkResponseMetadata(
        new ResponseMetadata(
            ImmutableMap.of(ResponseMetadata.AWS_REQUEST_ID, UUID.randomUUID().toString())));
    HttpResponse httpResponse = new HttpResponse(null, null);
    httpResponse.setStatusCode(200);
    httpResponse.addHeader("Content-Type", "application/json");
    publishResult.setSdkHttpMetadata(SdkHttpMetadata.from(httpResponse));
    return publishResult;
  }
}
