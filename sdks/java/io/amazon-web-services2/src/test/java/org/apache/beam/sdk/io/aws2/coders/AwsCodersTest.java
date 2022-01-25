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
package org.apache.beam.sdk.io.aws2.coders;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static software.amazon.awssdk.awscore.util.AwsHeader.AWS_REQUEST_ID;

import java.util.Map;
import java.util.UUID;
import org.apache.beam.sdk.util.CoderUtils;
import org.junit.Test;
import software.amazon.awssdk.awscore.AwsResponseMetadata;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.utils.ImmutableMap;

/** Tests for {@link AwsCoders}. */
public class AwsCodersTest {

  @Test
  public void testAwsResponseMetadataDecodeEncodeEquals() throws Exception {
    AwsResponseMetadata value = buildAwsResponseMetadata();
    AwsResponseMetadata clone = CoderUtils.clone(AwsCoders.awsResponseMetadata(), value);
    assertThat(clone.requestId(), equalTo(value.requestId()));
  }

  @Test
  public void testSdkHttpMetadataDecodeEncodeEquals() throws Exception {
    SdkHttpResponse value = buildSdkHttpMetadata();
    SdkHttpResponse clone = CoderUtils.clone(AwsCoders.sdkHttpResponse(), value);
    assertThat(clone.statusCode(), equalTo(value.statusCode()));
    assertThat(clone.headers(), equalTo(value.headers()));
  }

  @Test
  public void testSdkHttpMetadataWithoutHeadersDecodeEncodeEquals() throws Exception {
    SdkHttpResponse value = buildSdkHttpMetadata();
    SdkHttpResponse clone = CoderUtils.clone(AwsCoders.sdkHttpResponseWithoutHeaders(), value);
    assertThat(clone.statusCode(), equalTo(value.statusCode()));
    assertThat(clone.headers().isEmpty(), equalTo(true));
  }

  private AwsResponseMetadata buildAwsResponseMetadata() {
    Map<String, String> metadata = ImmutableMap.of(AWS_REQUEST_ID, UUID.randomUUID().toString());
    return new AwsResponseMetadata(metadata) {};
  }

  private SdkHttpResponse buildSdkHttpMetadata() {
    return SdkHttpResponse.builder()
        .statusCode(200)
        .appendHeader("Content-Type", "application/json")
        .build();
  }
}
