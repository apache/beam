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
package org.apache.beam.sdk.io.aws.coders;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import com.amazonaws.ResponseMetadata;
import com.amazonaws.http.HttpResponse;
import com.amazonaws.http.SdkHttpMetadata;
import java.util.UUID;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;

/** Tests for AWS coders. */
public class AwsCodersTest {

  @Test
  public void testResponseMetadataDecodeEncodeEquals() throws Exception {
    ResponseMetadata value = buildResponseMetadata();
    ResponseMetadata clone = CoderUtils.clone(AwsCoders.responseMetadata(), value);
    assertThat(clone.getRequestId(), equalTo(value.getRequestId()));
  }

  @Test
  public void testSdkHttpMetadataDecodeEncodeEquals() throws Exception {
    SdkHttpMetadata value = buildSdkHttpMetadata();
    SdkHttpMetadata clone = CoderUtils.clone(AwsCoders.sdkHttpMetadata(), value);
    assertThat(clone.getHttpStatusCode(), equalTo(value.getHttpStatusCode()));
    assertThat(clone.getHttpHeaders(), equalTo(value.getHttpHeaders()));
  }

  @Test
  public void testSdkHttpMetadataWithoutHeadersDecodeEncodeEquals() throws Exception {
    SdkHttpMetadata value = buildSdkHttpMetadata();
    SdkHttpMetadata clone = CoderUtils.clone(AwsCoders.sdkHttpMetadataWithoutHeaders(), value);
    assertThat(clone.getHttpStatusCode(), equalTo(value.getHttpStatusCode()));
    assertThat(clone.getHttpHeaders().isEmpty(), equalTo(true));
  }

  private ResponseMetadata buildResponseMetadata() {
    return new ResponseMetadata(
        ImmutableMap.of(ResponseMetadata.AWS_REQUEST_ID, UUID.randomUUID().toString()));
  }

  private SdkHttpMetadata buildSdkHttpMetadata() {
    HttpResponse httpResponse = new HttpResponse(null, null);
    httpResponse.setStatusCode(200);
    httpResponse.addHeader("Content-Type", "application/json");
    return SdkHttpMetadata.from(httpResponse);
  }
}
