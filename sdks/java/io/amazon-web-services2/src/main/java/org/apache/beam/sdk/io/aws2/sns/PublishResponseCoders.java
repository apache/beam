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

import static org.apache.beam.sdk.io.aws2.coders.AwsCoders.sdkHttpResponse;
import static org.apache.beam.sdk.io.aws2.coders.AwsCoders.sdkHttpResponseWithoutHeaders;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.aws2.coders.AwsCoders;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.awssdk.awscore.AwsResponseMetadata;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.sns.model.PublishResponse;

/**
 * Coders for SNS {@link PublishResponse}.
 *
 * @deprecated Schema based coder is inferred automatically.
 */
@Deprecated
public class PublishResponseCoders {
  private static final Coder<String> MESSAGE_ID_CODER = StringUtf8Coder.of();
  private static final NullableCoder<AwsResponseMetadata> METADATA_CODER =
      NullableCoder.of(AwsCoders.awsResponseMetadata());

  private PublishResponseCoders() {}

  /**
   * Returns a new SNS {@link PublishResponse} coder which by default serializes only the SNS
   * messageId.
   *
   * @return the {@link PublishResponse} coder
   */
  public static Coder<PublishResponse> defaultPublishResponse() {
    return new PublishResponseCoder(null, null);
  }

  /**
   * Returns a new SNS {@link PublishResponse} coder which serializes {@link AwsResponseMetadata}
   * and {@link SdkHttpResponse}, including the HTTP response headers.
   *
   * @return the {@link PublishResponse} coder
   */
  public static Coder<PublishResponse> fullPublishResponse() {
    return new PublishResponseCoder(METADATA_CODER, NullableCoder.of(sdkHttpResponse()));
  }

  /**
   * Returns a new SNS {@link PublishResponse} coder which serializes {@link AwsResponseMetadata}
   * and {@link SdkHttpResponse}, but not including the HTTP response headers.
   *
   * @return the {@link PublishResponse} coder
   */
  public static Coder<PublishResponse> fullPublishResponseWithoutHeaders() {
    return new PublishResponseCoder(
        METADATA_CODER, NullableCoder.of(sdkHttpResponseWithoutHeaders()));
  }

  private static class PublishResponseCoder extends CustomCoder<PublishResponse> {
    private final @Nullable NullableCoder<AwsResponseMetadata> metadataCoder;
    private final @Nullable NullableCoder<SdkHttpResponse> httpResponseCoder;

    private PublishResponseCoder(
        @Nullable NullableCoder<AwsResponseMetadata> responseMetadataEncoder,
        @Nullable NullableCoder<SdkHttpResponse> sdkHttpMetadataCoder) {
      this.metadataCoder = responseMetadataEncoder;
      this.httpResponseCoder = sdkHttpMetadataCoder;
    }

    @Override
    public void encode(PublishResponse value, OutputStream outStream)
        throws CoderException, IOException {
      MESSAGE_ID_CODER.encode(value.messageId(), outStream);
      if (metadataCoder != null) {
        metadataCoder.encode(value.responseMetadata(), outStream);
      }
      if (httpResponseCoder != null) {
        httpResponseCoder.encode(value.sdkHttpResponse(), outStream);
      }
    }

    @Override
    public PublishResponse decode(InputStream inStream) throws CoderException, IOException {
      PublishResponse.Builder responseBuilder =
          PublishResponse.builder().messageId(MESSAGE_ID_CODER.decode(inStream));
      if (metadataCoder != null) {
        AwsResponseMetadata metadata = metadataCoder.decode(inStream);
        if (metadata != null) {
          responseBuilder.responseMetadata(metadata);
        }
      }
      if (httpResponseCoder != null) {
        SdkHttpResponse httpResponse = httpResponseCoder.decode(inStream);
        if (httpResponse != null) {
          responseBuilder.sdkHttpResponse(httpResponse);
        }
      }
      return responseBuilder.build();
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      MESSAGE_ID_CODER.verifyDeterministic();
      if (metadataCoder != null) {
        metadataCoder.verifyDeterministic();
      }
      if (httpResponseCoder != null) {
        httpResponseCoder.verifyDeterministic();
      }
    }
  }
}
