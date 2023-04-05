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

import static software.amazon.awssdk.awscore.util.AwsHeader.AWS_REQUEST_ID;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import software.amazon.awssdk.awscore.AwsResponseMetadata;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.utils.ImmutableMap;

/**
 * {@link Coder}s for common AWS SDK objects.
 *
 * @deprecated {@link org.apache.beam.sdk.schemas.SchemaCoder SchemaCoders} for {@link
 *     software.amazon.awssdk.core.SdkPojo AWS model classes} will be automatically inferred by
 *     means of {@link org.apache.beam.sdk.io.aws2.schemas.AwsSchemaProvider AwsSchemaProvider}.
 */
@Deprecated
public final class AwsCoders {

  private AwsCoders() {}

  /**
   * Returns a new coder for {@link AwsResponseMetadata} (AWS request ID only).
   *
   * @return the {@link AwsResponseMetadata} coder
   */
  public static Coder<AwsResponseMetadata> awsResponseMetadata() {
    return new AwsResponseMetadataCoder();
  }

  /**
   * Returns a new coder for {@link SdkHttpResponse} (HTTP status code and headers).
   *
   * @return the SdkHttpResponse coder
   */
  public static Coder<SdkHttpResponse> sdkHttpResponse() {
    return new SdkHttpResponseCoder(true);
  }

  /**
   * Returns a new coder for {@link SdkHttpResponse} (HTTP status code only).
   *
   * @return the SdkHttpResponse coder
   */
  public static Coder<SdkHttpResponse> sdkHttpResponseWithoutHeaders() {
    return new SdkHttpResponseCoder(false);
  }

  private static class AwsResponseMetadataCoder extends AtomicCoder<AwsResponseMetadata> {
    private static final Coder<String> REQUEST_ID_CODER = StringUtf8Coder.of();

    private static class DecodedAwsResponseMetadata extends AwsResponseMetadata {
      protected DecodedAwsResponseMetadata(String requestId) {
        super(ImmutableMap.of(AWS_REQUEST_ID, requestId));
      }
    }

    @Override
    public void encode(AwsResponseMetadata value, OutputStream outStream)
        throws CoderException, IOException {
      REQUEST_ID_CODER.encode(value.requestId(), outStream);
    }

    @Override
    public AwsResponseMetadata decode(InputStream inStream) throws CoderException, IOException {
      return new DecodedAwsResponseMetadata(REQUEST_ID_CODER.decode(inStream));
    }
  }

  private static class SdkHttpResponseCoder extends CustomCoder<SdkHttpResponse> {
    private static final Coder<Integer> STATUS_CODE_CODER = VarIntCoder.of();
    private static final Coder<Map<String, List<String>>> HEADERS_ENCODER =
        NullableCoder.of(MapCoder.of(StringUtf8Coder.of(), ListCoder.of(StringUtf8Coder.of())));

    private final boolean includeHeaders;

    protected SdkHttpResponseCoder(boolean includeHeaders) {
      this.includeHeaders = includeHeaders;
    }

    @Override
    public void encode(SdkHttpResponse value, OutputStream outStream)
        throws CoderException, IOException {
      STATUS_CODE_CODER.encode(value.statusCode(), outStream);
      if (includeHeaders) {
        HEADERS_ENCODER.encode(value.headers(), outStream);
      }
    }

    @Override
    public SdkHttpResponse decode(InputStream inStream) throws CoderException, IOException {
      SdkHttpResponse.Builder httpResponseBuilder =
          SdkHttpResponse.builder().statusCode(STATUS_CODE_CODER.decode(inStream));

      if (includeHeaders) {
        Map<String, List<String>> headers = HEADERS_ENCODER.decode(inStream);
        if (headers != null) {
          httpResponseBuilder.headers(headers);
        }
      }
      return httpResponseBuilder.build();
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      STATUS_CODE_CODER.verifyDeterministic();
      if (includeHeaders) {
        HEADERS_ENCODER.verifyDeterministic();
      }
    }
  }
}
