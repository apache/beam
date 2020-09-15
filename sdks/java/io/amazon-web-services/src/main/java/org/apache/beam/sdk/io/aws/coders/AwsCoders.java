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

import com.amazonaws.ResponseMetadata;
import com.amazonaws.http.HttpResponse;
import com.amazonaws.http.SdkHttpMetadata;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;

/** {@link Coder}s for common AWS SDK objects. */
public final class AwsCoders {

  private AwsCoders() {}

  /**
   * Returns a new coder for ResponseMetadata.
   *
   * @return the ResponseMetadata coder
   */
  public static Coder<ResponseMetadata> responseMetadata() {
    return ResponseMetadataCoder.of();
  }

  /**
   * Returns a new coder for SdkHttpMetadata.
   *
   * @return the SdkHttpMetadata coder
   */
  public static Coder<SdkHttpMetadata> sdkHttpMetadata() {
    return new SdkHttpMetadataCoder(true);
  }

  /**
   * Returns a new coder for SdkHttpMetadata that does not serialize the response headers.
   *
   * @return the SdkHttpMetadata coder
   */
  public static Coder<SdkHttpMetadata> sdkHttpMetadataWithoutHeaders() {
    return new SdkHttpMetadataCoder(false);
  }

  private static class ResponseMetadataCoder extends AtomicCoder<ResponseMetadata> {

    private static final Coder<Map<String, String>> METADATA_ENCODER =
        NullableCoder.of(MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));
    private static final ResponseMetadataCoder INSTANCE = new ResponseMetadataCoder();

    private ResponseMetadataCoder() {}

    public static ResponseMetadataCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(ResponseMetadata value, OutputStream outStream)
        throws CoderException, IOException {
      METADATA_ENCODER.encode(
          ImmutableMap.of(ResponseMetadata.AWS_REQUEST_ID, value.getRequestId()), outStream);
    }

    @Override
    public ResponseMetadata decode(InputStream inStream) throws CoderException, IOException {
      return new ResponseMetadata(METADATA_ENCODER.decode(inStream));
    }
  }

  private static class SdkHttpMetadataCoder extends CustomCoder<SdkHttpMetadata> {

    private static final Coder<Integer> STATUS_CODE_CODER = VarIntCoder.of();
    private static final Coder<Map<String, String>> HEADERS_ENCODER =
        NullableCoder.of(MapCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of()));

    private final boolean includeHeaders;

    protected SdkHttpMetadataCoder(boolean includeHeaders) {
      this.includeHeaders = includeHeaders;
    }

    @Override
    public void encode(SdkHttpMetadata value, OutputStream outStream)
        throws CoderException, IOException {
      STATUS_CODE_CODER.encode(value.getHttpStatusCode(), outStream);
      if (includeHeaders) {
        HEADERS_ENCODER.encode(value.getHttpHeaders(), outStream);
      }
    }

    @Override
    public SdkHttpMetadata decode(InputStream inStream) throws CoderException, IOException {
      final int httpStatusCode = STATUS_CODE_CODER.decode(inStream);
      HttpResponse httpResponse = new HttpResponse(null, null);
      httpResponse.setStatusCode(httpStatusCode);
      if (includeHeaders) {
        Optional.ofNullable(HEADERS_ENCODER.decode(inStream))
            .ifPresent(
                headers ->
                    headers.keySet().forEach(k -> httpResponse.addHeader(k, headers.get(k))));
      }
      return SdkHttpMetadata.from(httpResponse);
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
