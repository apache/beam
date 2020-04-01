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

import com.amazonaws.ResponseMetadata;
import com.amazonaws.http.SdkHttpMetadata;
import com.amazonaws.services.sns.model.PublishResult;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.aws.coders.AwsCoders;

/** Coders for SNS {@link PublishResult}. */
public final class PublishResultCoders {

  private static final Coder<String> MESSAGE_ID_CODER = StringUtf8Coder.of();
  private static final Coder<ResponseMetadata> RESPONSE_METADATA_CODER =
      NullableCoder.of(AwsCoders.responseMetadata());

  private PublishResultCoders() {}

  /**
   * Returns a new PublishResult coder which by default serializes only the messageId.
   *
   * @return the PublishResult coder
   */
  public static Coder<PublishResult> defaultPublishResult() {
    return new PublishResultCoder(null, null);
  }

  /**
   * Returns a new PublishResult coder which serializes the sdkResponseMetadata and sdkHttpMetadata,
   * including the HTTP response headers.
   *
   * @return the PublishResult coder
   */
  public static Coder<PublishResult> fullPublishResult() {
    return new PublishResultCoder(
        RESPONSE_METADATA_CODER, NullableCoder.of(AwsCoders.sdkHttpMetadata()));
  }

  /**
   * Returns a new PublishResult coder which serializes the sdkResponseMetadata and sdkHttpMetadata,
   * but does not include the HTTP response headers.
   *
   * @return the PublishResult coder
   */
  public static Coder<PublishResult> fullPublishResultWithoutHeaders() {
    return new PublishResultCoder(
        RESPONSE_METADATA_CODER, NullableCoder.of(AwsCoders.sdkHttpMetadataWithoutHeaders()));
  }

  static class PublishResultCoder extends CustomCoder<PublishResult> {

    private final Coder<ResponseMetadata> responseMetadataEncoder;
    private final Coder<SdkHttpMetadata> sdkHttpMetadataCoder;

    private PublishResultCoder(
        Coder<ResponseMetadata> responseMetadataEncoder,
        Coder<SdkHttpMetadata> sdkHttpMetadataCoder) {
      this.responseMetadataEncoder = responseMetadataEncoder;
      this.sdkHttpMetadataCoder = sdkHttpMetadataCoder;
    }

    @Override
    public void encode(PublishResult value, OutputStream outStream)
        throws CoderException, IOException {
      MESSAGE_ID_CODER.encode(value.getMessageId(), outStream);
      if (responseMetadataEncoder != null) {
        responseMetadataEncoder.encode(value.getSdkResponseMetadata(), outStream);
      }
      if (sdkHttpMetadataCoder != null) {
        sdkHttpMetadataCoder.encode(value.getSdkHttpMetadata(), outStream);
      }
    }

    @Override
    public PublishResult decode(InputStream inStream) throws CoderException, IOException {
      String messageId = MESSAGE_ID_CODER.decode(inStream);
      PublishResult publishResult = new PublishResult().withMessageId(messageId);
      if (responseMetadataEncoder != null) {
        publishResult.setSdkResponseMetadata(responseMetadataEncoder.decode(inStream));
      }
      if (sdkHttpMetadataCoder != null) {
        publishResult.setSdkHttpMetadata(sdkHttpMetadataCoder.decode(inStream));
      }
      return publishResult;
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      MESSAGE_ID_CODER.verifyDeterministic();
      if (responseMetadataEncoder != null) {
        responseMetadataEncoder.verifyDeterministic();
      }
      if (sdkHttpMetadataCoder != null) {
        sdkHttpMetadataCoder.verifyDeterministic();
      }
    }
  }
}
