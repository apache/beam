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
package org.apache.beam.examples.webapis;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.io.InputStream;
import org.apache.beam.io.requestresponse.Caller;
import org.apache.beam.io.requestresponse.UserCodeExecutionException;
import org.apache.beam.io.requestresponse.UserCodeQuotaException;
import org.apache.beam.io.requestresponse.UserCodeRemoteSystemException;
import org.apache.beam.io.requestresponse.UserCodeTimeoutException;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.ByteStreams;

// [START webapis_java_image_caller]

/**
 * Implements {@link Caller} to process an {@link ImageRequest} into an {@link ImageResponse} by
 * invoking the HTTP request.
 */
class HttpImageClient implements Caller<KV<String, ImageRequest>, KV<String, ImageResponse>> {

  private static final int STATUS_TOO_MANY_REQUESTS = 429;
  private static final int STATUS_TIMEOUT = 408;
  private static final HttpRequestFactory REQUEST_FACTORY =
      new NetHttpTransport().createRequestFactory();

  static HttpImageClient of() {
    return new HttpImageClient();
  }

  /**
   * Invokes an HTTP Get request from the {@param request}, returning an {@link ImageResponse}
   * containing the image data.
   */
  @Override
  public KV<String, ImageResponse> call(KV<String, ImageRequest> requestKV)
      throws UserCodeExecutionException {

    String key = requestKV.getKey();
    ImageRequest request = requestKV.getValue();
    Preconditions.checkArgument(request != null);
    GenericUrl url = new GenericUrl(request.getImageUrl());

    try {
      HttpRequest imageRequest = REQUEST_FACTORY.buildGetRequest(url);
      HttpResponse response = imageRequest.execute();

      if (response.getStatusCode() >= 500) {
        // Tells transform to repeat the request.
        throw new UserCodeRemoteSystemException(response.getStatusMessage());
      }

      if (response.getStatusCode() >= 400) {

        switch (response.getStatusCode()) {
          case STATUS_TOO_MANY_REQUESTS:
            // Tells transform to repeat the request.
            throw new UserCodeQuotaException(response.getStatusMessage());

          case STATUS_TIMEOUT:
            // Tells transform to repeat the request.
            throw new UserCodeTimeoutException(response.getStatusMessage());

          default:
            // Tells the tranform to emit immediately into failure PCollection.
            throw new UserCodeExecutionException(response.getStatusMessage());
        }
      }

      InputStream is = response.getContent();
      byte[] bytes = ByteStreams.toByteArray(is);

      return KV.of(
          key,
          ImageResponse.builder()
              .setMimeType(request.getMimeType())
              .setData(ByteString.copyFrom(bytes))
              .build());

    } catch (IOException e) {

      // Tells the tranform to emit immediately into failure PCollection.
      throw new UserCodeExecutionException(e);
    }
  }
}

// [END webapis_java_image_caller]
