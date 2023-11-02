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
package org.apache.beam.io.requestresponse;

import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpMediaType;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.net.URI;
import org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest;
import org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse;
import org.apache.beam.testinfra.mockapis.echo.v1.EchoServiceGrpc;

/**
 * Implements {@link Caller} to call the {@link EchoServiceGrpc}'s HTTP handler. The purpose of
 * {@link EchoHTTPCaller} is to suppport integration tests.
 */
class EchoHTTPCaller implements Caller<EchoRequest, EchoResponse> {

  static EchoHTTPCaller of(URI uri) {
    return new EchoHTTPCaller(uri);
  }

  private static final String PATH = "/v1/echo";
  private static final HttpRequestFactory REQUEST_FACTORY =
      new NetHttpTransport().createRequestFactory();
  private static final HttpMediaType CONTENT_TYPE = new HttpMediaType("application/json");
  private static final int STATUS_CODE_TOO_MANY_REQUESTS = 429;

  private final URI uri;

  private EchoHTTPCaller(URI uri) {
    this.uri = uri;
  }

  /**
   * Overrides {@link Caller#call} invoking the {@link EchoServiceGrpc}'s HTTP handler with a {@link
   * EchoRequest}, returning either a successful {@link EchoResponse} or throwing either a {@link
   * UserCodeExecutionException}, a {@link UserCodeTimeoutException}, or a {@link
   * UserCodeQuotaException}.
   */
  @Override
  public EchoResponse call(EchoRequest request) throws UserCodeExecutionException {
    try {
      String json = JsonFormat.printer().omittingInsignificantWhitespace().print(request);
      ByteArrayContent body = ByteArrayContent.fromString(CONTENT_TYPE.getType(), json);
      HttpRequest httpRequest = REQUEST_FACTORY.buildPostRequest(getUrl(), body);
      HttpResponse httpResponse = httpRequest.execute();
      String responseJson = httpResponse.parseAsString();
      EchoResponse.Builder builder = EchoResponse.newBuilder();
      JsonFormat.parser().merge(responseJson, builder);
      return builder.build();
    } catch (IOException e) {
      if (e instanceof HttpResponseException) {
        HttpResponseException ex = (HttpResponseException) e;
        if (ex.getStatusCode() == STATUS_CODE_TOO_MANY_REQUESTS) {
          throw new UserCodeQuotaException(e);
        }
      }
      throw new UserCodeExecutionException(e);
    }
  }

  private GenericUrl getUrl() {
    String rawUrl = uri.toString();
    if (uri.getPath().isEmpty()) {
      rawUrl += PATH;
    }
    return new GenericUrl(rawUrl);
  }
}
