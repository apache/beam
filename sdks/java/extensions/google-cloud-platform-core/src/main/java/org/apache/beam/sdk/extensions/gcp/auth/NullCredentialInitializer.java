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
package org.apache.beam.sdk.extensions.gcp.auth;

import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpUnsuccessfulResponseHandler;
import java.io.IOException;

/**
 * A {@link HttpRequestInitializer} for requests that don't have credentials.
 *
 * <p>When the access is denied, it throws {@link IOException} with a detailed error message.
 */
public class NullCredentialInitializer implements HttpRequestInitializer {
  private static final int ACCESS_DENIED = 401;
  private static final String NULL_CREDENTIAL_REASON =
      "Unable to get application default credentials. Please see "
          + "https://developers.google.com/accounts/docs/application-default-credentials "
          + "for details on how to specify credentials. This version of the SDK is "
          + "dependent on the gcloud core component version 2015.02.05 or newer to "
          + "be able to get credentials from the currently authorized user via gcloud auth.";

  @Override
  public void initialize(HttpRequest httpRequest) throws IOException {
    httpRequest.setUnsuccessfulResponseHandler(new NullCredentialHttpUnsuccessfulResponseHandler());
  }

  private static class NullCredentialHttpUnsuccessfulResponseHandler
      implements HttpUnsuccessfulResponseHandler {

    @Override
    public boolean handleResponse(
        HttpRequest httpRequest, HttpResponse httpResponse, boolean supportsRetry)
        throws IOException {
      if (!httpResponse.isSuccessStatusCode() && httpResponse.getStatusCode() == ACCESS_DENIED) {
        throwNullCredentialException();
      }
      return supportsRetry;
    }
  }

  public static void throwNullCredentialException() {
    throw new RuntimeException(NULL_CREDENTIAL_REASON);
  }
}
