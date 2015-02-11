/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.util;

import com.google.api.client.auth.oauth2.BearerToken;
import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.util.IOUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

/**
 * A credential object which uses the GCloud command line tool to get
 * an access token.
 */
public class GCloudCredential extends Credential {
  private static final String DEFAULT_GCLOUD_BINARY = "gcloud";
  private final String binary;

  public GCloudCredential(HttpTransport transport) {
    this(DEFAULT_GCLOUD_BINARY, transport);
  }

  /**
   * Path to the GCloud binary.
   */
  public GCloudCredential(String binary, HttpTransport transport) {
    super(new Builder(BearerToken.authorizationHeaderAccessMethod())
        .setTransport(transport));

    this.binary = binary;
  }

  private String readStream(InputStream stream) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    IOUtils.copy(stream, baos);
    return baos.toString("UTF-8");
  }

  @Override
  protected TokenResponse executeRefreshToken() throws IOException {
    TokenResponse response = new TokenResponse();

    ProcessBuilder builder = new ProcessBuilder();
    // ProcessBuilder will search the path automatically for the binary
    // GCLOUD_BINARY.
    builder.command(Arrays.asList(binary, "auth", "print-access-token"));
    Process process = builder.start();

    try {
      process.waitFor();
    } catch (InterruptedException e) {
      throw new RuntimeException(
          "Could not obtain an access token using gcloud; timed out waiting " +
          "for gcloud.");
    }

    if (process.exitValue() != 0) {
      String output;
      try {
        output = readStream(process.getErrorStream());
      } catch (IOException e) {
        throw new RuntimeException(
            "Could not obtain an access token using gcloud.");
      }

      throw new RuntimeException(
          "Could not obtain an access token using gcloud. Result of " +
          "invoking gcloud was:\n" + output);
    }

    String output;
    try {
      output = readStream(process.getInputStream());
    } catch (IOException e) {
      throw new RuntimeException(
          "Could not obtain an access token using gcloud. We encountered an " +
          "an error trying to read stdout.", e);
    }
    String[] lines = output.split("\n");

    if (lines.length != 1) {
      throw new RuntimeException(
          "Could not obtain an access token using gcloud. Result of " +
          "invoking gcloud was:\n" + output);
    }

    // Access token should be good for 5 minutes.
    Long expiresInSeconds = 5L * 60;
    response.setExpiresInSeconds(expiresInSeconds);
    response.setAccessToken(output.trim());

    return response;
  }
}
