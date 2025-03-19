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
package org.apache.beam.sdk.io.aws2.common.providers;

import java.io.IOException;
import java.util.Arrays;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.auth.oauth2.GoogleCredentials;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.auth.oauth2.IdTokenCredentials;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.auth.oauth2.IdTokenProvider;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.auth.oauth2.IdTokenProvider.Option;

public class GCPWebIdTokenProvider implements WebIdTokenProvider {

  @Override
  public String resolveTokenValue(String audience) {
    try {
      IdTokenCredentials idTokenCredentials =
          IdTokenCredentials.newBuilder()
              .setIdTokenProvider((IdTokenProvider) GoogleCredentials.getApplicationDefault())
              .setTargetAudience(audience)
              .setOptions(Arrays.asList(Option.FORMAT_FULL, Option.LICENSES_TRUE))
              .build();
      return idTokenCredentials.refreshAccessToken().getTokenValue();
    } catch (IOException ex) {
      throw new RuntimeException(
          "Problems while retrieving service account default credentials.", ex);
    }
  }
}
