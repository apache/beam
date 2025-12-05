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
package org.apache.beam.sdk.io.aws2.auth;

import org.apache.beam.sdk.util.InstanceBuilder;

/**
 * Defines the behavior for a OIDC web identity token provider. Instances of this interface will be
 * used by an AWS credentials provider which will send the OIDC Token retrieved to dynamically
 * refresh federated authorized credentials.
 */
public interface WebIdTokenProvider {
  /**
   * Factory method for OIDC web identity token provider implementations.
   *
   * @param providerFQCN The fully qualified class name of an implementation of {@link
   *     WebIdTokenProvider}.
   * @return An instance of {@link WebIdTokenProvider}.
   */
  static WebIdTokenProvider create(String providerFQCN) {
    try {
      return InstanceBuilder.ofType(WebIdTokenProvider.class).fromClassName(providerFQCN).build();
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(
          "Problems while trying to instantiate a dynamic web id token provider class.", e);
    }
  }

  /**
   * Resolves the value for a OIDC web identity token.
   *
   * @param audience The audience for the token.
   * @return The encoded value for the OIDC web identity token.
   */
  String resolveTokenValue(String audience);
}
