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
package org.apache.beam.sdk.io.snowflake.credentials;

import org.apache.beam.sdk.io.snowflake.SnowflakePipelineOptions;
import org.apache.beam.sdk.io.snowflake.xlang.Configuration;

/**
 * Factory class for creating implementations of {@link SnowflakeCredentials} from {@link
 * SnowflakePipelineOptions}.
 */
public class SnowflakeCredentialsFactory {
  public static SnowflakeCredentials of(SnowflakePipelineOptions o) {
    return createCredentials(
        o.getOauthToken(),
        o.getPrivateKeyPath(),
        o.getPrivateKeyPassphrase(),
        o.getUsername(),
        o.getPassword());
  }

  public static SnowflakeCredentials createCredentials(Configuration c) {
    return createCredentials(
        c.getOAuthToken(),
        c.getPrivateKeyPath(),
        c.getPrivateKeyPassphrase(),
        c.getUsername(),
        c.getPassword());
  }

  private static SnowflakeCredentials createCredentials(
      String oAuth,
      String privateKeyPath,
      String privateKeyPassphrase,
      String username,
      String password) {

    if (isNotEmpty(oAuth)) {
      return new OAuthTokenSnowflakeCredentials(oAuth);
    } else if (isNotEmpty(privateKeyPath)
        && isNotEmpty(username)
        && isNotEmpty(privateKeyPassphrase)) {
      return new KeyPairSnowflakeCredentials(username, privateKeyPath, privateKeyPassphrase);
    } else if (isNotEmpty(username) && isNotEmpty(password)) {
      return new UsernamePasswordSnowflakeCredentials(username, password);
    } else {
      throw new RuntimeException("Can't get credentials");
    }
  }

  private static boolean isNotEmpty(String s) {
    return s != null && !s.isEmpty();
  }
}
