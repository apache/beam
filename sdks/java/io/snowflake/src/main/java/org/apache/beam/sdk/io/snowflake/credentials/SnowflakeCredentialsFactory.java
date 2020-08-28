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
import org.apache.beam.sdk.io.snowflake.crosslanguage.CrossLanguageConfiguration;

/**
 * Factory class for creating implementations of {@link SnowflakeCredentials} from {@link
 * SnowflakePipelineOptions}.
 */
public class SnowflakeCredentialsFactory {
  public static SnowflakeCredentials of(SnowflakePipelineOptions o) {
    if (oauthOptionsAvailable(o.getOauthToken())) {
      return new OAuthTokenSnowflakeCredentials(o.getOauthToken());
    } else if (usernamePasswordOptionsAvailable(o.getUsername(), o.getPassword())) {
      return new UsernamePasswordSnowflakeCredentials(o.getUsername(), o.getPassword());
    } else if (keyPairOptionsAvailable(
        o.getUsername(), o.getPrivateKeyPath(), o.getPrivateKeyPassphrase())) {
      return new KeyPairSnowflakeCredentials(
          o.getUsername(), o.getPrivateKeyPath(), o.getPrivateKeyPassphrase());
    }
    throw new RuntimeException("Can't get credentials from Options");
  }

  public static SnowflakeCredentials of(CrossLanguageConfiguration c) {
    if (oauthOptionsAvailable(c.getOAuthToken())) {
      return new OAuthTokenSnowflakeCredentials(c.getOAuthToken());
    } else if (usernamePasswordOptionsAvailable(c.getUsername(), c.getPassword())) {
      return new UsernamePasswordSnowflakeCredentials(c.getUsername(), c.getPassword());
    } else if (keyPairOptionsAvailable(
        c.getUsername(), c.getPrivateKeyPath(), c.getPrivateKeyPassphrase())) {
      return new KeyPairSnowflakeCredentials(
          c.getUsername(), c.getPrivateKeyPath(), c.getPrivateKeyPassphrase());
    }
    throw new RuntimeException("Can't get credentials from Options");
  }

  private static boolean oauthOptionsAvailable(String token) {
    return token != null && !token.isEmpty();
  }

  private static boolean usernamePasswordOptionsAvailable(String username, String password) {
    return username != null && !username.isEmpty() && !password.isEmpty();
  }

  private static boolean keyPairOptionsAvailable(
      String username, String privateKeyPath, String privateKeyPassphrase) {
    return username != null
        && !username.isEmpty()
        && !privateKeyPath.isEmpty()
        && !privateKeyPassphrase.isEmpty();
  }
}
