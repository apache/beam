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
package org.apache.beam.sdk.io.snowflake.test.unit.credentials;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.apache.beam.sdk.io.snowflake.SnowflakePipelineOptions;
import org.apache.beam.sdk.io.snowflake.credentials.KeyPairSnowflakeCredentials;
import org.apache.beam.sdk.io.snowflake.credentials.OAuthTokenSnowflakeCredentials;
import org.apache.beam.sdk.io.snowflake.credentials.SnowflakeCredentials;
import org.apache.beam.sdk.io.snowflake.credentials.SnowflakeCredentialsFactory;
import org.apache.beam.sdk.io.snowflake.credentials.UsernamePasswordSnowflakeCredentials;
import org.apache.beam.sdk.io.snowflake.test.TestUtils;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;

public class SnowflakeCredentialsFactoryTest {

  @Test
  public void usernamePasswordTest() {
    SnowflakePipelineOptions options = PipelineOptionsFactory.as(SnowflakePipelineOptions.class);
    options.setUsername("username");
    options.setPassword("password");

    SnowflakeCredentials credentials = SnowflakeCredentialsFactory.of(options);

    assertEquals(UsernamePasswordSnowflakeCredentials.class, credentials.getClass());
  }

  @Test
  public void oauthTokenTest() {
    SnowflakePipelineOptions options = PipelineOptionsFactory.as(SnowflakePipelineOptions.class);
    options.setOauthToken("token");

    SnowflakeCredentials credentials = SnowflakeCredentialsFactory.of(options);

    assertEquals(OAuthTokenSnowflakeCredentials.class, credentials.getClass());
  }

  @Test
  public void keyPairTest() {
    SnowflakePipelineOptions options = PipelineOptionsFactory.as(SnowflakePipelineOptions.class);
    options.setUsername("username");
    options.setPrivateKeyPath(TestUtils.getPrivateKeyPath(getClass()));
    options.setPrivateKeyPassphrase(TestUtils.getPrivateKeyPassphrase());

    SnowflakeCredentials credentials = SnowflakeCredentialsFactory.of(options);

    assertEquals(KeyPairSnowflakeCredentials.class, credentials.getClass());
  }

  @Test
  public void emptyOptionsTest() {
    SnowflakePipelineOptions options = PipelineOptionsFactory.as(SnowflakePipelineOptions.class);

    Exception ex =
        assertThrows(RuntimeException.class, () -> SnowflakeCredentialsFactory.of(options));
    assertEquals("Can't get credentials", ex.getMessage());
  }
}
