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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import org.apache.beam.sdk.io.snowflake.test.TestUtils;
import org.apache.beam.sdk.io.snowflake.xlang.ExternalRead;
import org.apache.beam.sdk.io.snowflake.xlang.ExternalRead.ReadConfiguration;
import org.junit.Test;

public class ExternalReadCredentialsTest {
  private static final String SERVER_NAME = "server_name.snowflakecomputing.com";
  private static final String DATABASE = "test_database";
  private static final String SCHEMA = "public";
  private static final String STAGING_BUCKET_NAME = "bucket/";

  @Test
  public void testBuildExternalTransformWithoutCredentials() {
    ReadConfiguration configuration = createTestConfiguration();
    assertThrows(
        RuntimeException.class, () -> new ExternalRead.ReadBuilder().buildExternal(configuration));
  }

  @Test
  public void testBuildExternalTransformUsingOAuthToken() {
    ReadConfiguration configuration = createTestConfiguration();
    configuration.setOAuthToken("token");
    assertNotNull(new ExternalRead.ReadBuilder().buildExternal(configuration));
  }

  @Test
  public void testBuildExternalTransformUsingUsernameAndPassword() {
    ReadConfiguration configuration = createTestConfiguration();
    configuration.setUsername("username");
    configuration.setPassword("password");
    assertNotNull(new ExternalRead.ReadBuilder().buildExternal(configuration));
  }

  @Test
  public void testBuildExternalTransformUsingKeyPair() {
    ReadConfiguration configuration = createTestConfiguration();
    configuration.setUsername("username");
    configuration.setPrivateKeyPath(TestUtils.getPrivateKeyPath(getClass()));
    configuration.setPrivateKeyPassphrase(TestUtils.getPrivateKeyPassphrase());
    assertNotNull(new ExternalRead.ReadBuilder().buildExternal(configuration));
  }

  private ReadConfiguration createTestConfiguration() {
    ReadConfiguration configuration = new ReadConfiguration();
    configuration.setServerName(SERVER_NAME);
    configuration.setDatabase(DATABASE);
    configuration.setSchema(SCHEMA);
    configuration.setStagingBucketName(STAGING_BUCKET_NAME);
    return configuration;
  }
}
