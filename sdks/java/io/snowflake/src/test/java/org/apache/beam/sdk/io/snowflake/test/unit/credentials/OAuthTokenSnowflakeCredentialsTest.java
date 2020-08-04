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
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.io.snowflake.SnowflakeIO;
import org.apache.beam.sdk.io.snowflake.credentials.OAuthTokenSnowflakeCredentials;
import org.junit.Test;

public class OAuthTokenSnowflakeCredentialsTest {

  @Test
  public void testConstructor() {
    OAuthTokenSnowflakeCredentials credentials = new OAuthTokenSnowflakeCredentials("token");

    assertEquals("token", credentials.getToken());
  }

  @Test
  public void testBuildingDataSource() {
    OAuthTokenSnowflakeCredentials credentials = new OAuthTokenSnowflakeCredentials("token");

    SnowflakeIO.DataSourceConfiguration configuration =
        SnowflakeIO.DataSourceConfiguration.create(credentials);

    assertEquals(credentials.getToken(), configuration.getOauthToken());
    assertTrue(configuration.getValidate());
  }
}
