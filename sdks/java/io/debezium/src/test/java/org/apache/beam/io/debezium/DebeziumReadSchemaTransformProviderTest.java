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
package org.apache.beam.io.debezium;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;

import java.util.Arrays;
import java.util.Collections;
import org.hamcrest.Matchers;
import org.junit.Test;

public class DebeziumReadSchemaTransformProviderTest {

  private static DebeziumReadSchemaTransformProvider.DebeziumReadSchemaTransformConfiguration
          .Builder
      validConfiguration() {
    return DebeziumReadSchemaTransformProvider.DebeziumReadSchemaTransformConfiguration.builder()
        .setUsername("user")
        .setPassword("password")
        .setHost("localhost")
        .setPort(5432)
        .setDatabase("POSTGRES")
        .setTable("inventory.customers")
        .setDebeziumConnectionProperties(Collections.emptyList());
  }

  @Test
  public void testValidConfiguration() {
    validConfiguration().build().validate();
  }

  @Test
  public void testEmptyHost() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validConfiguration().setHost("").build().validate());

    assertThat(exception.getMessage(), Matchers.containsString("host must not be empty"));
  }

  @Test
  public void testInvalidPort() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validConfiguration().setPort(0).build().validate());

    assertThat(exception.getMessage(), Matchers.containsString("port must be between"));
  }

  @Test
  public void testEmptyTable() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validConfiguration().setTable("").build().validate());

    assertThat(exception.getMessage(), Matchers.containsString("table must not be empty"));
  }

  @Test
  public void testInvalidConnectionProperty() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                validConfiguration()
                    .setDebeziumConnectionProperties(Collections.singletonList("invalid-property"))
                    .build()
                    .validate());

    assertThat(exception.getMessage(), Matchers.containsString("Expected key=value format"));
  }

  @Test
  public void testNullConnectionProperty() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                validConfiguration()
                    .setDebeziumConnectionProperties(Arrays.asList("valid=value", null))
                    .build()
                    .validate());

    assertThat(exception.getMessage(), Matchers.containsString("Expected key=value format"));
  }

  @Test
  public void testConnectionPropertyValueContainingEquals() {
    validConfiguration()
        .setDebeziumConnectionProperties(
            Collections.singletonList("some.property=value=containing=equals"))
        .build()
        .validate();
  }

  @Test
  public void testUnsupportedConnector() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new DebeziumReadSchemaTransformProvider()
                    .from(validConfiguration().setDatabase("UNKNOWN").build()));

    assertThat(exception.getMessage(), Matchers.containsString("Unsupported connector 'UNKNOWN'"));
    assertThat(exception.getMessage(), Matchers.containsString("MYSQL"));
    assertThat(exception.getMessage(), Matchers.containsString("POSTGRES"));
  }

  @Test
  public void testProviderContract() {
    DebeziumReadSchemaTransformProvider provider = new DebeziumReadSchemaTransformProvider();

    assertThat(provider.inputCollectionNames(), Matchers.empty());
    assertThat(provider.outputCollectionNames(), Matchers.contains("output"));
    assertThat(
        provider.identifier(),
        Matchers.equalTo("beam:schematransform:org.apache.beam:debezium_read:v1"));
  }
}
