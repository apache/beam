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
package org.apache.beam.it.jdbc;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.testcontainers.containers.MySQLContainer;

/** Integration tests for {@link MySQLResourceManager}. */
@RunWith(JUnit4.class)
public class MySQLResourceManagerTest<T extends MySQLContainer<T>> {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock private T container;

  private MySQLResourceManager testManager;

  private static final String TEST_ID = "test_id";

  @Before
  public void setUp() {
    when(container.withUsername(any())).thenReturn(container);
    when(container.withPassword(any())).thenReturn(container);
    when(container.withDatabaseName(anyString())).thenReturn(container);
    testManager = new MySQLResourceManager(container, new MySQLResourceManager.Builder(TEST_ID));
  }

  @Test
  public void testGetJDBCPortReturnsCorrectValue() {
    assertThat(testManager.getJDBCPort()).isEqualTo(MySQLContainer.MYSQL_PORT);
  }
}
