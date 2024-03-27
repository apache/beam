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
package org.apache.beam.it.cassandra;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.RejectedExecutionException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.testcontainers.containers.CassandraContainer;

/** Unit tests for {@link CassandraResourceManager}. */
@RunWith(JUnit4.class)
public class CassandraResourceManagerTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock private CqlSession cassandraClient;
  @Mock private CassandraContainer<?> container;

  private static final String TEST_ID = "test-id";
  private static final String COLLECTION_NAME = "collection-name";
  private static final String STATIC_KEYSPACE_NAME = "keyspace";
  private static final String HOST = "localhost";

  private CassandraResourceManager testManager;

  @Before
  public void setUp() {
    doReturn(container).when(container).withLogConsumer(any());

    testManager =
        new CassandraResourceManager(
            cassandraClient, container, CassandraResourceManager.builder(TEST_ID));
  }

  @Test
  public void testGetUriShouldReturnCorrectValue() {
    assertThat(testManager.getHost()).matches(HOST);
  }

  @Test
  public void testGetKeyspaceNameShouldReturnCorrectValue() {
    assertThat(testManager.getKeyspaceName())
        .matches(TEST_ID.replace('-', '_') + "_\\d{8}_\\d{6}_\\d{6}");
  }

  @Test
  public void testInsertDocumentsShouldThrowErrorWhenCassandraThrowsException() {

    doThrow(RejectedExecutionException.class)
        .when(cassandraClient)
        .execute(any(SimpleStatement.class));

    assertThrows(
        CassandraResourceManagerException.class,
        () -> testManager.insertDocument(COLLECTION_NAME, new HashMap<>()));
  }

  @Test
  public void testCleanupAllShouldNotDropStaticDatabase() throws IOException {
    CassandraResourceManager.Builder builder =
        CassandraResourceManager.builder(TEST_ID).setKeyspaceName(STATIC_KEYSPACE_NAME);
    CassandraResourceManager tm = new CassandraResourceManager(cassandraClient, container, builder);

    tm.cleanupAll();

    verify(cassandraClient, never()).execute(any(SimpleStatement.class));
    verify(cassandraClient).close();
  }

  @Test
  public void testCleanupShouldDropNonStaticDatabase() {

    testManager.cleanupAll();

    verify(cassandraClient).execute(any(SimpleStatement.class));
    verify(cassandraClient).close();
  }

  @Test
  public void testCleanupAllShouldThrowErrorWhenCassandraClientFailsToDropDatabase() {
    doThrow(RuntimeException.class).when(cassandraClient).execute(any(SimpleStatement.class));

    assertThrows(CassandraResourceManagerException.class, () -> testManager.cleanupAll());
  }

  @Test
  public void testCleanupAllShouldThrowErrorWhenCassandraClientFailsToClose() {
    doThrow(RuntimeException.class).when(cassandraClient).close();

    assertThrows(CassandraResourceManagerException.class, () -> testManager.cleanupAll());
  }
}
