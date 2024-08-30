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
package org.apache.beam.it.neo4j;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.AdditionalMatchers.and;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.endsWith;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.exceptions.ClientException;
import org.testcontainers.containers.Neo4jContainer;

/** Unit tests for {@link Neo4jResourceManager}. */
@RunWith(JUnit4.class)
public class Neo4jResourceManagerTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Mock private Driver neo4jDriver;
  @Mock private Session session;
  @Mock private Result result;
  @Mock private Neo4jContainer<?> container;

  private static final String TEST_ID = "test-id";
  private static final String STATIC_DATABASE_NAME = "database";
  private static final String HOST = "localhost";
  private static final int NEO4J_BOLT_PORT = 7687;
  private static final int MAPPED_PORT = 10000;

  private Neo4jResourceManager testManager;

  @Before
  public void setUp() {
    when(container.getMappedPort(NEO4J_BOLT_PORT)).thenReturn(MAPPED_PORT);
    when(session.run(anyString(), anyMap())).thenReturn(result);
    when(neo4jDriver.session(any())).thenReturn(session);
    doReturn(container).when(container).withLogConsumer(any());

    testManager =
        new Neo4jResourceManager(neo4jDriver, container, Neo4jResourceManager.builder(TEST_ID));
  }

  @Test
  public void testCreateResourceManagerBuilderReturnsDefaultNeo4jResourceManager() {
    assertThat(
            Neo4jResourceManager.builder(TEST_ID)
                .setAdminPassword("letmein!")
                .setDriver(neo4jDriver)
                .useStaticContainer()
                .setHost(HOST)
                .setPort(NEO4J_BOLT_PORT)
                .build())
        .isInstanceOf(Neo4jResourceManager.class);
  }

  @Test
  public void testDatabaseIsCreatedWithNoWaitOptions() {
    Neo4jResourceManager.Builder builder =
        Neo4jResourceManager.builder(TEST_ID)
            .setDatabaseName(STATIC_DATABASE_NAME, DatabaseWaitOptions.noWaitDatabase());
    new Neo4jResourceManager(neo4jDriver, container, builder);

    verify(session).run(and(startsWith("CREATE DATABASE"), endsWith("NOWAIT")), anyMap());
  }

  @Test
  public void testGetUriShouldReturnCorrectValue() {
    assertThat(testManager.getUri()).matches("neo4j://" + HOST + ":" + MAPPED_PORT);
  }

  @Test
  public void testGetDatabaseNameShouldReturnCorrectValue() {
    assertThat(testManager.getDatabaseName()).matches(TEST_ID + "-\\d{8}-\\d{6}-\\d{6}");
  }

  @Test
  public void testDropDatabaseShouldThrowErrorIfDriverFailsToRunQuery() {
    doThrow(ClientException.class).when(session).run(anyString(), anyMap());

    assertThrows(
        Neo4jResourceManagerException.class,
        () -> testManager.dropDatabase(STATIC_DATABASE_NAME, DatabaseWaitOptions.noWaitDatabase()));
  }

  @Test
  public void testRunShouldThrowErrorIfDriverFailsToRunParameterlessQuery() {
    doThrow(ClientException.class).when(session).run(anyString(), anyMap());

    assertThrows(
        Neo4jResourceManagerException.class, () -> testManager.run("MATCH (n) RETURN n LIMIT 1"));
  }

  @Test
  public void testRunShouldThrowErrorIfDriverFailsToRunQuery() {
    doThrow(ClientException.class).when(session).run(anyString(), anyMap());

    assertThrows(
        Neo4jResourceManagerException.class,
        () ->
            testManager.run(
                "MATCH (n) WHERE n < $val RETURN n LIMIT 1", Collections.singletonMap("val", 2)));
  }

  @Test
  public void testCleanupAllShouldNotDropStaticDatabase() {
    Neo4jResourceManager.Builder builder =
        Neo4jResourceManager.builder(TEST_ID).setDatabaseName(STATIC_DATABASE_NAME);
    Neo4jResourceManager tm = new Neo4jResourceManager(neo4jDriver, container, builder);

    tm.cleanupAll();

    verify(session, never()).run(startsWith("DROP DATABASE"), anyMap());
    verify(neo4jDriver).close();
  }

  @Test
  public void testCleanupShouldDropNonStaticDatabase() {
    when(session.run(anyString(), anyMap())).thenReturn(mock(Result.class));

    testManager.cleanupAll();

    verify(session).run(startsWith("DROP DATABASE"), anyMap());
    verify(neo4jDriver).close();
  }

  @Test
  public void testCleanupAllShouldThrowErrorWhenNeo4jDriverFailsToDropDatabase() {
    doThrow(ClientException.class).when(session).run(anyString(), anyMap());

    assertThrows(Neo4jResourceManagerException.class, () -> testManager.cleanupAll());
  }

  @Test
  public void testCleanupAllShouldThrowErrorWhenNeo4jDriverFailsToClose() {
    doThrow(RuntimeException.class).when(neo4jDriver).close();

    assertThrows(Neo4jResourceManagerException.class, () -> testManager.cleanupAll());
  }
}
