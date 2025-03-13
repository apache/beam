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
package org.apache.beam.it.gcp.datastore;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.GqlQuery;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.QueryResults;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Unit tests for {@link DatastoreResourceManager}. */
public class DatastoreResourceManagerTest {

  @Mock private Datastore datastoreMock;
  private DatastoreResourceManager resourceManager;
  @Mock private KeyFactory keyFactory;
  @Mock private Key key;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    when(datastoreMock.newKeyFactory()).thenReturn(keyFactory);
    when(keyFactory.newKey(anyLong())).thenReturn(key);
    when(keyFactory.setKind(anyString())).thenReturn(keyFactory);
    when(keyFactory.setNamespace(anyString())).thenReturn(keyFactory);

    resourceManager = new DatastoreResourceManager("test-namespace", datastoreMock);
  }

  @Test
  public void testInsert() {
    // Prepare test data
    Map<Long, FullEntity<?>> entities = new HashMap<>();
    Entity entity = Entity.newBuilder(datastoreMock.newKeyFactory().newKey(1L)).build();
    entities.put(1L, entity);

    // Mock the Datastore put method
    when(datastoreMock.put(any(FullEntity.class))).thenReturn(entity);

    // Execute the method under test
    List<Entity> result = resourceManager.insert("test_kind", entities);

    // Verify the result
    assertThat(result).hasSize(1);
    assertThat(result).contains(entity);
  }

  @Test
  public void testQuery() {
    // Prepare test data
    String gqlQuery = "SELECT * FROM test_kind";

    // Mock the Datastore run method
    QueryResults<Entity> mockResult = mock(QueryResults.class);
    Entity mockEntity = mock(Entity.class);
    when(datastoreMock.run(any(GqlQuery.class))).thenReturn(mockResult);
    when(mockResult.hasNext()).thenReturn(true).thenReturn(false);
    when(mockResult.next()).thenReturn(mockEntity);

    Key mockKey = mock(Key.class);
    when(mockEntity.getKey()).thenReturn(mockKey);
    when(mockKey.getNamespace()).thenReturn("test-namespace");

    // Execute the method under test
    List<Entity> result = resourceManager.query(gqlQuery);
    resourceManager.cleanupAll();

    // Verify the result
    assertThat(result).isNotEmpty();
    assertThat(result.get(0)).isEqualTo(mockEntity);

    verify(datastoreMock).delete(mockKey);
  }

  @Test
  public void testCleanupAll() {
    // Prepare test data
    Key key = datastoreMock.newKeyFactory().newKey(1L);
    resourceManager.insert(
        "test_kind", Collections.singletonMap(1L, Entity.newBuilder(key).build()));

    // Execute the method under test
    // Calling twice to assert that the key is just deleted once
    resourceManager.cleanupAll();
    resourceManager.cleanupAll();

    // Verify that the Datastore delete method was called with the correct key
    verify(datastoreMock, times(1)).delete(key);
  }
}
