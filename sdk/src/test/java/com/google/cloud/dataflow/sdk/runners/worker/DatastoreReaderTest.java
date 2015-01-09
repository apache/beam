/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.api.services.datastore.client.DatastoreHelper.makeProperty;
import static com.google.api.services.datastore.client.DatastoreHelper.makeValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.services.datastore.DatastoreV1.Entity;
import com.google.api.services.datastore.DatastoreV1.EntityResult;
import com.google.api.services.datastore.DatastoreV1.EntityResult.ResultType;
import com.google.api.services.datastore.DatastoreV1.Query;
import com.google.api.services.datastore.DatastoreV1.QueryResultBatch;
import com.google.api.services.datastore.DatastoreV1.QueryResultBatch.MoreResultsType;
import com.google.api.services.datastore.DatastoreV1.RunQueryRequest;
import com.google.api.services.datastore.DatastoreV1.RunQueryResponse;
import com.google.api.services.datastore.client.Datastore;
import com.google.api.services.datastore.client.DatastoreException;
import com.google.cloud.dataflow.sdk.io.DatastoreIO;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentMatcher;

import java.util.ArrayList;
import java.util.List;

/**
 * Unit tests for {@code DatastoreSource}.
 */
@RunWith(JUnit4.class)
public class DatastoreReaderTest {
  private static final String TEST_HOST = "http://localhost:8080";
  private static final String TEST_KIND = "mykind";
  private static final String TEST_DATASET = "mydataset";
  private static final String TEST_PROPERTY = "myproperty";

  private static class IsValidRequest extends ArgumentMatcher<RunQueryRequest> {
    @Override
    public boolean matches(Object o) {
      RunQueryRequest request = (RunQueryRequest) o;
      return request.hasQuery();
    }
  }

  private EntityResult createEntityResult(String kind, String val) {
    Entity entity = Entity.newBuilder().addProperty(
        makeProperty(TEST_PROPERTY, makeValue(val))).build();
    return EntityResult.newBuilder().setEntity(entity).build();
  }

  private Datastore buildMockDatastore() throws DatastoreException {
    Datastore datastore = mock(Datastore.class);
    RunQueryResponse.Builder firstResponseBuilder = RunQueryResponse.newBuilder();
    RunQueryResponse.Builder secondResponseBuilder = RunQueryResponse.newBuilder();
    RunQueryResponse.Builder thirdResponseBuilder = RunQueryResponse.newBuilder();
    {
      QueryResultBatch.Builder resultsBatch = QueryResultBatch.newBuilder();
      resultsBatch.addEntityResult(0, createEntityResult(TEST_KIND, "val0"));
      resultsBatch.addEntityResult(1, createEntityResult(TEST_KIND, "val1"));
      resultsBatch.addEntityResult(2, createEntityResult(TEST_KIND, "val2"));
      resultsBatch.addEntityResult(3, createEntityResult(TEST_KIND, "val3"));
      resultsBatch.addEntityResult(4, createEntityResult(TEST_KIND, "val4"));
      resultsBatch.setEntityResultType(ResultType.FULL);

      resultsBatch.setMoreResults(MoreResultsType.NOT_FINISHED);

      firstResponseBuilder.setBatch(resultsBatch.build());
    }
    {
      QueryResultBatch.Builder resultsBatch = QueryResultBatch.newBuilder();
      resultsBatch.addEntityResult(0, createEntityResult(TEST_KIND, "val5"));
      resultsBatch.addEntityResult(1, createEntityResult(TEST_KIND, "val6"));
      resultsBatch.addEntityResult(2, createEntityResult(TEST_KIND, "val7"));
      resultsBatch.addEntityResult(3, createEntityResult(TEST_KIND, "val8"));
      resultsBatch.addEntityResult(4, createEntityResult(TEST_KIND, "val9"));
      resultsBatch.setEntityResultType(ResultType.FULL);

      resultsBatch.setMoreResults(MoreResultsType.NOT_FINISHED);

      secondResponseBuilder.setBatch(resultsBatch.build());
    }
    {
      QueryResultBatch.Builder resultsBatch = QueryResultBatch.newBuilder();
      resultsBatch.setEntityResultType(ResultType.FULL);

      resultsBatch.setMoreResults(MoreResultsType.NO_MORE_RESULTS);

      thirdResponseBuilder.setBatch(resultsBatch.build());
    }
    when(datastore.runQuery(argThat(new IsValidRequest())))
        .thenReturn(firstResponseBuilder.build())
        .thenReturn(secondResponseBuilder.build())
        .thenReturn(thirdResponseBuilder.build());
    return datastore;
  }


  @Test
  public void testRead() throws Exception {
    Datastore datastore = buildMockDatastore();

    Query.Builder q = Query.newBuilder();
    q.addKindBuilder().setName(TEST_KIND);
    Query query = q.build();

    DatastoreIO.DatastoreReader iterator = new DatastoreIO.DatastoreReader(query, datastore);

    List<Entity> entityResults = new ArrayList<Entity>();
    while (iterator.advance()) {
      entityResults.add(iterator.getCurrent());
    }

    assertEquals(10, entityResults.size());
    for (int i = 0; i < 10; i++) {
      assertNotNull(entityResults.get(i).getPropertyList());
      assertEquals(entityResults.get(i).getPropertyList().size(), 1);
      assertTrue(entityResults.get(i).getPropertyList().get(0).hasValue());
      assertTrue(entityResults.get(i).getPropertyList().get(0).getValue().hasStringValue());
      assertEquals(
          entityResults.get(i).getPropertyList().get(0).getValue().getStringValue(), "val" + i);
    }
  }
}
