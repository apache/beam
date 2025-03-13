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
package org.apache.beam.sdk.io.aws2.dynamodb;

import static java.lang.Math.min;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables.getLast;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists.newArrayList;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists.transform;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.beam.sdk.io.aws2.MockClientBuilderFactory;
import org.apache.beam.sdk.io.aws2.dynamodb.DynamoDBIO.Read;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

@RunWith(MockitoJUnitRunner.class)
public class DynamoDBIOReadTest {
  private static final String tableName = "Test";

  @Rule public final TestPipeline pipeline = TestPipeline.create();
  @Rule public final ExpectedException thrown = ExpectedException.none();
  @Mock public DynamoDbClient client;

  @Before
  public void configureClientBuilderFactory() {
    MockClientBuilderFactory.set(pipeline, DynamoDbClientBuilder.class, client);
  }

  private Read<List<Map<String, AttributeValue>>> dynamoDbRead(Integer segments) {
    return DynamoDBIO.<List<Map<String, AttributeValue>>>read()
        .withScanRequestFn(
            in -> ScanRequest.builder().tableName(tableName).totalSegments(segments).build())
        .items();
  }

  @Test
  public void testReadOneSegment() {
    MockData mockData = new MockData(range(0, 10));
    mockData.mockScan(10, client); // 1 scan iteration

    Read<List<Map<String, AttributeValue>>> read = dynamoDbRead(1);
    PCollection<List<Map<String, AttributeValue>>> actual = pipeline.apply(read);

    PAssert.that(actual.apply(Count.globally())).containsInAnyOrder(1L);
    PAssert.that(actual).containsInAnyOrder(mockData.getAllItems());

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testReadWithCustomLimit() {
    final int requestedLimit = 100;
    MockData mockData = new MockData(range(0, 10));
    mockData.mockScan(requestedLimit, client); // 1 scan iteration

    pipeline.apply(
        dynamoDbRead(1)
            .withScanRequestFn(
                in ->
                    ScanRequest.builder()
                        .tableName(tableName)
                        .totalSegments(1)
                        .limit(requestedLimit)
                        .build()));

    pipeline.run().waitUntilFinish();

    verify(client).scan(argThat((ScanRequest req) -> requestedLimit == req.limit()));
  }

  @Test
  public void testReadThreeSegments() {
    MockData mockData = new MockData(range(0, 10), range(10, 20), range(20, 30));
    mockData.mockScan(10, client); // 1 scan iteration per segment

    Read<List<Map<String, AttributeValue>>> read = dynamoDbRead(3);
    PCollection<List<Map<String, AttributeValue>>> actual = pipeline.apply(read);

    PAssert.that(actual.apply(Count.globally())).containsInAnyOrder(3L);
    PAssert.that(actual.apply(Flatten.iterables())).containsInAnyOrder(mockData.getAllItems());

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testReadWithStartKey() {
    MockData mockData = new MockData(range(0, 10), range(20, 32));
    mockData.mockScan(5, client); // 2 + 3 scan iterations

    PCollection<List<Map<String, AttributeValue>>> actual = pipeline.apply(dynamoDbRead(2));

    PAssert.that(actual.apply(Count.globally())).containsInAnyOrder(5L);
    PAssert.that(actual.apply(Flatten.iterables())).containsInAnyOrder(mockData.getAllItems());

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testReadMissingScanRequestFn() {
    pipeline.enableAbandonedNodeEnforcement(false);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("withScanRequestFn() is required");

    pipeline.apply(dynamoDbRead(null).withScanRequestFn(null));
  }

  @Test
  public void testReadMissingTotalSegments() {
    pipeline.enableAbandonedNodeEnforcement(false);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("TotalSegments is required with withScanRequestFn() and greater zero");

    pipeline.apply(dynamoDbRead(null));
  }

  @Test
  public void testReadInvalidTotalSegments() {
    pipeline.enableAbandonedNodeEnforcement(false);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("TotalSegments is required with withScanRequestFn() and greater zero");

    pipeline.apply(dynamoDbRead(0));
  }

  private static class MockData {
    private final List<List<Integer>> data;

    MockData(IntStream... segments) {
      data = Arrays.stream(segments).map(ids -> newArrayList(ids.iterator())).collect(toList());
    }

    List<Map<String, AttributeValue>> getAllItems() {
      return data.stream().flatMap(ids -> ids.stream()).map(id -> item(id)).collect(toList());
    }

    void mockScan(int sizeLimit, DynamoDbClient mock) {
      for (int segment = 0; segment < data.size(); segment++) {
        List<Integer> ids = data.get(segment);

        List<Map<String, AttributeValue>> items = null;
        Map<String, AttributeValue> startKey, lastKey;
        for (int start = 0; start < ids.size(); start += sizeLimit) {
          startKey = items != null ? getLast(items) : ImmutableMap.of();
          items = transform(ids.subList(start, min(ids.size(), start + sizeLimit)), id -> item(id));
          lastKey = start + sizeLimit < ids.size() ? getLast(items) : ImmutableMap.of();

          when(mock.scan(argThat(matchesScanRequest(segment, startKey))))
              .thenReturn(ScanResponse.builder().items(items).lastEvaluatedKey(lastKey).build());
        }
      }
    }

    ArgumentMatcher<ScanRequest> matchesScanRequest(
        Integer segment, Map<String, AttributeValue> startKey) {
      return req ->
          req != null && segment.equals(req.segment()) && startKey.equals(req.exclusiveStartKey());
    }
  }

  private static Map<String, AttributeValue> item(int id) {
    return ImmutableMap.of(
        "rangeKey", AttributeValue.builder().n(String.valueOf(id)).build(),
        "hashKey", AttributeValue.builder().s(String.valueOf(id)).build());
  }
}
