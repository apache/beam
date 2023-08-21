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
package org.apache.beam.sdk.io.aws.dynamodb;

import static java.lang.Math.min;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables.getLast;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists.newArrayList;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists.transform;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DynamoDBIOReadTest {
  private static final String tableName = "Test";

  @Rule public final TestPipeline pipeline = TestPipeline.create();
  @Rule public final ExpectedException thrown = ExpectedException.none();
  @Mock public AmazonDynamoDB client;

  @Test
  public void testReadOneSegment() {
    MockData mockData = new MockData(range(0, 10));
    mockData.mockScan(10, client); // 1 scan iteration

    PCollection<List<Map<String, AttributeValue>>> actual =
        pipeline.apply(
            DynamoDBIO.<List<Map<String, AttributeValue>>>read()
                .withAwsClientsProvider(StaticAwsClientsProvider.of(client))
                .withScanRequestFn(
                    in -> new ScanRequest().withTableName(tableName).withTotalSegments(1))
                .items());

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
        DynamoDBIO.<List<Map<String, AttributeValue>>>read()
            .withAwsClientsProvider(StaticAwsClientsProvider.of(client))
            .withScanRequestFn(
                in ->
                    new ScanRequest()
                        .withTableName(tableName)
                        .withTotalSegments(1)
                        .withLimit(requestedLimit))
            .items());

    pipeline.run().waitUntilFinish();

    verify(client).scan(argThat((ScanRequest req) -> requestedLimit == req.getLimit()));
  }

  @Test
  public void testReadThreeSegments() {
    MockData mockData = new MockData(range(0, 10), range(10, 20), range(20, 30));
    mockData.mockScan(10, client); // 1 scan iteration per segment

    PCollection<List<Map<String, AttributeValue>>> actual =
        pipeline.apply(
            DynamoDBIO.<List<Map<String, AttributeValue>>>read()
                .withAwsClientsProvider(StaticAwsClientsProvider.of(client))
                .withScanRequestFn(
                    in -> new ScanRequest().withTableName(tableName).withTotalSegments(3))
                .items());

    PAssert.that(actual.apply(Count.globally())).containsInAnyOrder(3L);
    PAssert.that(actual.apply(Flatten.iterables())).containsInAnyOrder(mockData.getAllItems());

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testReadWithStartKey() {
    MockData mockData = new MockData(range(0, 10), range(20, 32));
    mockData.mockScan(5, client); // 2 + 3 scan iterations

    PCollection<List<Map<String, AttributeValue>>> actual =
        pipeline.apply(
            DynamoDBIO.<List<Map<String, AttributeValue>>>read()
                .withAwsClientsProvider(StaticAwsClientsProvider.of(client))
                .withScanRequestFn(
                    in -> new ScanRequest().withTableName(tableName).withTotalSegments(2))
                .items());

    PAssert.that(actual.apply(Count.globally())).containsInAnyOrder(5L);
    PAssert.that(actual.apply(Flatten.iterables())).containsInAnyOrder(mockData.getAllItems());

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testReadMissingScanRequestFn() {
    pipeline.enableAbandonedNodeEnforcement(false);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("withScanRequestFn() is required");

    pipeline.apply(DynamoDBIO.read().withAwsClientsProvider(StaticAwsClientsProvider.of(client)));
  }

  @Test
  public void testReadMissingAwsClientsProvider() {
    pipeline.enableAbandonedNodeEnforcement(false);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("withAwsClientsProvider() is required");

    pipeline.apply(DynamoDBIO.read().withScanRequestFn(in -> new ScanRequest()));
  }

  @Test
  public void testReadMissingTotalSegments() {
    pipeline.enableAbandonedNodeEnforcement(false);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("TotalSegments is required with withScanRequestFn() and greater zero");

    pipeline.apply(
        DynamoDBIO.read()
            .withAwsClientsProvider(StaticAwsClientsProvider.of(client))
            .withScanRequestFn(in -> new ScanRequest()));
  }

  @Test
  public void testReadInvalidTotalSegments() {
    pipeline.enableAbandonedNodeEnforcement(false);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("TotalSegments is required with withScanRequestFn() and greater zero");

    pipeline.apply(
        DynamoDBIO.read()
            .withAwsClientsProvider(StaticAwsClientsProvider.of(client))
            .withScanRequestFn(in -> new ScanRequest().withTotalSegments(0)));
  }

  private static class MockData {
    private final List<List<Integer>> data;

    MockData(IntStream... segments) {
      data = Arrays.stream(segments).map(ids -> newArrayList(ids.iterator())).collect(toList());
    }

    List<Map<String, AttributeValue>> getAllItems() {
      return data.stream().flatMap(ids -> ids.stream()).map(id -> item(id)).collect(toList());
    }

    void mockScan(int sizeLimit, AmazonDynamoDB mock) {
      for (int segment = 0; segment < data.size(); segment++) {
        List<Integer> ids = data.get(segment);

        List<Map<String, AttributeValue>> items = null;
        Map<String, AttributeValue> startKey, lastKey;
        for (int start = 0; start < ids.size(); start += sizeLimit) {
          startKey = items != null ? getLast(items) : null;
          items = transform(ids.subList(start, min(ids.size(), start + sizeLimit)), id -> item(id));
          lastKey = start + sizeLimit < ids.size() ? getLast(items) : null;

          when(mock.scan(argThat(matchesScanRequest(segment, startKey))))
              .thenReturn(new ScanResult().withItems(items).withLastEvaluatedKey(lastKey));
        }
      }
    }

    ArgumentMatcher<ScanRequest> matchesScanRequest(
        Integer segment, Map<String, AttributeValue> startKey) {
      return req ->
          req != null
              && segment.equals(req.getSegment())
              && Objects.equals(startKey, req.getExclusiveStartKey());
    }
  }

  private static Map<String, AttributeValue> item(int id) {
    return ImmutableMap.of(
        "rangeKey", new AttributeValue().withN(String.valueOf(id)),
        "hashKey", new AttributeValue().withS(String.valueOf(id)));
  }
}
