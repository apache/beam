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

import static org.apache.beam.sdk.io.aws2.dynamodb.DynamoDBIO.RetryConfiguration.DEFAULT_RETRY_PREDICATE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

/** Test Coverage for the IO. */
public class DynamoDBIOTest implements Serializable {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public final transient ExpectedLogs expectedLogs = ExpectedLogs.none(DynamoDBIO.class);

  private static final String tableName = "TaskA";
  private static final int numOfItems = 10;

  @BeforeClass
  public static void setup() {
    DynamoDBIOTestHelper.startServerClient();
  }

  @AfterClass
  public static void destroy() {
    DynamoDBIOTestHelper.stopServerClient(tableName);
  }

  @Before
  public void createTable() {
    DynamoDBIOTestHelper.createTestTable(tableName);
  }

  @After
  public void cleanTable() {
    DynamoDBIOTestHelper.deleteTestTable(tableName);
  }

  // Test cases for Reader.
  @Test
  public void testReaderOneSegment() {
    List<Map<String, AttributeValue>> expected =
        DynamoDBIOTestHelper.generateTestData(tableName, numOfItems);

    PCollection<List<Map<String, AttributeValue>>> actual =
        pipeline.apply(
            DynamoDBIO.<List<Map<String, AttributeValue>>>read()
                .withDynamoDbClientProvider(
                    DynamoDbClientProviderMock.of(DynamoDBIOTestHelper.getDynamoDBClient()))
                .withScanRequestFn(
                    (SerializableFunction<Void, ScanRequest>)
                        input ->
                            ScanRequest.builder().tableName(tableName).totalSegments(1).build())
                .items());
    PAssert.that(actual).containsInAnyOrder(expected);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testReaderThreeSegments() {
    TupleTag<List<Map<String, AttributeValue>>> outputTag = new TupleTag<>();
    PCollectionTuple writeOutput =
        pipeline
            .apply(
                DynamoDBIO.<List<Map<String, AttributeValue>>>read()
                    .withDynamoDbClientProvider(
                        DynamoDbClientProviderMock.of(DynamoDBIOTestHelper.getDynamoDBClient()))
                    .withScanRequestFn(
                        (SerializableFunction<Void, ScanRequest>)
                            input ->
                                ScanRequest.builder().tableName(tableName).totalSegments(3).build())
                    .items())
            .apply(
                ParDo.of(
                        new DoFn<
                            List<Map<String, AttributeValue>>,
                            List<Map<String, AttributeValue>>>() {
                          @ProcessElement
                          public void processElement(
                              @Element List<Map<String, AttributeValue>> input,
                              OutputReceiver<List<Map<String, AttributeValue>>> out) {
                            out.output(input);
                          }
                        })
                    .withOutputTags(outputTag, TupleTagList.empty()));

    final PCollection<Long> resultSetCount = writeOutput.get(outputTag).apply(Count.globally());
    // Since we don't know what item will fall into what segment, so assert 3 result set returned
    PAssert.that(resultSetCount).containsInAnyOrder(ImmutableList.of(3L));
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testReaderWithLimit() {
    List<Map<String, AttributeValue>> expected =
        DynamoDBIOTestHelper.generateTestData(tableName, numOfItems);

    // Maximum number of records in scan result
    final int limit = 5;

    PCollection<Map<String, AttributeValue>> actual =
        pipeline
            .apply(
                DynamoDBIO.<List<Map<String, AttributeValue>>>read()
                    .withDynamoDbClientProvider(
                        DynamoDbClientProviderMock.of(DynamoDBIOTestHelper.getDynamoDBClient()))
                    .withScanRequestFn(
                        (SerializableFunction<Void, ScanRequest>)
                            input ->
                                ScanRequest.builder()
                                    .tableName(tableName)
                                    .totalSegments(1)
                                    .limit(limit)
                                    .build())
                    .items())
            .apply(ParDo.of(new IterateListDoFn()));
    PAssert.that(actual).containsInAnyOrder(expected);
    pipeline.run().waitUntilFinish();
  }

  // Test cases for Reader's arguments.
  @Test
  public void testMissingScanRequestFn() {
    thrown.expectMessage("withScanRequestFn() is required");
    pipeline.apply(
        DynamoDBIO.read()
            .withDynamoDbClientProvider(
                DynamoDbClientProviderMock.of(DynamoDBIOTestHelper.getDynamoDBClient())));
    try {
      pipeline.run().waitUntilFinish();
      fail("withScanRequestFn() is required");
    } catch (IllegalArgumentException ex) {
      assertEquals("withScanRequestFn() is required", ex.getMessage());
    }
  }

  @Test
  public void testMissingDynamoDbClientProvider() {
    thrown.expectMessage("withDynamoDbClientProvider() is required");
    pipeline.apply(
        DynamoDBIO.read()
            .withScanRequestFn(
                (SerializableFunction<Void, ScanRequest>)
                    input -> ScanRequest.builder().tableName(tableName).totalSegments(3).build()));
    try {
      pipeline.run().waitUntilFinish();
      fail("withDynamoDbClientProvider() is required");
    } catch (IllegalArgumentException ex) {
      assertEquals("withDynamoDbClientProvider() is required", ex.getMessage());
    }
  }

  @Test
  public void testMissingTotalSegments() {
    thrown.expectMessage("TotalSegments is required with withScanRequestFn()");
    pipeline.apply(
        DynamoDBIO.read()
            .withScanRequestFn(
                (SerializableFunction<Void, ScanRequest>)
                    input -> ScanRequest.builder().tableName(tableName).build())
            .withDynamoDbClientProvider(
                DynamoDbClientProviderMock.of(DynamoDBIOTestHelper.getDynamoDBClient())));
    try {
      pipeline.run().waitUntilFinish();
      fail("TotalSegments is required with withScanRequestFn()");
    } catch (IllegalArgumentException ex) {
      assertEquals("TotalSegments is required with withScanRequestFn()", ex.getMessage());
    }
  }

  @Test
  public void testNegativeTotalSegments() {
    thrown.expectMessage("TotalSegments is required with withScanRequestFn() and greater zero");
    pipeline.apply(
        DynamoDBIO.read()
            .withScanRequestFn(
                (SerializableFunction<Void, ScanRequest>)
                    input -> ScanRequest.builder().tableName(tableName).totalSegments(-1).build())
            .withDynamoDbClientProvider(
                DynamoDbClientProviderMock.of(DynamoDBIOTestHelper.getDynamoDBClient())));
    try {
      pipeline.run().waitUntilFinish();
      fail("withTotalSegments() is expected and greater than zero");
    } catch (IllegalArgumentException ex) {
      assertEquals(
          "TotalSegments is required with withScanRequestFn() and greater zero", ex.getMessage());
    }
  }

  @Test
  public void testWriteDataToDynamo() {
    List<KV<String, Integer>> items =
        ImmutableList.of(KV.of("test1", 111), KV.of("test2", 222), KV.of("test3", 333));

    final PCollection<Void> output =
        pipeline
            .apply(Create.of(items))
            .apply(
                DynamoDBIO.<KV<String, Integer>>write()
                    .withWriteRequestMapperFn(
                        (SerializableFunction<KV<String, Integer>, KV<String, WriteRequest>>)
                            entry -> {
                              Map<String, AttributeValue> putRequest =
                                  ImmutableMap.of(
                                      "hashKey1",
                                          AttributeValue.builder().s(entry.getKey()).build(),
                                      "rangeKey2",
                                          AttributeValue.builder()
                                              .n(entry.getValue().toString())
                                              .build());

                              WriteRequest writeRequest =
                                  WriteRequest.builder()
                                      .putRequest(PutRequest.builder().item(putRequest).build())
                                      .build();
                              return KV.of(tableName, writeRequest);
                            })
                    .withRetryConfiguration(
                        DynamoDBIO.RetryConfiguration.builder()
                            .setMaxAttempts(5)
                            .setMaxDuration(Duration.standardMinutes(1))
                            .setRetryPredicate(DEFAULT_RETRY_PREDICATE)
                            .build())
                    .withDynamoDbClientProvider(
                        DynamoDbClientProviderMock.of(DynamoDBIOTestHelper.getDynamoDBClient())));

    final PCollection<Long> publishedResultsSize = output.apply(Count.globally());
    PAssert.that(publishedResultsSize).containsInAnyOrder(0L);

    pipeline.run().waitUntilFinish();

    // Make sure data written to the table are in the table.
    int actualItemCount = DynamoDBIOTestHelper.readDataFromTable(tableName).size();
    assertEquals(3, actualItemCount);
  }

  @Rule public transient ExpectedException thrown = ExpectedException.none();

  @Test
  public void testRetries() throws Throwable {
    thrown.expectMessage("Error writing to DynamoDB");

    List<KV<String, Integer>> items =
        ImmutableList.of(KV.of("test1", 111), KV.of("test2", 222), KV.of("test3", 333));

    DynamoDbClient amazonDynamoDBMock = Mockito.mock(DynamoDbClient.class);
    Mockito.when(amazonDynamoDBMock.batchWriteItem(Mockito.any(BatchWriteItemRequest.class)))
        .thenThrow(DynamoDbException.builder().message("Service unavailable").build());

    pipeline
        .apply(Create.of(items))
        .apply(
            DynamoDBIO.<KV<String, Integer>>write()
                .withWriteRequestMapperFn(
                    (SerializableFunction<KV<String, Integer>, KV<String, WriteRequest>>)
                        entry -> {
                          Map<String, AttributeValue> putRequest =
                              ImmutableMap.of(
                                  "hashKey1", AttributeValue.builder().s(entry.getKey()).build(),
                                  "rangeKey2",
                                      AttributeValue.builder()
                                          .n(entry.getValue().toString())
                                          .build());

                          WriteRequest writeRequest =
                              WriteRequest.builder()
                                  .putRequest(PutRequest.builder().item(putRequest).build())
                                  .build();
                          return KV.of(tableName, writeRequest);
                        })
                .withRetryConfiguration(
                    DynamoDBIO.RetryConfiguration.builder()
                        .setMaxAttempts(4)
                        .setMaxDuration(Duration.standardSeconds(10))
                        .setRetryPredicate(DEFAULT_RETRY_PREDICATE)
                        .build())
                .withDynamoDbClientProvider(DynamoDbClientProviderMock.of(amazonDynamoDBMock)));

    try {
      pipeline.run().waitUntilFinish();
    } catch (final Pipeline.PipelineExecutionException e) {
      // check 3 retries were initiated by inspecting the log before passing on the exception
      expectedLogs.verifyWarn(String.format(DynamoDBIO.Write.WriteFn.RETRY_ATTEMPT_LOG, 1));
      expectedLogs.verifyWarn(String.format(DynamoDBIO.Write.WriteFn.RETRY_ATTEMPT_LOG, 2));
      expectedLogs.verifyWarn(String.format(DynamoDBIO.Write.WriteFn.RETRY_ATTEMPT_LOG, 3));
      throw e.getCause();
    }
    fail("Pipeline is expected to fail because we were unable to write to DynamoDb.");
  }

  /**
   * A DoFn used to generate outputs duplicated N times, where N is the input. Used to generate
   * bundles with duplicate elements.
   */
  private static class WriteDuplicateGeneratorDoFn extends DoFn<Integer, KV<String, Integer>> {
    @ProcessElement
    public void processElement(ProcessContext ctx) {
      for (int i = 0; i < ctx.element(); i++) {
        for (int j = 1; j <= numOfItems; j++) {
          KV<String, Integer> item = KV.of("test" + j, 1000 + j);
          ctx.output(item);
        }
      }
    }
  }

  @Test
  public void testWriteDeduplication() {
    // designate duplication factor for each bundle
    final List<Integer> duplications = Arrays.asList(1, 2, 3);

    final List<String> deduplicateKeys = Arrays.asList("hashKey1", "rangeKey2");

    DynamoDbClient amazonDynamoDBMock = Mockito.mock(DynamoDbClient.class);

    pipeline
        .apply(Create.of(duplications))
        .apply("duplicate", ParDo.of(new WriteDuplicateGeneratorDoFn()))
        .apply(
            DynamoDBIO.<KV<String, Integer>>write()
                .withWriteRequestMapperFn(
                    (SerializableFunction<KV<String, Integer>, KV<String, WriteRequest>>)
                        entry -> {
                          Map<String, AttributeValue> putRequest =
                              ImmutableMap.of(
                                  "hashKey1",
                                  AttributeValue.builder().s(entry.getKey()).build(),
                                  "rangeKey2",
                                  AttributeValue.builder().n(entry.getValue().toString()).build());

                          WriteRequest writeRequest =
                              WriteRequest.builder()
                                  .putRequest(PutRequest.builder().item(putRequest).build())
                                  .build();
                          return KV.of(tableName, writeRequest);
                        })
                .withRetryConfiguration(
                    DynamoDBIO.RetryConfiguration.builder()
                        .setMaxAttempts(5)
                        .setMaxDuration(Duration.standardMinutes(1))
                        .setRetryPredicate(DEFAULT_RETRY_PREDICATE)
                        .build())
                .withDynamoDbClientProvider(DynamoDbClientProviderMock.of(amazonDynamoDBMock))
                .withDeduplicateKeys(deduplicateKeys));

    pipeline.run().waitUntilFinish();

    ArgumentCaptor<BatchWriteItemRequest> argumentCaptor =
        ArgumentCaptor.forClass(BatchWriteItemRequest.class);
    Mockito.verify(amazonDynamoDBMock, Mockito.times(3)).batchWriteItem(argumentCaptor.capture());
    List<BatchWriteItemRequest> batchRequests = argumentCaptor.getAllValues();
    batchRequests.forEach(
        batchRequest -> {
          List<WriteRequest> requests = batchRequest.requestItems().get(tableName);
          // assert that each bundle contains expected number of items
          assertEquals(numOfItems, requests.size());
          List<Map<String, AttributeValue>> requestKeys =
              requests.stream()
                  .map(
                      request ->
                          request.putRequest() != null
                              ? request.putRequest().item()
                              : request.deleteRequest().key())
                  .collect(Collectors.toList());
          // assert no duplicate keys in each bundle
          assertEquals(new HashSet<>(requestKeys).size(), requestKeys.size());
        });
  }

  private static class IterateListDoFn
      extends DoFn<List<Map<String, AttributeValue>>, Map<String, AttributeValue>> {

    @ProcessElement
    public void processElement(
        @Element List<Map<String, AttributeValue>> items,
        OutputReceiver<Map<String, AttributeValue>> out) {
      for (Map<String, AttributeValue> item : items) {
        out.output(item);
      }
    }
  }
}
