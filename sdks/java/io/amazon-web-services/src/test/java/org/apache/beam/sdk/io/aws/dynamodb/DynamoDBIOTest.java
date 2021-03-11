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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
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
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/** Test Coverage for the IO. */
public class DynamoDBIOTest implements Serializable {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public final transient ExpectedLogs expectedLogs = ExpectedLogs.none(DynamoDBIO.class);

  private static final String tableName = "TaskA";
  private static final int numOfItems = 10;

  private static List<Map<String, AttributeValue>> expected;

  @BeforeClass
  public static void setup() {
    DynamoDBIOTestHelper.startServerClient();
    DynamoDBIOTestHelper.createTestTable(tableName);
    expected = DynamoDBIOTestHelper.generateTestData(tableName, numOfItems);
  }

  @AfterClass
  public static void destroy() {
    DynamoDBIOTestHelper.stopServerClient(tableName);
  }

  // Test cases for Reader.
  @Test
  public void testReadScanResult() {
    PCollection<List<Map<String, AttributeValue>>> actual =
        pipeline.apply(
            DynamoDBIO.<List<Map<String, AttributeValue>>>read()
                .withAwsClientsProvider(
                    AwsClientsProviderMock.of(DynamoDBIOTestHelper.getDynamoDBClient()))
                .withScanRequestFn(
                    (SerializableFunction<Void, ScanRequest>)
                        input -> new ScanRequest(tableName).withTotalSegments(1))
                .items());
    PAssert.that(actual).containsInAnyOrder(expected);
    pipeline.run().waitUntilFinish();
  }

  // Test cases for Reader's arguments.
  @Test
  public void testMissingScanRequestFn() {
    thrown.expectMessage("withScanRequestFn() is required");
    pipeline.apply(
        DynamoDBIO.read()
            .withAwsClientsProvider(
                AwsClientsProviderMock.of(DynamoDBIOTestHelper.getDynamoDBClient())));
    try {
      pipeline.run().waitUntilFinish();
      fail("withScanRequestFn() is required");
    } catch (IllegalArgumentException ex) {
      assertEquals("withScanRequestFn() is required", ex.getMessage());
    }
  }

  @Test
  public void testMissingAwsClientsProvider() {
    thrown.expectMessage("withAwsClientsProvider() is required");
    pipeline.apply(
        DynamoDBIO.read()
            .withScanRequestFn(
                (SerializableFunction<Void, ScanRequest>)
                    input -> new ScanRequest(tableName).withTotalSegments(3)));
    try {
      pipeline.run().waitUntilFinish();
      fail("withAwsClientsProvider() is required");
    } catch (IllegalArgumentException ex) {
      assertEquals("withAwsClientsProvider() is required", ex.getMessage());
    }
  }

  @Test
  public void testMissingTotalSegments() {
    thrown.expectMessage("TotalSegments is required with withScanRequestFn()");
    pipeline.apply(
        DynamoDBIO.read()
            .withScanRequestFn(
                (SerializableFunction<Void, ScanRequest>) input -> new ScanRequest(tableName))
            .withAwsClientsProvider(
                AwsClientsProviderMock.of(DynamoDBIOTestHelper.getDynamoDBClient())));
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
                    input -> new ScanRequest(tableName).withTotalSegments(-1))
            .withAwsClientsProvider(
                AwsClientsProviderMock.of(DynamoDBIOTestHelper.getDynamoDBClient())));
    try {
      pipeline.run().waitUntilFinish();
      fail("withTotalSegments() is expected and greater than zero");
    } catch (IllegalArgumentException ex) {
      assertEquals(
          "TotalSegments is required with withScanRequestFn() and greater zero", ex.getMessage());
    }
  }

  // Test cases for Writer.
  @Test
  public void testWriteDataToDynamo() {
    final List<WriteRequest> writeRequests = DynamoDBIOTestHelper.generateWriteRequests(numOfItems);

    final PCollection<Void> output =
        pipeline
            .apply(Create.of(writeRequests))
            .apply(
                DynamoDBIO.<WriteRequest>write()
                    .withWriteRequestMapperFn(
                        (SerializableFunction<WriteRequest, KV<String, WriteRequest>>)
                            writeRequest -> KV.of(tableName, writeRequest))
                    .withRetryConfiguration(
                        DynamoDBIO.RetryConfiguration.create(5, Duration.standardMinutes(1)))
                    .withAwsClientsProvider(
                        AwsClientsProviderMock.of(DynamoDBIOTestHelper.getDynamoDBClient())));

    final PCollection<Long> publishedResultsSize = output.apply(Count.globally());
    PAssert.that(publishedResultsSize).containsInAnyOrder(0L);

    pipeline.run().waitUntilFinish();
  }

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testRetries() throws Throwable {
    thrown.expectMessage("Error writing to DynamoDB");

    final List<WriteRequest> writeRequests = DynamoDBIOTestHelper.generateWriteRequests(numOfItems);

    AmazonDynamoDB amazonDynamoDBMock = Mockito.mock(AmazonDynamoDB.class);
    Mockito.when(amazonDynamoDBMock.batchWriteItem(Mockito.any(BatchWriteItemRequest.class)))
        .thenThrow(new AmazonDynamoDBException("Service unavailable"));

    pipeline
        .apply(Create.of(writeRequests))
        .apply(
            DynamoDBIO.<WriteRequest>write()
                .withWriteRequestMapperFn(
                    (SerializableFunction<WriteRequest, KV<String, WriteRequest>>)
                        writeRequest -> KV.of(tableName, writeRequest))
                .withRetryConfiguration(
                    DynamoDBIO.RetryConfiguration.create(4, Duration.standardSeconds(10)))
                .withAwsClientsProvider(AwsClientsProviderMock.of(amazonDynamoDBMock)));

    try {
      pipeline.run().waitUntilFinish();
    } catch (final Pipeline.PipelineExecutionException e) {
      // check 3 retries were initiated by inspecting the log before passing on the exception
      expectedLogs.verifyWarn(String.format(DynamoDBIO.Write.WriteFn.RETRY_ATTEMPT_LOG, 1));
      expectedLogs.verifyWarn(String.format(DynamoDBIO.Write.WriteFn.RETRY_ATTEMPT_LOG, 2));
      expectedLogs.verifyWarn(String.format(DynamoDBIO.Write.WriteFn.RETRY_ATTEMPT_LOG, 3));
      throw e.getCause();
    }
    fail("Pipeline is expected to fail because we were unable to write to DynamoDB.");
  }

  /**
   * A DoFn used to generate outputs duplicated N times, where N is the input. Used to generate
   * bundles with duplicate elements.
   */
  private static class WriteDuplicateGeneratorDoFn extends DoFn<Integer, WriteRequest> {
    @ProcessElement
    public void processElement(ProcessContext ctx) {
      for (int i = 0; i < ctx.element(); i++) {
        DynamoDBIOTestHelper.generateWriteRequests(numOfItems).forEach(ctx::output);
      }
    }
  }

  @Test
  public void testWriteDeduplication() {
    // designate duplication factor for each bundle
    final List<Integer> duplications = Arrays.asList(1, 2, 3);

    final List<String> deduplicateKeys =
        Arrays.asList(DynamoDBIOTestHelper.ATTR_NAME_1, DynamoDBIOTestHelper.ATTR_NAME_2);

    AmazonDynamoDB amazonDynamoDBMock = Mockito.mock(AmazonDynamoDB.class);

    pipeline
        .apply(Create.of(duplications))
        .apply("duplicate", ParDo.of(new WriteDuplicateGeneratorDoFn()))
        .apply(
            DynamoDBIO.<WriteRequest>write()
                .withWriteRequestMapperFn(
                    (SerializableFunction<WriteRequest, KV<String, WriteRequest>>)
                        writeRequest -> KV.of(tableName, writeRequest))
                .withRetryConfiguration(
                    DynamoDBIO.RetryConfiguration.create(5, Duration.standardMinutes(1)))
                .withAwsClientsProvider(AwsClientsProviderMock.of(amazonDynamoDBMock))
                .withDeduplicateKeys(deduplicateKeys));

    pipeline.run().waitUntilFinish();

    ArgumentCaptor<BatchWriteItemRequest> argumentCaptor =
        ArgumentCaptor.forClass(BatchWriteItemRequest.class);
    Mockito.verify(amazonDynamoDBMock, Mockito.times(3)).batchWriteItem(argumentCaptor.capture());
    List<BatchWriteItemRequest> batchRequests = argumentCaptor.getAllValues();
    batchRequests.forEach(
        batchRequest -> {
          List<WriteRequest> requests = batchRequest.getRequestItems().get(tableName);
          // assert that each bundle contains expected number of items
          assertEquals(numOfItems, requests.size());
          List<Map<String, AttributeValue>> requestKeys =
              requests.stream()
                  .map(
                      request ->
                          request.getPutRequest() != null
                              ? request.getPutRequest().getItem()
                              : request.getDeleteRequest().getKey())
                  .collect(Collectors.toList());
          // assert no duplicate keys in each bundle
          assertEquals(new HashSet<>(requestKeys).size(), requestKeys.size());
        });
  }
}
