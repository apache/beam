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

import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static java.util.stream.IntStream.rangeClosed;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Maps.transformValues;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.DeleteRequest;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.aws.dynamodb.DynamoDBIO.RetryConfiguration;
import org.apache.beam.sdk.io.aws.dynamodb.DynamoDBIO.Write.WriteFn;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DynamoDBIOWriteTest {
  private static final String tableName = "Test";

  @Rule public final TestPipeline pipeline = TestPipeline.create();
  @Rule public final ExpectedLogs writeFnLogs = ExpectedLogs.none(WriteFn.class);
  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Mock public AmazonDynamoDB client;

  @Test
  public void testWritePutItems() {
    List<Item> items = range(0, 100).mapToObj(Item::of).collect(toList());

    Supplier<List<Item>> capturePuts =
        captureBatchWrites(client, req -> req.getPutRequest().getItem());

    PCollection<Void> output =
        pipeline
            .apply(Create.of(items))
            .apply(
                DynamoDBIO.<Item>write()
                    .withWriteRequestMapperFn(putRequestMapper)
                    .withAwsClientsProvider(StaticAwsClientsProvider.of(client)));

    PAssert.that(output).empty();
    pipeline.run().waitUntilFinish();

    assertThat(capturePuts.get()).containsExactlyInAnyOrderElementsOf(items);
  }

  @Test
  public void testWritePutItemsWithDuplicates() {
    List<Item> items = range(0, 100).mapToObj(Item::of).collect(toList());

    Supplier<List<Item>> capturePuts =
        captureBatchWrites(client, req -> req.getPutRequest().getItem());

    pipeline
        .apply(Create.of(items))
        // generate identical duplicates
        .apply(ParDo.of(new AddDuplicatesDoFn(3, false)))
        .apply(
            DynamoDBIO.<Item>write()
                .withWriteRequestMapperFn(putRequestMapper)
                .withAwsClientsProvider(StaticAwsClientsProvider.of(client)));

    pipeline.run().waitUntilFinish();

    assertThat(capturePuts.get()).hasSize(100);
    assertThat(capturePuts.get()).containsExactlyInAnyOrderElementsOf(items);
  }

  @Test
  public void testWritePutItemsWithDuplicatesByKey() {
    List<Item> items = range(0, 100).mapToObj(Item::of).collect(toList());

    Supplier<List<Item>> capturePuts =
        captureBatchWrites(client, req -> req.getPutRequest().getItem());

    pipeline
        .apply(Create.of(items))
        // decorate duplicates so they are different
        .apply(ParDo.of(new AddDuplicatesDoFn(3, true)))
        .apply(
            DynamoDBIO.<Item>write()
                .withWriteRequestMapperFn(putRequestMapper)
                .withAwsClientsProvider(StaticAwsClientsProvider.of(client))
                .withDeduplicateKeys(ImmutableList.of("id")));

    pipeline.run().waitUntilFinish();

    assertThat(capturePuts.get()).hasSize(100);
    assertThat(capturePuts.get()).containsExactlyInAnyOrderElementsOf(items);
  }

  @Test
  public void testWriteDeleteItems() {
    List<Item> items = range(0, 100).mapToObj(Item::of).collect(toList());

    Supplier<List<Item>> captureDeletes =
        captureBatchWrites(client, req -> req.getDeleteRequest().getKey());

    PCollection<Void> output =
        pipeline
            .apply(Create.of(items))
            .apply(
                DynamoDBIO.<Item>write()
                    .withWriteRequestMapperFn(deleteRequestMapper)
                    .withAwsClientsProvider(StaticAwsClientsProvider.of(client)));

    PAssert.that(output).empty();
    pipeline.run().waitUntilFinish();

    assertThat(captureDeletes.get()).hasSize(100);
    assertThat(captureDeletes.get()).containsExactlyInAnyOrderElementsOf(items);
  }

  @Test
  public void testWriteDeleteItemsWithDuplicates() {
    List<Item> items = range(0, 100).mapToObj(Item::of).collect(toList());

    Supplier<List<Item>> captureDeletes =
        captureBatchWrites(client, req -> req.getDeleteRequest().getKey());

    pipeline
        .apply(Create.of(items))
        // generate identical duplicates
        .apply(ParDo.of(new AddDuplicatesDoFn(3, false)))
        .apply(
            DynamoDBIO.<Item>write()
                .withWriteRequestMapperFn(deleteRequestMapper)
                .withAwsClientsProvider(StaticAwsClientsProvider.of(client)));

    pipeline.run().waitUntilFinish();

    assertThat(captureDeletes.get()).hasSize(100);
    assertThat(captureDeletes.get()).containsExactlyInAnyOrderElementsOf(items);
  }

  @Test
  public void testWritePutItemsWithRetrySuccess() {
    when(client.batchWriteItem(any(BatchWriteItemRequest.class)))
        .thenThrow(
            AmazonDynamoDBException.class,
            AmazonDynamoDBException.class,
            AmazonDynamoDBException.class)
        .thenReturn(new BatchWriteItemResult());

    pipeline
        .apply(Create.of(Item.of(1)))
        .apply(
            "write",
            DynamoDBIO.<Item>write()
                .withWriteRequestMapperFn(putRequestMapper)
                .withAwsClientsProvider(StaticAwsClientsProvider.of(client))
                .withRetryConfiguration(try4Times));

    PipelineResult result = pipeline.run();
    result.waitUntilFinish();

    verify(client, times(4)).batchWriteItem(any(BatchWriteItemRequest.class));
    range(1, 4).forEach(i -> writeFnLogs.verifyWarn(String.format(WriteFn.RETRY_ATTEMPT_LOG, i)));
  }

  @Test
  public void testWritePutItemsWithRetryFailure() throws Throwable {
    thrown.expect(IOException.class);
    thrown.expectMessage("Error writing to DynamoDB");
    thrown.expectMessage("No more attempts allowed");

    when(client.batchWriteItem(any(BatchWriteItemRequest.class)))
        .thenThrow(AmazonDynamoDBException.class);

    pipeline
        .apply(Create.of(Item.of(1)))
        .apply(
            DynamoDBIO.<Item>write()
                .withWriteRequestMapperFn(putRequestMapper)
                .withAwsClientsProvider(StaticAwsClientsProvider.of(client))
                .withRetryConfiguration(try4Times));

    try {
      pipeline.run().waitUntilFinish();
    } catch (final Pipeline.PipelineExecutionException e) {
      verify(client, times(4)).batchWriteItem(any(BatchWriteItemRequest.class));
      range(1, 4).forEach(i -> writeFnLogs.verifyWarn(String.format(WriteFn.RETRY_ATTEMPT_LOG, i)));
      throw e.getCause();
    }
  }

  @DefaultCoder(AvroCoder.class)
  static class Item implements Serializable {
    Map<String, String> entries;

    private Item() {}

    private Item(Map<String, String> entries) {
      this.entries = entries;
    }

    static Item of(int id) {
      return new Item(ImmutableMap.of("id", String.valueOf(id)));
    }

    static Item of(Map<String, AttributeValue> attributes) {
      return new Item(ImmutableMap.copyOf(transformValues(attributes, a -> a.getS())));
    }

    Item withEntry(String key, String value) {
      return new Item(
          ImmutableMap.<String, String>builder().putAll(entries).put(key, value).build());
    }

    Map<String, AttributeValue> attributeMap() {
      return new HashMap<>(transformValues(entries, v -> new AttributeValue().withS(v)));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      return Objects.equals(entries, ((Item) o).entries);
    }

    @Override
    public int hashCode() {
      return Objects.hash(entries);
    }

    @Override
    public String toString() {
      return "Item" + entries;
    }
  }

  private Supplier<List<Item>> captureBatchWrites(
      AmazonDynamoDB mock, Function<WriteRequest, Map<String, AttributeValue>> extractor) {
    ArgumentCaptor<BatchWriteItemRequest> reqCaptor =
        ArgumentCaptor.forClass(BatchWriteItemRequest.class);
    when(mock.batchWriteItem(reqCaptor.capture())).thenReturn(new BatchWriteItemResult());

    return () ->
        reqCaptor.getAllValues().stream()
            .flatMap(req -> req.getRequestItems().values().stream())
            .flatMap(writes -> writes.stream())
            .map(extractor)
            .map(Item::of)
            .collect(toList());
  }

  private static SerializableFunction<Item, KV<String, WriteRequest>> putRequestMapper =
      item -> {
        PutRequest req = new PutRequest().withItem(item.attributeMap());
        return KV.of(tableName, new WriteRequest().withPutRequest(req));
      };

  private static SerializableFunction<Item, KV<String, WriteRequest>> deleteRequestMapper =
      key -> {
        DeleteRequest req = new DeleteRequest().withKey(key.attributeMap());
        return KV.of(tableName, new WriteRequest().withDeleteRequest(req));
      };

  private static RetryConfiguration try4Times =
      RetryConfiguration.create(4, Duration.standardSeconds(1), Duration.millis(1));

  /**
   * A DoFn that adds N duplicates to a bundle. The original is emitted last and is the only item
   * kept if deduplicating appropriately.
   */
  private static class AddDuplicatesDoFn extends DoFn<Item, Item> {
    private final int duplicates;
    private final SerializableBiFunction<Item, Integer, Item> decorator;

    AddDuplicatesDoFn(int duplicates, boolean decorate) {
      this.duplicates = duplicates;
      this.decorator =
          decorate ? (item, i) -> item.withEntry("duplicate", i.toString()) : (item, i) -> item;
    }

    @ProcessElement
    public void processElement(ProcessContext ctx) {
      Item original = ctx.element();
      rangeClosed(1, duplicates).forEach(i -> ctx.output(decorator.apply(original, i)));
      ctx.output(original);
    }
  }
}
