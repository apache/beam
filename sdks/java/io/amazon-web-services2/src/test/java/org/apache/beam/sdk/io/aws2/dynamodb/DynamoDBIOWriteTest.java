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

import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static java.util.stream.IntStream.rangeClosed;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps.filterKeys;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Maps.transformValues;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.aws2.MockClientBuilderFactory;
import org.apache.beam.sdk.io.aws2.dynamodb.DynamoDBIO.Write;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

@RunWith(MockitoJUnitRunner.class)
public class DynamoDBIOWriteTest {
  private static final String tableName = "Test";

  @Rule public final TestPipeline pipeline = TestPipeline.create();
  @Rule public final ExpectedException thrown = ExpectedException.none();

  @Mock public DynamoDbClient client;

  @Before
  public void configureClientBuilderFactory() {
    MockClientBuilderFactory.set(pipeline, DynamoDbClientBuilder.class, client);
  }

  @Test
  public void testWritePutItems() {
    List<Item> items = Item.range(0, 100);

    Supplier<List<Item>> capturePuts = captureBatchWrites(client, req -> req.putRequest().item());

    Write<Item> write = DynamoDBIO.<Item>write().withWriteRequestMapperFn(putRequestMapper);
    PCollection<Void> output = pipeline.apply(Create.of(items)).apply(write);

    PAssert.that(output).empty();
    pipeline.run().waitUntilFinish();

    assertThat(capturePuts.get()).containsExactlyInAnyOrderElementsOf(items);
  }

  @Test
  public void testWritePutItemsWithDuplicates() {
    List<Item> items = Item.range(0, 100);

    Supplier<List<List<Item>>> captureRequests =
        captureBatchWriteRequests(client, req -> req.putRequest().item());

    pipeline
        .apply(Create.of(items))
        // generate identical duplicates
        .apply(ParDo.of(new AddDuplicatesDoFn(3, false)))
        .apply(DynamoDBIO.<Item>write().withWriteRequestMapperFn(putRequestMapper));

    pipeline.run().waitUntilFinish();

    List<List<Item>> requests = captureRequests.get();
    for (List<Item> reqItems : requests) {
      assertThat(reqItems).doesNotHaveDuplicates(); // each request is free of duplicates
    }

    assertThat(requests.stream().flatMap(List::stream)).containsAll(items);
  }

  @Test
  public void testWritePutItemsWithDuplicatesByKey() {
    ImmutableList<String> keys = ImmutableList.of("id");
    List<Item> items = Item.range(0, 100);

    Supplier<List<List<Item>>> captureRequests =
        captureBatchWriteRequests(client, req -> req.putRequest().item());

    pipeline
        .apply(Create.of(items))
        // decorate duplicates so they are different
        .apply(ParDo.of(new AddDuplicatesDoFn(3, true)))
        .apply(
            DynamoDBIO.<Item>write()
                .withWriteRequestMapperFn(putRequestMapper)
                .withDeduplicateKeys(keys));

    pipeline.run().waitUntilFinish();

    List<List<Item>> requests = captureRequests.get();
    for (List<Item> reqItems : requests) {
      List<Item> keysOnly =
          reqItems.stream()
              .map(item -> new Item(filterKeys(item.entries, keys::contains)))
              .collect(toList());
      assertThat(keysOnly).doesNotHaveDuplicates(); // each request is free of duplicates
    }

    assertThat(requests.stream().flatMap(List::stream)).containsAll(items);
  }

  @Test
  public void testWriteDeleteItems() {
    List<Item> items = Item.range(0, 100);

    Supplier<List<Item>> captureDeletes =
        captureBatchWrites(client, req -> req.deleteRequest().key());

    Write<Item> write = DynamoDBIO.<Item>write().withWriteRequestMapperFn(deleteRequestMapper);
    PCollection<Void> output = pipeline.apply(Create.of(items)).apply(write);

    PAssert.that(output).empty();
    pipeline.run().waitUntilFinish();

    assertThat(captureDeletes.get()).hasSize(100);
    assertThat(captureDeletes.get()).containsExactlyInAnyOrderElementsOf(items);
  }

  @Test
  public void testWriteDeleteItemsWithDuplicates() {
    List<Item> items = Item.range(0, 100);

    Supplier<List<List<Item>>> captureRequests =
        captureBatchWriteRequests(client, req -> req.deleteRequest().key());

    pipeline
        .apply(Create.of(items))
        // generate identical duplicates
        .apply(ParDo.of(new AddDuplicatesDoFn(3, false)))
        .apply(DynamoDBIO.<Item>write().withWriteRequestMapperFn(deleteRequestMapper));

    pipeline.run().waitUntilFinish();

    List<List<Item>> requests = captureRequests.get();
    for (List<Item> reqItems : requests) {
      assertThat(reqItems).doesNotHaveDuplicates(); // each request is free of duplicates
    }

    assertThat(requests.stream().flatMap(List::stream)).containsAll(items);
  }

  @Test
  public void testWritePutItemsWithPartialSuccess() {
    List<WriteRequest> writes = putRequests(Item.range(0, 10));

    when(client.batchWriteItem(any(BatchWriteItemRequest.class)))
        .thenReturn(partialWriteSuccess(writes.subList(4, 10)))
        .thenReturn(partialWriteSuccess(writes.subList(8, 10)))
        .thenReturn(BatchWriteItemResponse.builder().build());

    pipeline
        .apply(Create.of(10)) // number if items to produce
        .apply(ParDo.of(new GenerateItems())) // 10 items in one bundle
        .apply("write", DynamoDBIO.<Item>write().withWriteRequestMapperFn(putRequestMapper));

    PipelineResult result = pipeline.run();
    result.waitUntilFinish();

    verify(client, times(3)).batchWriteItem(any(BatchWriteItemRequest.class));

    InOrder ordered = inOrder(client);
    ordered.verify(client).batchWriteItem(argThat(matchWritesUnordered(writes)));
    ordered.verify(client).batchWriteItem(argThat(matchWritesUnordered(writes.subList(4, 10))));
    ordered.verify(client).batchWriteItem(argThat(matchWritesUnordered(writes.subList(8, 10))));
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
      return new Item(ImmutableMap.copyOf(transformValues(attributes, a -> a.s())));
    }

    static List<Item> range(int startInclusive, int endExclusive) {
      return IntStream.range(startInclusive, endExclusive).mapToObj(Item::of).collect(toList());
    }

    Item withEntry(String key, String value) {
      return new Item(
          ImmutableMap.<String, String>builder().putAll(entries).put(key, value).build());
    }

    Map<String, AttributeValue> attributeMap() {
      return new HashMap<>(transformValues(entries, v -> AttributeValue.builder().s(v).build()));
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

  private Supplier<List<List<Item>>> captureBatchWriteRequests(
      DynamoDbClient mock, Function<WriteRequest, Map<String, AttributeValue>> extractor) {
    ArgumentCaptor<BatchWriteItemRequest> reqCaptor =
        ArgumentCaptor.forClass(BatchWriteItemRequest.class);
    when(mock.batchWriteItem(reqCaptor.capture()))
        .thenReturn(BatchWriteItemResponse.builder().build());

    return () ->
        reqCaptor.getAllValues().stream()
            .flatMap(req -> req.requestItems().values().stream())
            .map(writes -> writes.stream().map(extractor).map(Item::of).collect(toList()))
            .collect(toList());
  }

  private Supplier<List<Item>> captureBatchWrites(
      DynamoDbClient mock, Function<WriteRequest, Map<String, AttributeValue>> extractor) {
    Supplier<List<List<Item>>> requests = captureBatchWriteRequests(mock, extractor);
    return () -> requests.get().stream().flatMap(reqs -> reqs.stream()).collect(toList());
  }

  private static ArgumentMatcher<BatchWriteItemRequest> matchWritesUnordered(
      List<WriteRequest> writes) {
    return (BatchWriteItemRequest req) ->
        req != null
            && req.requestItems().get(tableName).size() == writes.size()
            && req.requestItems().get(tableName).containsAll(writes);
  }

  private static BatchWriteItemResponse partialWriteSuccess(List<WriteRequest> unprocessed) {
    return BatchWriteItemResponse.builder()
        .unprocessedItems(ImmutableMap.of(tableName, unprocessed))
        .build();
  }

  private static List<WriteRequest> putRequests(List<Item> items) {
    return items.stream().map(putRequest).collect(toList());
  }

  private static Function<Item, WriteRequest> putRequest =
      item ->
          WriteRequest.builder()
              .putRequest(PutRequest.builder().item(item.attributeMap()).build())
              .build();

  private static Function<Item, WriteRequest> deleteRequest =
      key ->
          WriteRequest.builder()
              .deleteRequest(DeleteRequest.builder().key(key.attributeMap()).build())
              .build();

  private static SerializableFunction<Item, KV<String, WriteRequest>> putRequestMapper =
      item -> KV.of(tableName, putRequest.apply(item));

  private static SerializableFunction<Item, KV<String, WriteRequest>> deleteRequestMapper =
      key -> KV.of(tableName, deleteRequest.apply(key));

  private static class GenerateItems extends DoFn<Integer, Item> {
    @ProcessElement
    public void processElement(ProcessContext ctx) {
      range(0, ctx.element()).forEach(i -> ctx.output(Item.of(i)));
    }
  }

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
