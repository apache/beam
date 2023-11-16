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
package org.apache.beam.sdk.io.aws2.dynamodb.testing;

import static org.apache.beam.sdk.io.common.TestRow.getExpectedHashForRowCount;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.DYNAMODB;

import java.util.Map;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.aws2.ITEnvironment;
import org.apache.beam.sdk.io.aws2.dynamodb.DynamoDBIO;
import org.apache.beam.sdk.io.common.HashingFn;
import org.apache.beam.sdk.io.common.TestRow;
import org.apache.beam.sdk.io.common.TestRow.DeterministicallyConstructTestRowFn;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

@RunWith(JUnit4.class)
/**
 * Integration test to write and read from DynamoDB.
 *
 * <p>By default this runs against Localstack, but you can use {@link DynamoDBIOIT.ITOptions} to
 * configure tests to run against AWS DynamoDB.
 *
 * <pre>{@code
 * ./gradlew :sdks:java:io:amazon-web-services2:integrationTest \
 *   --info \
 *   --tests "org.apache.beam.sdk.io.aws2.dynamodb.testing.DynamoDBIOIT" \
 *   -DintegrationTestPipelineOptions='["--awsRegion=eu-central-1","--useLocalstack=false"]'
 * }</pre>
 */
public class DynamoDBIOIT {
  public interface ITOptions extends ITEnvironment.ITOptions {
    @Description("DynamoDB table name")
    @Default.String("beam-dynamodbio-it")
    String getDynamoDBTable();

    void setDynamoDBTable(String value);

    @Description("DynamoDB total segments")
    @Default.Integer(2)
    Integer getDynamoDBSegments();

    void setDynamoDBSegments(Integer segments);

    @Description("Create DynamoDB table. Enabled when using localstack")
    @Default.Boolean(false)
    Boolean getCreateTable();

    void setCreateTable(Boolean createTable);
  }

  private static final String COL_ID = "id";
  private static final String COL_NAME = "name";

  @ClassRule
  public static ITEnvironment<ITOptions> env =
      new ITEnvironment<>(DYNAMODB, ITOptions.class, "DYNAMODB_ERROR_PROBABILITY=0.1");

  @Rule public TestPipeline pipelineWrite = env.createTestPipeline();
  @Rule public TestPipeline pipelineRead = env.createTestPipeline();
  @Rule public ExternalResource dbTable = CreateDbTable.optionally(env.options());

  /** Test which write and then read data from DynamoDB. */
  @Test
  public void testWriteThenRead() {
    runWrite();
    runRead();
  }

  /** Write test dataset to DynamoDB. */
  private void runWrite() {
    int rows = env.options().getNumberOfRows();
    pipelineWrite
        .apply("Generate Sequence", GenerateSequence.from(0).to(rows))
        .apply("Prepare TestRows", ParDo.of(new DeterministicallyConstructTestRowFn()))
        .apply(
            "Write to DynamoDB",
            DynamoDBIO.<TestRow>write().withWriteRequestMapperFn(row -> buildWriteRequest(row)));
    pipelineWrite.run().waitUntilFinish();
  }

  /** Read test dataset from DynamoDB. */
  private void runRead() {
    int rows = env.options().getNumberOfRows();
    PCollection<Map<String, AttributeValue>> records =
        pipelineRead
            .apply(
                "Read from DynamoDB",
                DynamoDBIO.read().withScanRequestFn(in -> buildScanRequest()).items())
            .apply("Flatten result", Flatten.iterables());

    PAssert.thatSingleton(records.apply("Count All", Count.globally())).isEqualTo((long) rows);

    PCollection<String> consolidatedHashcode =
        records
            .apply(MapElements.into(strings()).via(record -> record.get(COL_NAME).s()))
            .apply("Hash records", Combine.globally(new HashingFn()).withoutDefaults());

    PAssert.that(consolidatedHashcode).containsInAnyOrder(getExpectedHashForRowCount(rows));

    pipelineRead.run().waitUntilFinish();
  }

  private static ScanRequest buildScanRequest() {
    return ScanRequest.builder()
        .tableName(env.options().getDynamoDBTable())
        .totalSegments(env.options().getDynamoDBSegments())
        .build();
  }

  private static KV<String, WriteRequest> buildWriteRequest(TestRow row) {
    AttributeValue id = AttributeValue.builder().n(row.id().toString()).build();
    AttributeValue name = AttributeValue.builder().s(row.name()).build();
    PutRequest req = PutRequest.builder().item(ImmutableMap.of(COL_ID, id, COL_NAME, name)).build();
    return KV.of(env.options().getDynamoDBTable(), WriteRequest.builder().putRequest(req).build());
  }

  static class CreateDbTable extends ExternalResource {
    static ExternalResource optionally(ITOptions opts) {
      boolean create = opts.getCreateTable() || opts.getUseLocalstack();
      return create ? new CreateDbTable() : new ExternalResource() {};
    }

    private final String name = env.options().getDynamoDBTable();
    private final DynamoDbClient client = env.buildClient(DynamoDbClient.builder());

    @Override
    protected void before() throws Throwable {
      CreateTableRequest request =
          CreateTableRequest.builder()
              .tableName(name)
              .attributeDefinitions(
                  attribute(COL_ID, ScalarAttributeType.N),
                  attribute(COL_NAME, ScalarAttributeType.S))
              .keySchema(keyElement(COL_ID, KeyType.HASH), keyElement(COL_NAME, KeyType.RANGE))
              .provisionedThroughput(
                  ProvisionedThroughput.builder()
                      .readCapacityUnits(1000L)
                      .writeCapacityUnits(1000L)
                      .build())
              .build();
      TableStatus status = client.createTable(request).tableDescription().tableStatus();
      int attempts = 10;
      for (int i = 0; i <= attempts; ++i) {
        if (status == TableStatus.ACTIVE) {
          return;
        }
        Thread.sleep(1000L);
        status = client.describeTable(b -> b.tableName(name).build()).table().tableStatus();
      }
      throw new RuntimeException("Unable to initialize table");
    }

    @Override
    protected void after() {
      client.deleteTable(b -> b.tableName(name).build());
      client.close();
    }

    private AttributeDefinition attribute(String name, ScalarAttributeType type) {
      return AttributeDefinition.builder().attributeName(name).attributeType(type).build();
    }

    private KeySchemaElement keyElement(String name, KeyType type) {
      return KeySchemaElement.builder().attributeName(name).keyType(type).build();
    }
  }
}
