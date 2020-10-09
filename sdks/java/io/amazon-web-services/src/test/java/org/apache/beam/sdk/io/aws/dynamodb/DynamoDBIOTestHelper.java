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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Rule;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

/** A utility to generate test table and data for {@link DynamoDBIOTest}. */
class DynamoDBIOTestHelper implements Serializable {

  @Rule
  private static LocalStackContainer localStackContainer =
      new LocalStackContainer(DockerImageName.parse("localstack/localstack:latest"))
          .withServices(LocalStackContainer.Service.DYNAMODB);

  private static AmazonDynamoDB dynamoDBClient;

  static final String ATTR_NAME_1 = "hashKey1";
  static final String ATTR_NAME_2 = "rangeKey2";

  static void startServerClient() {
    localStackContainer.start();

    if (dynamoDBClient == null) {
      dynamoDBClient =
          AmazonDynamoDBClientBuilder.standard()
              .withEndpointConfiguration(
                  localStackContainer.getEndpointConfiguration(
                      LocalStackContainer.Service.DYNAMODB))
              .withCredentials(localStackContainer.getDefaultCredentialsProvider())
              .build();
    }
  }

  static void stopServerClient(String tableName) {
    if (dynamoDBClient != null) {
      dynamoDBClient.deleteTable(tableName);
      dynamoDBClient.shutdown();
    }
    localStackContainer.stop();
  }

  static AmazonDynamoDB getDynamoDBClient() {
    // Note: each test case got to have their own dynamo client obj, can't be shared
    // Otherwise will run into connection pool issue
    return AmazonDynamoDBClientBuilder.standard()
        .withEndpointConfiguration(
            localStackContainer.getEndpointConfiguration(LocalStackContainer.Service.DYNAMODB))
        .withCredentials(localStackContainer.getDefaultCredentialsProvider())
        .build();
  }

  static List<Map<String, AttributeValue>> generateTestData(String tableName, int numOfItems) {
    BatchWriteItemRequest batchWriteItemRequest =
        generateBatchWriteItemRequest(tableName, numOfItems);

    dynamoDBClient.batchWriteItem(batchWriteItemRequest);
    ScanResult scanResult = dynamoDBClient.scan(new ScanRequest().withTableName(tableName));

    List<Map<String, AttributeValue>> items = scanResult.getItems();
    Assert.assertEquals(numOfItems, items.size());
    return items;
  }

  static BatchWriteItemRequest generateBatchWriteItemRequest(String tableName, int numOfItems) {
    BatchWriteItemRequest batchWriteItemRequest = new BatchWriteItemRequest();
    batchWriteItemRequest.addRequestItemsEntry(tableName, generateWriteRequests(numOfItems));
    return batchWriteItemRequest;
  }

  static List<WriteRequest> generateWriteRequests(int numOfItem) {
    List<WriteRequest> writeRequests = new ArrayList<>();
    for (int i = 1; i <= numOfItem; i++) {
      WriteRequest writeRequest = new WriteRequest();
      writeRequest.setPutRequest(generatePutRequest("hashKeyDataStr_" + i, "1000" + i));
      writeRequests.add(writeRequest);
    }
    return writeRequests;
  }

  private static PutRequest generatePutRequest(String hashKeyData, String rangeKeyData) {
    PutRequest putRequest = new PutRequest();
    putRequest.addItemEntry(ATTR_NAME_1, new AttributeValue(hashKeyData));
    putRequest.addItemEntry(ATTR_NAME_2, new AttributeValue().withN(rangeKeyData));
    return putRequest;
  }

  static void createTestTable(String tableName) {
    CreateTableResult res = createDynamoTable(tableName);

    TableDescription tableDesc = res.getTableDescription();

    Assert.assertEquals(tableName, tableDesc.getTableName());
    Assert.assertTrue(tableDesc.getKeySchema().toString().contains(ATTR_NAME_1));
    Assert.assertTrue(tableDesc.getKeySchema().toString().contains(ATTR_NAME_2));

    Assert.assertEquals(
        tableDesc.getProvisionedThroughput().getReadCapacityUnits(), Long.valueOf(1000));
    Assert.assertEquals(
        tableDesc.getProvisionedThroughput().getWriteCapacityUnits(), Long.valueOf(1000));
    Assert.assertEquals("ACTIVE", tableDesc.getTableStatus());
    Assert.assertEquals(
        "arn:aws:dynamodb:us-east-1:000000000000:table/" + tableName, tableDesc.getTableArn());

    ListTablesResult tables = dynamoDBClient.listTables();
    Assert.assertEquals(1, tables.getTableNames().size());
  }

  private static CreateTableResult createDynamoTable(String tableName) {

    ImmutableList<AttributeDefinition> attributeDefinitions =
        ImmutableList.of(
            new AttributeDefinition(ATTR_NAME_1, ScalarAttributeType.S),
            new AttributeDefinition(ATTR_NAME_2, ScalarAttributeType.N));

    ImmutableList<KeySchemaElement> ks =
        ImmutableList.of(
            new KeySchemaElement(ATTR_NAME_1, KeyType.HASH),
            new KeySchemaElement(ATTR_NAME_2, KeyType.RANGE));

    ProvisionedThroughput provisionedthroughput = new ProvisionedThroughput(1000L, 1000L);
    CreateTableRequest request =
        new CreateTableRequest()
            .withTableName(tableName)
            .withAttributeDefinitions(attributeDefinitions)
            .withKeySchema(ks)
            .withProvisionedThroughput(provisionedthroughput);

    return dynamoDBClient.createTable(request);
  }
}
