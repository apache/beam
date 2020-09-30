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

import java.io.Serializable;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Rule;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

/** A utility to generate test table and data for {@link DynamoDBIOTest}. */
class DynamoDBIOTestHelper implements Serializable {

  @Rule
  public static GenericContainer dynamoContainer =
      new GenericContainer<>(DockerImageName.parse("amazon/dynamodb-local:latest"))
          .withExposedPorts(8000);

  private static DynamoDbClient dynamoDBClient;

  static final String ATTR_NAME_1 = "hashKey1";
  static final String ATTR_NAME_2 = "rangeKey2";

  static void startServerClient() {
    dynamoContainer.start();

    if (dynamoDBClient == null) {
      dynamoDBClient = getDynamoDBClient();
    }
  }

  static void stopServerClient(String tableName) {
    if (dynamoDBClient != null) {
      dynamoDBClient.close();
    }
    dynamoContainer.stop();
  }

  static DynamoDbClient getDynamoDBClient() {
    // Note: each test case got to have their own dynamodb client obj, can't be shared
    // Otherwise will run into connection pool issue
    return DynamoDbClient.builder()
        .endpointOverride(URI.create(getContainerEndpoint()))
        .region(Region.US_EAST_1)
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create("accessKey", "secretKey")))
        .build();
  }

  static List<Map<String, AttributeValue>> generateTestData(String tableName, int numOfItems) {
    BatchWriteItemRequest batchWriteItemRequest =
        generateBatchWriteItemRequest(tableName, numOfItems);

    dynamoDBClient.batchWriteItem(batchWriteItemRequest);
    ScanResponse scanResult =
        dynamoDBClient.scan(ScanRequest.builder().tableName(tableName).build());

    List<Map<String, AttributeValue>> items = scanResult.items();
    Assert.assertEquals(numOfItems, items.size());
    return items;
  }

  static BatchWriteItemRequest generateBatchWriteItemRequest(String tableName, int numOfItems) {
    BatchWriteItemRequest batchWriteItemRequest =
        BatchWriteItemRequest.builder()
            .requestItems(ImmutableMap.of(tableName, generateWriteRequests(numOfItems)))
            .build();
    return batchWriteItemRequest;
  }

  static List<WriteRequest> generateWriteRequests(int numOfItem) {
    List<WriteRequest> writeRequests = new ArrayList<>();
    for (int i = 1; i <= numOfItem; i++) {
      WriteRequest writeRequest =
          WriteRequest.builder()
              .putRequest(generatePutRequest("hashKeyDataStr_" + i, "1000" + i))
              .build();
      writeRequests.add(writeRequest);
    }
    return writeRequests;
  }

  private static PutRequest generatePutRequest(String hashKeyData, String rangeKeyData) {
    ImmutableMap<String, AttributeValue> attrValueMap =
        ImmutableMap.of(
            ATTR_NAME_1, AttributeValue.builder().s(hashKeyData).build(),
            ATTR_NAME_2, AttributeValue.builder().n(rangeKeyData).build());

    PutRequest.Builder putRequestBuilder = PutRequest.builder();
    putRequestBuilder.item(attrValueMap);
    return putRequestBuilder.build();
  }

  static List<Map<String, AttributeValue>> readDataFromTable(String tableName) {
    ScanRequest scanRequest = ScanRequest.builder().tableName(tableName).build();
    ScanResponse scanResponse = dynamoDBClient.scan(scanRequest);
    return scanResponse.items();
  }

  static void deleteTestTable(String tableName) {
    DeleteTableRequest request = DeleteTableRequest.builder().tableName(tableName).build();
    dynamoDBClient.deleteTable(request);
  }

  static void createTestTable(String tableName) {
    CreateTableResponse res = createDynamoTable(tableName);

    TableDescription tableDesc = res.tableDescription();

    Assert.assertEquals(tableName, tableDesc.tableName());
    Assert.assertTrue(tableDesc.keySchema().toString().contains(ATTR_NAME_1));
    Assert.assertTrue(tableDesc.keySchema().toString().contains(ATTR_NAME_2));

    Assert.assertEquals(tableDesc.provisionedThroughput().readCapacityUnits(), Long.valueOf(1000));
    Assert.assertEquals(tableDesc.provisionedThroughput().writeCapacityUnits(), Long.valueOf(1000));
    Assert.assertEquals(TableStatus.ACTIVE, tableDesc.tableStatus());
    Assert.assertEquals(
        "arn:aws:dynamodb:ddblocal:000000000000:table/" + tableName, tableDesc.tableArn());

    ListTablesResponse tables = dynamoDBClient.listTables();
    Assert.assertEquals(1, tables.tableNames().size());
  }

  private static CreateTableResponse createDynamoTable(String tableName) {

    ImmutableList<AttributeDefinition> attributeDefinitions =
        ImmutableList.of(
            AttributeDefinition.builder()
                .attributeName(ATTR_NAME_1)
                .attributeType(ScalarAttributeType.S)
                .build(),
            AttributeDefinition.builder()
                .attributeName(ATTR_NAME_2)
                .attributeType(ScalarAttributeType.N)
                .build());

    ImmutableList<KeySchemaElement> ks =
        ImmutableList.of(
            KeySchemaElement.builder().attributeName(ATTR_NAME_1).keyType(KeyType.HASH).build(),
            KeySchemaElement.builder().attributeName(ATTR_NAME_2).keyType(KeyType.RANGE).build());

    ProvisionedThroughput provisionedthroughput =
        ProvisionedThroughput.builder().readCapacityUnits(1000L).writeCapacityUnits(1000L).build();
    CreateTableRequest request =
        CreateTableRequest.builder()
            .tableName(tableName)
            .attributeDefinitions(attributeDefinitions)
            .keySchema(ks)
            .provisionedThroughput(provisionedthroughput)
            .build();

    return dynamoDBClient.createTable(request);
  }

  // This helper function is copied from localstack
  private static String getContainerEndpoint() {
    final String address = dynamoContainer.getContainerIpAddress();
    String ipAddress = address;
    try {
      ipAddress = InetAddress.getByName(address).getHostAddress();
    } catch (UnknownHostException ignored) {
    }
    ipAddress = ipAddress + ".nip.io";
    while (true) {
      try {
        //noinspection ResultOfMethodCallIgnored
        InetAddress.getAllByName(ipAddress);
        break;
      } catch (UnknownHostException ignored) {
      }
    }
    return "http://" + ipAddress + ":" + dynamoContainer.getFirstMappedPort();
  }
}
