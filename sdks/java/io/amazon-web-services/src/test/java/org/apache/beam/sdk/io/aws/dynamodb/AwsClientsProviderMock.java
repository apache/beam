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

import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import org.mockito.Mockito;

/** Mocking AwsClientProvider. */
public class AwsClientsProviderMock implements AwsClientsProvider {

  private static AwsClientsProviderMock instance = new AwsClientsProviderMock();
  private static AmazonDynamoDB db;

  private AwsClientsProviderMock() {}

  public static AwsClientsProviderMock of(AmazonDynamoDB dynamoDB) {
    db = dynamoDB;
    return instance;
  }

  @Override
  public AmazonCloudWatch getCloudWatchClient() {
    return Mockito.mock(AmazonCloudWatch.class);
  }

  @Override
  public AmazonDynamoDB createDynamoDB() {
    return db;
  }
}
