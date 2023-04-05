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

import static java.util.Collections.synchronizedMap;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import java.util.HashMap;
import java.util.Map;

/** Client provider supporting unserializable clients such as mock instances for unit tests. */
class StaticAwsClientsProvider implements AwsClientsProvider {
  private static final Map<Integer, AmazonDynamoDB> clients = synchronizedMap(new HashMap<>());

  private final int id;
  private final transient boolean cleanup;

  private StaticAwsClientsProvider(AmazonDynamoDB client) {
    this.id = System.identityHashCode(client);
    this.cleanup = true;
  }

  static AwsClientsProvider of(AmazonDynamoDB client) {
    StaticAwsClientsProvider provider = new StaticAwsClientsProvider(client);
    clients.put(provider.id, client);
    return provider;
  }

  @Override
  public AmazonDynamoDB createDynamoDB() {
    return clients.get(id);
  }

  @Override
  protected void finalize() {
    if (cleanup) {
      clients.remove(id);
    }
  }
}
