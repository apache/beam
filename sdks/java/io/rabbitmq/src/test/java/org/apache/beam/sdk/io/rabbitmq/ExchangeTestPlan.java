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
package org.apache.beam.sdk.io.rabbitmq;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

class ExchangeTestPlan {
  private final RabbitMqIO.Read read;
  private final int numRecords;
  private final int numRecordsToPublish;

    public ExchangeTestPlan(RabbitMqIO.Read read, int maxRecordsRead) {
        this(read, maxRecordsRead, maxRecordsRead);
    }

  public ExchangeTestPlan(RabbitMqIO.Read read, int maxRecordsRead, int numRecordsToPublish) {
    this.read = read;
    this.numRecords = maxRecordsRead;
    this.numRecordsToPublish = numRecordsToPublish;
  }

  public RabbitMqIO.Read getRead() {
    return this.read;
  }

  public int getNumRecords() {
    return numRecords;
  }

  public int getNumRecordsToPublish() {
        return numRecordsToPublish;
  }


  public Supplier<String> publishRoutingKeyGen() {
    return () -> "someRoutingKey";
  }

  public List<String> expectedResults() {
    return RabbitMqTestUtils.generateRecords(numRecordsToPublish).stream()
        .map(RabbitMqTestUtils::recordToString)
        .collect(Collectors.toList());
  }
}
