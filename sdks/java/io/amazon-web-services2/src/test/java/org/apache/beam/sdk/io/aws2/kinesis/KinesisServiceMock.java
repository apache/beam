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
package org.apache.beam.sdk.io.aws2.kinesis;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists.newArrayList;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.joda.time.DateTime;

/** Simple mock implementation of Kinesis service for testing, singletone. */
public class KinesisServiceMock {
  private static KinesisServiceMock instance;

  // Mock stream where client is supposed to write
  private String existedStream;

  private AtomicInteger addedRecords = new AtomicInteger(0);
  private AtomicInteger seqNumber = new AtomicInteger(0);
  private List<List<AmazonKinesisMock.TestData>> shardedData;

  private KinesisServiceMock() {}

  public static synchronized KinesisServiceMock getInstance() {
    if (instance == null) {
      instance = new KinesisServiceMock();
    }
    return instance;
  }

  public synchronized void init(String stream, int shardsNum) {
    existedStream = stream;
    addedRecords.set(0);
    seqNumber.set(0);
    shardedData = newArrayList();
    for (int i = 0; i < shardsNum; i++) {
      List<AmazonKinesisMock.TestData> shardData = newArrayList();
      shardedData.add(shardData);
    }
  }

  public AtomicInteger getAddedRecords() {
    return addedRecords;
  }

  public String getExistedStream() {
    return existedStream;
  }

  public synchronized void addShardedData(ByteBuffer data, DateTime arrival) {
    String dataString = StandardCharsets.UTF_8.decode(data).toString();

    List<AmazonKinesisMock.TestData> shardData = shardedData.get(0);

    seqNumber.incrementAndGet();
    AmazonKinesisMock.TestData testData =
        new AmazonKinesisMock.TestData(
            dataString, arrival.toInstant(), Integer.toString(seqNumber.get()));
    shardData.add(testData);

    addedRecords.incrementAndGet();
  }

  public synchronized List<List<AmazonKinesisMock.TestData>> getShardedData() {
    return shardedData;
  }
}
