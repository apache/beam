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

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables.concat;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables.cycle;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables.limit;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists.newArrayList;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists.transform;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Streams.stream;
import static org.mockito.ArgumentMatchers.any;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.beam.sdk.io.common.TestRow;
import org.mockito.ArgumentMatcher;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;

public abstract class PutRecordsHelpers {
  protected static final String ERROR_CODE = "ProvisionedThroughputExceededException";

  PutRecordsResponse successResponse = PutRecordsResponse.builder().build();

  protected PutRecordsRequest anyRequest() {
    return any();
  }

  protected ArgumentMatcher<PutRecordsRequest> containsAll(Iterable<TestRow> rows) {
    return req -> req.records().containsAll(fromTestRows(rows));
  }

  protected ArgumentMatcher<PutRecordsRequest> hasSize(int size) {
    return req -> req.records().size() == size;
  }

  protected ArgumentMatcher<PutRecordsRequest> hasRecordSize(int bytes) {
    return req -> req.records().stream().allMatch(e -> bytesOf(e.data()).length == bytes);
  }

  protected ArgumentMatcher<PutRecordsRequest> hasPartitions(String... partitions) {
    return req ->
        hasSize(partitions.length).matches(req)
            && transform(req.records(), r -> r.partitionKey()).containsAll(asList(partitions));
  }

  protected ArgumentMatcher<PutRecordsRequest> hasExplicitPartitions(String... partitions) {
    return req ->
        hasSize(partitions.length).matches(req)
            && transform(req.records(), r -> r.explicitHashKey()).containsAll(asList(partitions));
  }

  protected PutRecordsResponse partialSuccessResponse(int successes, int errors) {
    PutRecordsResultEntry e = PutRecordsResultEntry.builder().errorCode(ERROR_CODE).build();
    PutRecordsResultEntry s = PutRecordsResultEntry.builder().build();
    return PutRecordsResponse.builder()
        .records(newArrayList(concat(limit(cycle(s), successes), limit(cycle(e), errors))))
        .build();
  }

  protected TestRow toTestRow(PutRecordsRequestEntry record) {
    int id = ByteBuffer.wrap(bytesOf(record.data())).getInt();
    return TestRow.create(id, record.partitionKey());
  }

  protected List<PutRecordsRequestEntry> fromTestRows(Iterable<TestRow> rows) {
    return stream(rows).map(this::fromTestRow).collect(toList());
  }

  protected PutRecordsRequestEntry fromTestRow(TestRow row) {
    return PutRecordsRequestEntry.builder()
        .partitionKey(row.name())
        .data(SdkBytes.fromByteArray(bytesOf(row.id())))
        .build();
  }

  private static byte[] bytesOf(SdkBytes data) {
    return data.asByteArrayUnsafe();
  }

  protected static byte[] bytesOf(int n) {
    return ByteBuffer.allocate(4).putInt(n).array();
  }
}
