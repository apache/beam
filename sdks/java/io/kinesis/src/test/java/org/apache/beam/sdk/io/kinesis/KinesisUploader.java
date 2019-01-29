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
package org.apache.beam.sdk.io.kinesis;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Lists;

/** Sends records to Kinesis in reliable way. */
public class KinesisUploader {

  public static final int MAX_NUMBER_OF_RECORDS_IN_BATCH = 499;

  public static void uploadAll(List<String> data, KinesisTestOptions options) {
    AmazonKinesis client =
        AmazonKinesisClientBuilder.standard()
            .withCredentials(
                new AWSStaticCredentialsProvider(
                    new BasicAWSCredentials(options.getAwsAccessKey(), options.getAwsSecretKey())))
            .withRegion(options.getAwsKinesisRegion())
            .build();

    List<List<String>> partitions = Lists.partition(data, MAX_NUMBER_OF_RECORDS_IN_BATCH);
    for (List<String> partition : partitions) {
      List<PutRecordsRequestEntry> allRecords = new ArrayList<>();
      for (String row : partition) {
        allRecords.add(
            new PutRecordsRequestEntry()
                .withData(ByteBuffer.wrap(row.getBytes(StandardCharsets.UTF_8)))
                .withPartitionKey(Integer.toString(row.hashCode())));
      }

      PutRecordsResult result;
      do {
        result =
            client.putRecords(
                new PutRecordsRequest()
                    .withStreamName(options.getAwsKinesisStream())
                    .withRecords(allRecords));
        List<PutRecordsRequestEntry> failedRecords = new ArrayList<>();
        int i = 0;
        for (PutRecordsResultEntry row : result.getRecords()) {
          if (row.getErrorCode() != null) {
            failedRecords.add(allRecords.get(i));
          }
          ++i;
        }
        allRecords = failedRecords;
      } while (result.getFailedRecordCount() > 0);
    }
  }
}
