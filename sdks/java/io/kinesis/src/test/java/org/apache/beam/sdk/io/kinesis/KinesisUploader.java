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

import static com.google.common.collect.Lists.newArrayList;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.PutRecordsResultEntry;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Sends records to Kinesis in reliable way.
 */
public class KinesisUploader {

  public static final int MAX_NUMBER_OF_RECORDS_IN_BATCH = 499;

  public static void uploadAll(List<String> data, KinesisTestOptions options) {
    AmazonKinesisClient client = new AmazonKinesisClient(
        new StaticCredentialsProvider(
            new BasicAWSCredentials(
                options.getAwsAccessKey(), options.getAwsSecretKey()))
    ).withRegion(Regions.fromName(options.getAwsKinesisRegion()));

    List<List<String>> partitions = Lists.partition(data, MAX_NUMBER_OF_RECORDS_IN_BATCH);

    for (List<String> partition : partitions) {
      List<PutRecordsRequestEntry> allRecords = newArrayList();
      for (String row : partition) {
        allRecords.add(new PutRecordsRequestEntry().
            withData(ByteBuffer.wrap(row.getBytes(Charsets.UTF_8))).
            withPartitionKey(Integer.toString(row.hashCode()))

        );
      }

      PutRecordsResult result;
      do {
        result = client.putRecords(
            new PutRecordsRequest().
                withStreamName(options.getAwsKinesisStream()).
                withRecords(allRecords));
        List<PutRecordsRequestEntry> failedRecords = newArrayList();
        int i = 0;
        for (PutRecordsResultEntry row : result.getRecords()) {
          if (row.getErrorCode() != null) {
            failedRecords.add(allRecords.get(i));
          }
          ++i;
        }
        allRecords = failedRecords;
      }

      while (result.getFailedRecordCount() > 0);
    }
  }

}
