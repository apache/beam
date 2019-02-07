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
package org.apache.beam.sdk.io.common;

import static org.apache.beam.sdk.io.common.IOITHelper.getHashForRecordCount;

import com.google.cloud.Timestamp;
import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.testutils.NamedTestResult;
import org.apache.beam.sdk.testutils.metrics.MetricsReader;
import org.apache.beam.sdk.testutils.publishing.BigQueryResultsPublisher;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.Iterables;

/** Contains helper methods for file based IO Integration tests. */
public class FileBasedIOITHelper {

  private FileBasedIOITHelper() {}

  public static FileBasedIOTestPipelineOptions readFileBasedIOITPipelineOptions() {
    return IOITHelper.readIOTestPipelineOptions(FileBasedIOTestPipelineOptions.class);
  }

  public static String appendTimestampSuffix(String text) {
    return String.format("%s_%s", text, new Date().getTime());
  }

  public static String getExpectedHashForLineCount(int lineCount) {
    Map<Integer, String> expectedHashes =
        ImmutableMap.of(
            1000, "8604c70b43405ef9803cb49b77235ea2",
            100_000, "4c8bb3b99dcc59459b20fefba400d446",
            1_000_000, "9796db06e7a7960f974d5a91164afff1",
            100_000_000, "6ce05f456e2fdc846ded2abd0ec1de95");

    return getHashForRecordCount(lineCount, expectedHashes);
  }

  /** Constructs text lines in files used for testing. */
  public static class DeterministicallyConstructTestTextLineFn extends DoFn<Long, String> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(String.format("IO IT Test line of text. Line seed: %s", c.element()));
    }
  }

  public static void publishTestMetrics(
      PipelineResult result,
      String bigQueryTable,
      String bigQueryDataset,
      String namespace,
      String writeStartName,
      String writeEndName,
      String readStartName,
      String readEndName) {
    if (bigQueryTable != null && bigQueryDataset != null) {
      MetricsReader reader = new MetricsReader(result, namespace);
      long writeStart = reader.getStartTimeMetric(writeStartName);
      long writeEnd = reader.getEndTimeMetric(writeEndName);
      long readStart = reader.getStartTimeMetric(readStartName);
      long readEnd = reader.getEndTimeMetric(readEndName);
      double writeTime = (writeEnd - writeStart) / 1000.0;
      double readTime = (readEnd - readStart) / 1000.0;

      String uuid = UUID.randomUUID().toString();
      Timestamp timestamp = Timestamp.now();
      BigQueryResultsPublisher publisher =
          BigQueryResultsPublisher.create(bigQueryDataset, NamedTestResult.getSchema());
      publisher.publish(
          NamedTestResult.create(uuid, timestamp.toString(), "read_time", readTime), bigQueryTable);
      publisher.publish(
          NamedTestResult.create(uuid, timestamp.toString(), "write_time", writeTime),
          bigQueryTable);
    }
  }

  /** Deletes matching files using the FileSystems API. */
  public static class DeleteFileFn extends DoFn<String, Void> {

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {
      MatchResult match =
          Iterables.getOnlyElement(FileSystems.match(Collections.singletonList(c.element())));

      Set<ResourceId> resourceIds = new HashSet<>();
      for (MatchResult.Metadata metadataElem : match.metadata()) {
        resourceIds.add(metadataElem.resourceId());
      }

      FileSystems.delete(resourceIds);
    }
  }
}
