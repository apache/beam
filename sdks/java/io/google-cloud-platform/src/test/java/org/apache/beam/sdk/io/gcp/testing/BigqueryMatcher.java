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
package org.apache.beam.sdk.io.gcp.testing;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testing.SerializableMatcher;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A matcher to verify data in BigQuery by processing given query and comparing with content's
 * checksum.
 *
 * <p>Example:
 *
 * <pre>{@code [
 *   assertThat(job, new BigqueryMatcher(appName, projectId, queryString, expectedChecksum));
 * ]}</pre>
 */
@NotThreadSafe
public class BigqueryMatcher extends TypeSafeMatcher<PipelineResult>
    implements SerializableMatcher<PipelineResult> {
  private static final Logger LOG = LoggerFactory.getLogger(BigqueryMatcher.class);

  // The total number of rows in query response to be formatted for debugging purpose
  private static final int TOTAL_FORMATTED_ROWS = 20;

  private final String projectId;
  private final String query;
  private final String expectedChecksum;
  private String actualChecksum;
  private transient QueryResponse response;
  private BigqueryClient bigqueryClient;

  public BigqueryMatcher(
      String applicationName, String projectId, String query, String expectedChecksum) {
    validateArgument("applicationName", applicationName);
    validateArgument("projectId", projectId);
    validateArgument("query", query);
    validateArgument("expectedChecksum", expectedChecksum);

    this.projectId = projectId;
    this.query = query;
    this.expectedChecksum = expectedChecksum;
    this.bigqueryClient = BigqueryClient.getClient(applicationName);
  }

  @Override
  protected boolean matchesSafely(PipelineResult pipelineResult) {
    LOG.info("Verifying Bigquery data");

    // execute query
    LOG.debug("Executing query: {}", query);
    try {
      response = bigqueryClient.queryWithRetries(query, this.projectId);
    } catch (IOException | InterruptedException e) {
      if (e instanceof InterruptedIOException) {
        Thread.currentThread().interrupt();
      }
      throw new RuntimeException("Failed to fetch BigQuery data.", e);
    }

    if (!response.getJobComplete()) {
      // query job not complete, verification failed
      return false;
    } else {
      // compute checksum
      actualChecksum = generateHash(response.getRows());
      LOG.debug("Generated a SHA1 checksum based on queried data: {}", actualChecksum);

      return expectedChecksum.equals(actualChecksum);
    }
  }

  private void validateArgument(String name, String value) {
    checkArgument(!Strings.isNullOrEmpty(value), "Expected valid %s, but was %s", name, value);
  }

  private String generateHash(@Nonnull List<TableRow> rows) {
    List<HashCode> rowHashes = Lists.newArrayList();
    for (TableRow row : rows) {
      List<String> cellsInOneRow = Lists.newArrayList();
      for (TableCell cell : row.getF()) {
        cellsInOneRow.add(Objects.toString(cell.getV()));
        Collections.sort(cellsInOneRow);
      }
      rowHashes.add(Hashing.sha1().hashString(cellsInOneRow.toString(), StandardCharsets.UTF_8));
    }
    return Hashing.combineUnordered(rowHashes).toString();
  }

  @Override
  public void describeTo(Description description) {
    description.appendText("Expected checksum is (").appendText(expectedChecksum).appendText(")");
  }

  @Override
  public void describeMismatchSafely(PipelineResult pResult, Description description) {
    String info;
    if (!response.getJobComplete()) {
      // query job not complete
      info = String.format("The query job hasn't completed. Got response: %s", response);
    } else {
      // checksum mismatch
      info =
          String.format(
              "was (%s).%n" + "\tTotal number of rows are: %d.%n" + "\tQueried data details:%s",
              actualChecksum, response.getTotalRows(), formatRows(TOTAL_FORMATTED_ROWS));
    }
    description.appendText(info);
  }

  private String formatRows(int totalNumRows) {
    StringBuilder samples = new StringBuilder();
    List<TableRow> rows = response.getRows();
    for (int i = 0; i < totalNumRows && i < rows.size(); i++) {
      samples.append(String.format("%n\t\t"));
      for (TableCell field : rows.get(i).getF()) {
        samples.append(String.format("%-10s", field.getV()));
      }
    }
    if (rows.size() > totalNumRows) {
      samples.append(String.format("%n\t\t..."));
    }
    return samples.toString();
  }
}
