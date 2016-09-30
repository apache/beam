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
package org.apache.beam.sdk.testing;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableRow;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.util.Transport;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A matcher to verify data in BigQuery by processing given query
 * and comparing with content's checksum.
 *
 * <p>Example:
 * <pre>{@code [
 *   assertThat(job, new BigqueryMatcher(appName, projectId, queryString, expectedChecksum));
 * ]}</pre>
 */
@NotThreadSafe
public class BigqueryMatcher extends TypeSafeMatcher<PipelineResult>
    implements SerializableMatcher<PipelineResult> {
  private static final Logger LOG = LoggerFactory.getLogger(BigqueryMatcher.class);

  private static final int MAX_QUERY_RETRY = 4;

  private final String applicationName;
  private final String projectId;
  private final String query;
  private final String expectedChecksum;
  private String actualChecksum;
  private transient QueryResponse response;

  public BigqueryMatcher(
      String applicationName, String projectId, String query, String expectedChecksum) {
    validateArgument("applicationName", applicationName);
    validateArgument("projectId", projectId);
    validateArgument("query", query);
    validateArgument("expectedChecksum", expectedChecksum);

    this.applicationName = applicationName;
    this.projectId = projectId;
    this.query = query;
    this.expectedChecksum = expectedChecksum;
  }

  @Override
  protected boolean matchesSafely(PipelineResult pipelineResult) {
    LOG.info("Verifying Bigquery data");
    Bigquery bigqueryClient = newBigqueryClient(applicationName);

    LOG.info("Executing query: {}", query);
    response = queryWithRetries(bigqueryClient);

    if (response == null || response.getRows() == null || response.getRows().isEmpty()) {
      return false;
    }
    actualChecksum = generateHash(response.getRows());
    LOG.info("Generated a SHA1 checksum based on queried data: {}", actualChecksum);
    return expectedChecksum.equals(actualChecksum);
  }

  @VisibleForTesting
  Bigquery newBigqueryClient(String applicationName) {
    HttpTransport transport = Transport.getTransport();
    JsonFactory jsonFactory = Transport.getJsonFactory();
    Credential credential = getDefaultCredential(transport, jsonFactory);

    return new Bigquery.Builder(transport, jsonFactory, credential)
        .setApplicationName(applicationName)
        .build();
  }

  private void validateArgument(String name, String value) {
    checkArgument(
        !Strings.isNullOrEmpty(value), "Expected valid %s, but was %s", name, value);
  }

  private Credential getDefaultCredential(HttpTransport transport, JsonFactory jsonFactory) {
    GoogleCredential credential;
    try {
      credential = GoogleCredential.getApplicationDefault(transport, jsonFactory);
    } catch (IOException e) {
      throw new RuntimeException("Failed to get application default credential.", e);
    }

    if (credential.createScopedRequired()) {
      Collection<String> bigqueryScope =
          Lists.newArrayList(BigqueryScopes.CLOUD_PLATFORM_READ_ONLY);
      credential = credential.createScoped(bigqueryScope);
    }
    return credential;
  }

  private String generateHash(List<TableRow> rows) {
    List<HashCode> rowHashes = Lists.newArrayList();
    for (TableRow row : rows) {
      List<HashCode> cellHashes = Lists.newArrayList();
      for (TableCell cell : row.getF()) {
        cellHashes.add(Hashing.sha1().hashString(cell.toString(), StandardCharsets.UTF_8));
      }
      rowHashes.add(Hashing.combineUnordered(cellHashes));
    }
    return Hashing.combineUnordered(rowHashes).toString();
  }

  private QueryResponse queryWithRetries(Bigquery bigqueryClient) {
    QueryRequest queryContent = new QueryRequest();
    queryContent.setQuery(query);
    for (int i = 0; i < MAX_QUERY_RETRY; i++) {
      try {
        return bigqueryClient.jobs().query(projectId, queryContent).execute();
      } catch (IOException e) {
        LOG.warn("There were problems getting BigQuery response: {}", e.getMessage());
        LOG.debug("Exception information: {}", e);
        try {
          Thread.sleep(1000L);
        } catch (InterruptedException exp) {
          throw new RuntimeException(exp);
        }
      }
    }
    return null;
  }

  @Override
  public void describeTo(Description description) {
    description
        .appendText("Expected checksum is (")
        .appendText(expectedChecksum)
        .appendText(")");
  }

  @Override
  public void describeMismatchSafely(PipelineResult pResult, Description description) {
    String info;
    if (response == null) {
      info = "BigQuery response is null";
    } else if (response.getRows() == null) {
      info = "rows that is from BigQuery response is null";
    } else if (response.getRows().isEmpty()) {
      info = "rows that is from BigQuery response is empty";
    } else {
      info = String.format("was (%s).%n"
          + "\tTotal number of rows are: %d.%n"
          + "\tQueried data details:%s",
          actualChecksum, response.getTotalRows(), printRows(4));
    }
    description.appendText(info);
  }

  private String printRows(int rowNumber) {
    StringBuilder samples = new StringBuilder();
    List<TableRow> rows = response.getRows();
    for (int i = 0; i < rowNumber && i < rows.size(); i++) {
      samples.append("\n\t\t");
      for (TableCell field : rows.get(i).getF()) {
        samples.append(String.format("%-10s", field.getV()));
      }
    }
    if (rows.size() > rowNumber) {
      samples.append("\n\t\t....");
    }
    return samples.toString();
  }
}
