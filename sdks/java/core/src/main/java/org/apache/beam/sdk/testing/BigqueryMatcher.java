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
import com.google.api.services.bigquery.model.TableRow;
import com.google.common.base.Strings;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
 *   assertTrue(job, new BigQueryMatcher(appName, projectId, queryString, expectedChecksum));
 * ]}</pre>
 */
public class BigqueryMatcher extends TypeSafeMatcher<PipelineResult>
    implements SerializableMatcher<PipelineResult> {
  private static final Logger LOG = LoggerFactory.getLogger(BigqueryMatcher.class);

  private final String applicationName;
  private final String projectId;
  private final String query;
  private final String expectedChecksum;
  private String actualChecksum;

  public BigqueryMatcher(
      String applicationName, String projectId, String query, String expectedChecksum) {
    validateArguments(applicationName, projectId, query, expectedChecksum);

    this.applicationName = applicationName;
    this.projectId = projectId;
    this.query = query;
    this.expectedChecksum = expectedChecksum;
  }

  @Override
  protected boolean matchesSafely(PipelineResult pipelineResult) {
    LOG.info("Verifying Bigquery data");
    Bigquery bigqueryClient = newBigqueryClient(applicationName);

    QueryResponse response;
    try {
      LOG.info("Executing query: {}", query);
      QueryRequest queryContent = new QueryRequest();
      queryContent.setQuery(query);
      response = bigqueryClient.jobs().query(projectId, queryContent).execute();
    } catch (IOException e) {
      throw new RuntimeException("Failed to retrieve BigQuery data.", e);
    }

    if (response == null) {
      LOG.warn("Invalid query response.");
      return false;
    }
    actualChecksum = hashing(response.getRows());
    LOG.info("Generated a SHA1 checksum based on queried data: {}", actualChecksum);
    return expectedChecksum.equals(actualChecksum);
  }

  void validateArguments(String... args) {
    for (String argument : args) {
      checkArgument(
          !Strings.isNullOrEmpty(argument),
          "Expected valid argument, but was %s", argument);
    }
  }

  Bigquery newBigqueryClient(String applicationName) {
    HttpTransport transport = Transport.getTransport();
    JsonFactory jsonFactory = Transport.getJsonFactory();
    Credential credential = getDefaultCredential(transport, jsonFactory);

    return new Bigquery.Builder(transport, jsonFactory, credential)
        .setApplicationName(applicationName)
        .build();
  }

  Credential getDefaultCredential(HttpTransport transport, JsonFactory jsonFactory) {
    GoogleCredential credential;
    try {
      credential = GoogleCredential.getApplicationDefault(transport, jsonFactory);
    } catch (IOException e) {
      throw new RuntimeException("Failed to get application default credential.", e);
    }

    if (credential.createScopedRequired()) {
      Collection<String> bigqueryScope = BigqueryScopes.all();
      credential = credential.createScoped(bigqueryScope);
    }
    return credential;
  }

  String hashing(List<TableRow> rows) {
    if (rows == null || rows.isEmpty()) {
      LOG.warn("Invalid data for hashing. Empty checksum is returned.");
      return "";
    }
    List<HashCode> hashCodes = new ArrayList<>();
    for (TableRow row : rows) {
      hashCodes.add(Hashing.sha1().hashString(row.toString(), StandardCharsets.UTF_8));
    }
    return Hashing.combineUnordered(hashCodes).toString();
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
    description
        .appendText("was (")
        .appendText(actualChecksum)
        .appendText(")");
  }
}
