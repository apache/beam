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

import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.Sleeper;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.BigqueryScopes;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.QueryRequest;
import com.google.api.services.bigquery.model.QueryResponse;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest;
import com.google.api.services.bigquery.model.TableDataInsertAllRequest.Rows;
import com.google.api.services.bigquery.model.TableList;
import com.google.api.services.bigquery.model.TableList.Tables;
import com.google.auth.Credentials;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.beam.sdk.util.BackOffAdapter;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Transport;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper class to call Bigquery API calls.
 *
 * <p>Example:
 *
 * <p>Get a new Bigquery client:
 *
 * <pre>{@code [
 *    BigqueryClient client = BigqueryClient.getNewBigquerryClient(applicationName);
 * ]}</pre>
 *
 * <p>Execute a query with retries:
 *
 * <pre>{@code [
 *    QueryResponse response = client.queryWithRetries(queryString, projectId);
 * ]}</pre>
 *
 * <p>Create a new dataset in one project:
 *
 * <pre>{@code [
 *    client.createNewDataset(projectId, datasetId);
 * ]}</pre>
 *
 * <p>Delete a dataset in one project, included its all tables:
 *
 * <pre>{@code [
 *    client.deleteDataset(projectId, datasetId);
 * ]}</pre>
 *
 * <p>Create a new table
 *
 * <pre>{@code [
 *    client.createNewTable(projectId, datasetId, newTable)
 * ]}</pre>
 *
 * <p>Insert data into table
 *
 * <pre>{@code [
 *    client.insertDataToTable(projectId, datasetId, tableName, rows)
 * ]}</pre>
 */
public class BigqueryClient {
  private static final Logger LOG = LoggerFactory.getLogger(BigqueryClient.class);
  // The maximum number of retries to execute a BigQuery RPC
  static final int MAX_QUERY_RETRIES = 4;

  // The initial backoff for executing a BigQuery RPC
  private static final Duration INITIAL_BACKOFF = Duration.standardSeconds(1L);

  // The backoff factory with initial configs
  static final FluentBackoff BACKOFF_FACTORY =
      FluentBackoff.DEFAULT.withMaxRetries(MAX_QUERY_RETRIES).withInitialBackoff(INITIAL_BACKOFF);

  private Bigquery bqClient;

  private static Credentials getDefaultCredential() {
    GoogleCredentials credential;
    try {
      credential = GoogleCredentials.getApplicationDefault();
    } catch (IOException e) {
      throw new RuntimeException("Failed to get application default credential.", e);
    }

    if (credential.createScopedRequired()) {
      Collection<String> bigqueryScope = Lists.newArrayList(BigqueryScopes.all());
      credential = credential.createScoped(bigqueryScope);
    }
    return credential;
  }

  public static Bigquery getNewBigquerryClient(String applicationName) {
    HttpTransport transport = Transport.getTransport();
    JsonFactory jsonFactory = Transport.getJsonFactory();
    Credentials credential = getDefaultCredential();
    return new Bigquery.Builder(transport, jsonFactory, new HttpCredentialsAdapter(credential))
        .setApplicationName(applicationName)
        .build();
  }

  public static BigqueryClient getClient(String applicationName) {
    return new BigqueryClient(applicationName);
  }

  public BigqueryClient(String applicationName) {
    bqClient = BigqueryClient.getNewBigquerryClient(applicationName);
  }

  @Nonnull
  public QueryResponse queryWithRetries(String query, String projectId)
      throws IOException, InterruptedException {
    Sleeper sleeper = Sleeper.DEFAULT;
    BackOff backoff = BackOffAdapter.toGcpBackOff(BACKOFF_FACTORY.backoff());
    IOException lastException = null;
    QueryRequest bqQueryRequest = new QueryRequest().setQuery(query);
    do {
      if (lastException != null) {
        LOG.warn("Retrying query ({}) after exception", bqQueryRequest.getQuery(), lastException);
      }
      try {
        QueryResponse response = bqClient.jobs().query(projectId, bqQueryRequest).execute();
        if (response != null) {
          return response;
        } else {
          lastException =
              new IOException("Expected valid response from query job, but received null.");
        }
      } catch (IOException e) {
        // ignore and retry
        lastException = e;
      }
    } while (BackOffUtils.next(sleeper, backoff));

    throw new RuntimeException(
        String.format(
            "Unable to get BigQuery response after retrying %d times using query (%s)",
            MAX_QUERY_RETRIES, bqQueryRequest.getQuery()),
        lastException);
  }

  public void createNewDataset(String projectId, String datasetId) {
    try {
      bqClient
          .datasets()
          .insert(
              projectId,
              new Dataset().setDatasetReference(new DatasetReference().setDatasetId(datasetId)))
          .execute();
      LOG.info("Successfully created new dataset : " + datasetId);
    } catch (Exception e) {
      LOG.debug("Exceptions caught when creating new dataset: " + e.getMessage());
    }
  }

  public void deleteTable(String projectId, String datasetId, String tableName) {
    try {
      bqClient.tables().delete(projectId, datasetId, tableName).execute();
      LOG.info("Successfully deleted table: " + tableName);
    } catch (Exception e) {
      LOG.debug("Exception caught when deleting table: " + e.getMessage());
    }
  }

  public void deleteDataset(String projectId, String datasetId) {
    try {
      TableList tables = bqClient.tables().list(projectId, datasetId).execute();
      for (Tables table : tables.getTables()) {
        this.deleteTable(projectId, datasetId, table.getTableReference().getTableId());
      }
    } catch (Exception e) {
      LOG.debug("Exceptions caught when listing all tables: " + e.getMessage());
    }

    try {
      bqClient.datasets().delete(projectId, datasetId).execute();
      LOG.info("Successfully deleted dataset: " + datasetId);
    } catch (Exception e) {
      LOG.debug("Exceptions caught when deleting dataset: " + e.getMessage());
    }
  }

  public void createNewTable(String projectId, String datasetId, Table newTable) {
    try {
      this.bqClient.tables().insert(projectId, datasetId, newTable).execute();
      LOG.info("Successfully created new table: " + newTable.getId());
    } catch (Exception e) {
      LOG.debug("Exceptions caught when creating new table: " + e.getMessage());
    }
  }

  public void insertDataToTable(
      String projectId, String datasetId, String tableName, List<Map<String, Object>> rows) {
    try {
      List<Rows> dataRows =
          rows.stream().map(row -> new Rows().setJson(row)).collect(Collectors.toList());
      this.bqClient
          .tabledata()
          .insertAll(
              projectId, datasetId, tableName, new TableDataInsertAllRequest().setRows(dataRows))
          .execute();
      LOG.info("Successfully inserted data into table : " + tableName);
    } catch (Exception e) {
      LOG.debug("Exceptions caught when inserting data: " + e.getMessage());
    }
  }
}
