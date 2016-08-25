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
package org.apache.beam.examples.common;

import org.apache.beam.runners.dataflow.BlockingDataflowRunner;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.util.MonitoringUtil;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.BigQueryOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PubsubOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.runners.PipelineRunner;
import org.apache.beam.sdk.util.AttemptBoundedExponentialBackOff;
import org.apache.beam.sdk.util.Transport;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.googleapis.services.AbstractGoogleClientRequest;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.BackOffUtils;
import com.google.api.client.util.Sleeper;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Datasets;
import com.google.api.services.bigquery.Bigquery.Tables;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.pubsub.Pubsub;
import com.google.api.services.pubsub.model.Subscription;
import com.google.api.services.pubsub.model.Topic;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * The utility class that sets up and tears down external resources,
 * and cancels the streaming pipelines once the program terminates.
 *
 * <p>It is used to run Beam examples, such as TrafficMaxLaneFlow and TrafficRoutes.
 */
public class ExampleUtils {

  private static final int SC_NOT_FOUND = 404;

  private final PipelineOptions options;
  private Bigquery bigQueryClient = null;
  private Pubsub pubsubClient = null;
  private Dataflow dataflowClient = null;
  private Set<DataflowPipelineJob> jobsToCancel = Sets.newHashSet();
  private List<String> pendingMessages = Lists.newArrayList();

  /**
   * Do resources and runner options setup.
   */
  public ExampleUtils(PipelineOptions options) {
    this.options = options;
    setupRunner();
  }

  /**
   * Sets up external resources that are required by the example,
   * such as Pub/Sub topics and BigQuery tables.
   *
   * @throws IOException if there is a problem setting up the resources
   */
  public void setup() throws IOException {
    Sleeper sleeper = Sleeper.DEFAULT;
    BackOff backOff = new AttemptBoundedExponentialBackOff(3, 200);
    Throwable lastException = null;
    try {
      do {
        try {
          setupPubsub();
          setupBigQueryTable();
          return;
        } catch (GoogleJsonResponseException e) {
          lastException = e;
        }
      } while (BackOffUtils.next(sleeper, backOff));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      // Ignore InterruptedException
    }
    throw new RuntimeException(lastException);
  }

  /**
   * Sets up the Google Cloud Pub/Sub topic.
   *
   * <p>If the topic doesn't exist, a new topic with the given name will be created.
   *
   * @throws IOException if there is a problem setting up the Pub/Sub topic
   */
  public void setupPubsub() throws IOException {
    ExamplePubsubTopicAndSubscriptionOptions pubsubOptions =
        options.as(ExamplePubsubTopicAndSubscriptionOptions.class);
    if (!pubsubOptions.getPubsubTopic().isEmpty()) {
      pendingMessages.add("**********************Set Up Pubsub************************");
      setupPubsubTopic(pubsubOptions.getPubsubTopic());
      pendingMessages.add("The Pub/Sub topic has been set up for this example: "
          + pubsubOptions.getPubsubTopic());

      if (!pubsubOptions.getPubsubSubscription().isEmpty()) {
        setupPubsubSubscription(
            pubsubOptions.getPubsubTopic(), pubsubOptions.getPubsubSubscription());
        pendingMessages.add("The Pub/Sub subscription has been set up for this example: "
            + pubsubOptions.getPubsubSubscription());
      }
    }
  }

  /**
   * Sets up the BigQuery table with the given schema.
   *
   * <p>If the table already exists, the schema has to match the given one. Otherwise, the example
   * will throw a RuntimeException. If the table doesn't exist, a new table with the given schema
   * will be created.
   *
   * @throws IOException if there is a problem setting up the BigQuery table
   */
  public void setupBigQueryTable() throws IOException {
    ExampleBigQueryTableOptions bigQueryTableOptions =
        options.as(ExampleBigQueryTableOptions.class);
    if (bigQueryTableOptions.getBigQueryDataset() != null
        && bigQueryTableOptions.getBigQueryTable() != null
        && bigQueryTableOptions.getBigQuerySchema() != null) {
      pendingMessages.add("******************Set Up Big Query Table*******************");
      setupBigQueryTable(bigQueryTableOptions.getProject(),
                         bigQueryTableOptions.getBigQueryDataset(),
                         bigQueryTableOptions.getBigQueryTable(),
                         bigQueryTableOptions.getBigQuerySchema());
      pendingMessages.add("The BigQuery table has been set up for this example: "
          + bigQueryTableOptions.getProject()
          + ":" + bigQueryTableOptions.getBigQueryDataset()
          + "." + bigQueryTableOptions.getBigQueryTable());
    }
  }

  /**
   * Tears down external resources that can be deleted upon the example's completion.
   */
  private void tearDown() {
    pendingMessages.add("*************************Tear Down*************************");
    ExamplePubsubTopicAndSubscriptionOptions pubsubOptions =
        options.as(ExamplePubsubTopicAndSubscriptionOptions.class);
    if (!pubsubOptions.getPubsubTopic().isEmpty()) {
      try {
        deletePubsubTopic(pubsubOptions.getPubsubTopic());
        pendingMessages.add("The Pub/Sub topic has been deleted: "
            + pubsubOptions.getPubsubTopic());
      } catch (IOException e) {
        pendingMessages.add("Failed to delete the Pub/Sub topic : "
            + pubsubOptions.getPubsubTopic());
      }
      if (!pubsubOptions.getPubsubSubscription().isEmpty()) {
        try {
          deletePubsubSubscription(pubsubOptions.getPubsubSubscription());
          pendingMessages.add("The Pub/Sub subscription has been deleted: "
              + pubsubOptions.getPubsubSubscription());
        } catch (IOException e) {
          pendingMessages.add("Failed to delete the Pub/Sub subscription : "
              + pubsubOptions.getPubsubSubscription());
        }
      }
    }

    ExampleBigQueryTableOptions bigQueryTableOptions =
        options.as(ExampleBigQueryTableOptions.class);
    if (bigQueryTableOptions.getBigQueryDataset() != null
        && bigQueryTableOptions.getBigQueryTable() != null
        && bigQueryTableOptions.getBigQuerySchema() != null) {
      pendingMessages.add("The BigQuery table might contain the example's output, "
          + "and it is not deleted automatically: "
          + bigQueryTableOptions.getProject()
          + ":" + bigQueryTableOptions.getBigQueryDataset()
          + "." + bigQueryTableOptions.getBigQueryTable());
      pendingMessages.add("Please go to the Developers Console to delete it manually."
          + " Otherwise, you may be charged for its usage.");
    }
  }

  private void setupBigQueryTable(String projectId, String datasetId, String tableId,
      TableSchema schema) throws IOException {
    if (bigQueryClient == null) {
      bigQueryClient = Transport.newBigQueryClient(options.as(BigQueryOptions.class)).build();
    }

    Datasets datasetService = bigQueryClient.datasets();
    if (executeNullIfNotFound(datasetService.get(projectId, datasetId)) == null) {
      Dataset newDataset = new Dataset().setDatasetReference(
          new DatasetReference().setProjectId(projectId).setDatasetId(datasetId));
      datasetService.insert(projectId, newDataset).execute();
    }

    Tables tableService = bigQueryClient.tables();
    Table table = executeNullIfNotFound(tableService.get(projectId, datasetId, tableId));
    if (table == null) {
      Table newTable = new Table().setSchema(schema).setTableReference(
          new TableReference().setProjectId(projectId).setDatasetId(datasetId).setTableId(tableId));
      tableService.insert(projectId, datasetId, newTable).execute();
    } else if (!table.getSchema().equals(schema)) {
      throw new RuntimeException(
          "Table exists and schemas do not match, expecting: " + schema.toPrettyString()
          + ", actual: " + table.getSchema().toPrettyString());
    }
  }

  private void setupPubsubTopic(String topic) throws IOException {
    if (pubsubClient == null) {
      pubsubClient = Transport.newPubsubClient(options.as(PubsubOptions.class)).build();
    }
    if (executeNullIfNotFound(pubsubClient.projects().topics().get(topic)) == null) {
      pubsubClient.projects().topics().create(topic, new Topic().setName(topic)).execute();
    }
  }

  private void setupPubsubSubscription(String topic, String subscription) throws IOException {
    if (pubsubClient == null) {
      pubsubClient = Transport.newPubsubClient(options.as(PubsubOptions.class)).build();
    }
    if (executeNullIfNotFound(pubsubClient.projects().subscriptions().get(subscription)) == null) {
      Subscription subInfo = new Subscription()
        .setAckDeadlineSeconds(60)
        .setTopic(topic);
      pubsubClient.projects().subscriptions().create(subscription, subInfo).execute();
    }
  }

  /**
   * Deletes the Google Cloud Pub/Sub topic.
   *
   * @throws IOException if there is a problem deleting the Pub/Sub topic
   */
  private void deletePubsubTopic(String topic) throws IOException {
    if (pubsubClient == null) {
      pubsubClient = Transport.newPubsubClient(options.as(PubsubOptions.class)).build();
    }
    if (executeNullIfNotFound(pubsubClient.projects().topics().get(topic)) != null) {
      pubsubClient.projects().topics().delete(topic).execute();
    }
  }

  /**
   * Deletes the Google Cloud Pub/Sub subscription.
   *
   * @throws IOException if there is a problem deleting the Pub/Sub subscription
   */
  private void deletePubsubSubscription(String subscription) throws IOException {
    if (pubsubClient == null) {
      pubsubClient = Transport.newPubsubClient(options.as(PubsubOptions.class)).build();
    }
    if (executeNullIfNotFound(pubsubClient.projects().subscriptions().get(subscription)) != null) {
      pubsubClient.projects().subscriptions().delete(subscription).execute();
    }
  }

  /**
   * Do some runner setup: check that the DirectRunner is not used in conjunction with
   * streaming, and if streaming is specified, use the DataflowRunner.
   */
  private void setupRunner() {
    Class<? extends PipelineRunner<?>> runner = options.getRunner();
    if (options.as(StreamingOptions.class).isStreaming()
        && runner.equals(BlockingDataflowRunner.class)) {
      // In order to cancel the pipelines automatically,
      // {@literal DataflowRunner} is forced to be used.
      options.setRunner(DataflowRunner.class);
    }
  }

  /**
   * If {@literal DataflowRunner} or {@literal BlockingDataflowRunner} is used,
   * waits for the pipeline to finish and cancels it (and the injector) before the program exists.
   */
  public void waitToFinish(PipelineResult result) {
    if (result instanceof DataflowPipelineJob) {
      final DataflowPipelineJob job = (DataflowPipelineJob) result;
      jobsToCancel.add(job);
      if (!options.as(ExampleOptions.class).getKeepJobsRunning()) {
        addShutdownHook(jobsToCancel);
      }
      try {
        job.waitUntilFinish();
      } catch (Exception e) {
        throw new RuntimeException("Failed to wait for job to finish: " + job.getJobId());
      }
    } else {
      // Do nothing if the given PipelineResult doesn't support waitUntilFinish(),
      // such as EvaluationResults returned by DirectRunner.
      tearDown();
      printPendingMessages();
    }
  }

  private void addShutdownHook(final Collection<DataflowPipelineJob> jobs) {
    if (dataflowClient == null) {
      dataflowClient = options.as(DataflowPipelineOptions.class).getDataflowClient();
    }

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        tearDown();
        printPendingMessages();
        for (DataflowPipelineJob job : jobs) {
          System.out.println("Canceling example pipeline: " + job.getJobId());
          try {
            job.cancel();
          } catch (IOException e) {
            System.out.println("Failed to cancel the job,"
                + " please go to the Developers Console to cancel it manually");
            System.out.println(
                MonitoringUtil.getJobMonitoringPageURL(job.getProjectId(), job.getJobId()));
          }
        }

        for (DataflowPipelineJob job : jobs) {
          boolean cancellationVerified = false;
          for (int retryAttempts = 6; retryAttempts > 0; retryAttempts--) {
            if (job.getState().isTerminal()) {
              cancellationVerified = true;
              System.out.println("Canceled example pipeline: " + job.getJobId());
              break;
            } else {
              System.out.println(
                  "The example pipeline is still running. Verifying the cancellation.");
            }
            Uninterruptibles.sleepUninterruptibly(10, TimeUnit.SECONDS);
          }
          if (!cancellationVerified) {
            System.out.println("Failed to verify the cancellation for job: " + job.getJobId());
            System.out.println("Please go to the Developers Console to verify manually:");
            System.out.println(
                MonitoringUtil.getJobMonitoringPageURL(job.getProjectId(), job.getJobId()));
          }
        }
      }
    });
  }

  private void printPendingMessages() {
    System.out.println();
    System.out.println("***********************************************************");
    System.out.println("***********************************************************");
    for (String message : pendingMessages) {
      System.out.println(message);
    }
    System.out.println("***********************************************************");
    System.out.println("***********************************************************");
  }

  private static <T> T executeNullIfNotFound(
      AbstractGoogleClientRequest<T> request) throws IOException {
    try {
      return request.execute();
    } catch (GoogleJsonResponseException e) {
      if (e.getStatusCode() == SC_NOT_FOUND) {
        return null;
      } else {
        throw e;
      }
    }
  }
}
