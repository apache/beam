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
package ${package}.common;

import org.apache.beam.runners.dataflow.BlockingDataflowRunner;
import org.apache.beam.runners.dataflow.DataflowPipelineJob;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.runners.dataflow.util.MonitoringUtil;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.googleapis.services.AbstractGoogleClientRequest;
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
import com.google.api.services.pubsub.model.Topic;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.BigQueryOptions;
import org.apache.beam.sdk.transforms.IntraBundleParallelization;
import org.apache.beam.sdk.util.Transport;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletResponse;

/**
 * The utility class that sets up and tears down external resources, starts the Google Cloud Pub/Sub
 * injector, and cancels the streaming and the injector pipelines once the program terminates.
 *
 * <p>It is used to run Dataflow examples, such as TrafficMaxLaneFlow and TrafficRoutes.
 */
public class DataflowExampleUtils {

  private final DataflowPipelineOptions options;
  private Bigquery bigQueryClient = null;
  private Pubsub pubsubClient = null;
  private Dataflow dataflowClient = null;
  private Set<DataflowPipelineJob> jobsToCancel = Sets.newHashSet();
  private List<String> pendingMessages = Lists.newArrayList();

  /**
   * Define an interface that supports the PubSub and BigQuery example options.
   */
  public static interface DataflowExampleUtilsOptions
        extends DataflowExampleOptions, ExamplePubsubTopicOptions, ExampleBigQueryTableOptions {
  }

  public DataflowExampleUtils(DataflowPipelineOptions options) {
    this.options = options;
  }

  /**
   * Do resources and runner options setup.
   */
  public DataflowExampleUtils(DataflowPipelineOptions options, boolean isUnbounded)
      throws IOException {
    this.options = options;
    setupResourcesAndRunner(isUnbounded);
  }

  /**
   * Sets up external resources that are required by the example,
   * such as Pub/Sub topics and BigQuery tables.
   *
   * @throws IOException if there is a problem setting up the resources
   */
  public void setup() throws IOException {
    setupPubsubTopic();
    setupBigQueryTable();
  }

  /**
   * Set up external resources, and configure the runner appropriately.
   */
  public void setupResourcesAndRunner(boolean isUnbounded) throws IOException {
    if (isUnbounded) {
      options.setStreaming(true);
    }
    setup();
    setupRunner();
  }

  /**
   * Sets up the Google Cloud Pub/Sub topic.
   *
   * <p>If the topic doesn't exist, a new topic with the given name will be created.
   *
   * @throws IOException if there is a problem setting up the Pub/Sub topic
   */
  public void setupPubsubTopic() throws IOException {
    ExamplePubsubTopicOptions pubsubTopicOptions = options.as(ExamplePubsubTopicOptions.class);
    if (!pubsubTopicOptions.getPubsubTopic().isEmpty()) {
      pendingMessages.add("*******************Set Up Pubsub Topic*********************");
      setupPubsubTopic(pubsubTopicOptions.getPubsubTopic());
      pendingMessages.add("The Pub/Sub topic has been set up for this example: "
          + pubsubTopicOptions.getPubsubTopic());
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
    ExamplePubsubTopicOptions pubsubTopicOptions = options.as(ExamplePubsubTopicOptions.class);
    if (!pubsubTopicOptions.getPubsubTopic().isEmpty()) {
      try {
        deletePubsubTopic(pubsubTopicOptions.getPubsubTopic());
        pendingMessages.add("The Pub/Sub topic has been deleted: "
            + pubsubTopicOptions.getPubsubTopic());
      } catch (IOException e) {
        pendingMessages.add("Failed to delete the Pub/Sub topic : "
            + pubsubTopicOptions.getPubsubTopic());
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
      pubsubClient = Transport.newPubsubClient(options).build();
    }
    if (executeNullIfNotFound(pubsubClient.projects().topics().get(topic)) == null) {
      pubsubClient.projects().topics().create(topic, new Topic().setName(topic)).execute();
    }
  }

  /**
   * Deletes the Google Cloud Pub/Sub topic.
   *
   * @throws IOException if there is a problem deleting the Pub/Sub topic
   */
  private void deletePubsubTopic(String topic) throws IOException {
    if (pubsubClient == null) {
      pubsubClient = Transport.newPubsubClient(options).build();
    }
    if (executeNullIfNotFound(pubsubClient.projects().topics().get(topic)) != null) {
      pubsubClient.projects().topics().delete(topic).execute();
    }
  }

  /**
   * If this is an unbounded (streaming) pipeline, and both inputFile and pubsub topic are defined,
   * start an 'injector' pipeline that publishes the contents of the file to the given topic, first
   * creating the topic if necessary.
   */
  public void startInjectorIfNeeded(String inputFile) {
    ExamplePubsubTopicOptions pubsubTopicOptions = options.as(ExamplePubsubTopicOptions.class);
    if (pubsubTopicOptions.isStreaming()
        && inputFile != null && !inputFile.isEmpty()
        && pubsubTopicOptions.getPubsubTopic() != null
        && !pubsubTopicOptions.getPubsubTopic().isEmpty()) {
      runInjectorPipeline(inputFile, pubsubTopicOptions.getPubsubTopic());
    }
  }

  public void setupRunner() {
    if (options.isStreaming() && options.getRunner().equals(BlockingDataflowRunner.class)) {
      // In order to cancel the pipelines automatically,
      // {@literal DataflowRunner} is forced to be used.
      options.setRunner(DataflowRunner.class);
    }
  }

  /**
   * Runs the batch injector for the streaming pipeline.
   *
   * <p>The injector pipeline will read from the given text file, and inject data
   * into the Google Cloud Pub/Sub topic.
   */
  public void runInjectorPipeline(String inputFile, String topic) {
    DataflowPipelineOptions copiedOptions = options.cloneAs(DataflowPipelineOptions.class);
    copiedOptions.setStreaming(false);
    copiedOptions.setWorkerHarnessContainerImage(
        DataflowRunner.BATCH_WORKER_HARNESS_CONTAINER_IMAGE);
    copiedOptions.setNumWorkers(
        options.as(ExamplePubsubTopicOptions.class).getInjectorNumWorkers());
    copiedOptions.setJobName(options.getJobName() + "-injector");
    Pipeline injectorPipeline = Pipeline.create(copiedOptions);
    injectorPipeline.apply(TextIO.Read.from(inputFile))
                    .apply(IntraBundleParallelization
                        .of(PubsubFileInjector.publish(topic))
                        .withMaxParallelism(20));
    DataflowPipelineJob injectorJob = (DataflowPipelineJob) injectorPipeline.run();
    jobsToCancel.add(injectorJob);
  }

  /**
   * Runs the provided injector pipeline for the streaming pipeline.
   */
  public void runInjectorPipeline(Pipeline injectorPipeline) {
    DataflowPipelineJob injectorJob = (DataflowPipelineJob) injectorPipeline.run();
    jobsToCancel.add(injectorJob);
  }

  /**
   * Start the auxiliary injector pipeline, then wait for this pipeline to finish.
   */
  public void mockUnboundedSource(String inputFile, PipelineResult result) {
    startInjectorIfNeeded(inputFile);
    waitToFinish(result);
  }

  /**
   * If {@literal DataflowRunner} or {@literal BlockingDataflowRunner} is used,
   * waits for the pipeline to finish and cancels it (and the injector) before the program exists.
   */
  public void waitToFinish(PipelineResult result) {
    if (result instanceof DataflowPipelineJob) {
      final DataflowPipelineJob job = (DataflowPipelineJob) result;
      jobsToCancel.add(job);
      if (!options.as(DataflowExampleOptions.class).getKeepJobsRunning()) {
        addShutdownHook(jobsToCancel);
      }
      try {
        job.waitToFinish(-1, TimeUnit.SECONDS, new MonitoringUtil.LoggingHandler());
      } catch (Exception e) {
        throw new RuntimeException("Failed to wait for job to finish: " + job.getJobId());
      }
    } else {
      // Do nothing if the given PipelineResult doesn't support waitToFinish(),
      // such as EvaluationResults returned by DirectRunner.
    }
  }

  private void addShutdownHook(final Collection<DataflowPipelineJob> jobs) {
    if (dataflowClient == null) {
      dataflowClient = options.getDataflowClient();
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
            try {
              Thread.sleep(10000);
            } catch (InterruptedException e) {
              // Ignore
            }
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
      if (e.getStatusCode() == HttpServletResponse.SC_NOT_FOUND) {
        return null;
      } else {
        throw e;
      }
    }
  }
}
