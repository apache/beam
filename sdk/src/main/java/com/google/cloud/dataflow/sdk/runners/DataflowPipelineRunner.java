/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.runners;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.util.Joiner;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.DataflowPackage;
import com.google.api.services.dataflow.model.Job;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsValidator;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.util.DataflowReleaseInfo;
import com.google.cloud.dataflow.sdk.util.IOChannelUtils;
import com.google.cloud.dataflow.sdk.util.MonitoringUtil;
import com.google.cloud.dataflow.sdk.util.PathValidator;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.Transport;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PInput;
import com.google.cloud.dataflow.sdk.values.POutput;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import com.fasterxml.jackson.core.JsonProcessingException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link PipelineRunner} that executes the operations in the
 * pipeline by first translating them to the Dataflow representation
 * using the {@link DataflowPipelineTranslator} and then submitting
 * them to a Dataflow service for execution.
 */
public class DataflowPipelineRunner extends PipelineRunner<DataflowPipelineJob> {
  private static final Logger LOG = LoggerFactory.getLogger(DataflowPipelineRunner.class);

  /** Provided configuration options. */
  private final DataflowPipelineOptions options;

  /** Client for the Dataflow service. This is used to actually submit jobs. */
  private final Dataflow dataflowClient;

  /** Translator for this DataflowPipelineRunner, based on options. */
  private final DataflowPipelineTranslator translator;

  /** A set of user defined functions to invoke at different points in execution. */
  private DataflowPipelineRunnerHooks hooks;

  // Environment version information
  private static final String ENVIRONMENT_MAJOR_VERSION = "1";

  /**
   * Construct a runner from the provided options.
   *
   * @param options Properties which configure the runner.
   * @return The newly created runner.
   */
  public static DataflowPipelineRunner fromOptions(PipelineOptions options) {
    DataflowPipelineOptions dataflowOptions =
        PipelineOptionsValidator.validate(DataflowPipelineOptions.class, options);
    ArrayList<String> missing = new ArrayList<>();

    if (dataflowOptions.getProject() == null) {
      missing.add("project");
    }
    if (dataflowOptions.getAppName() == null) {
      missing.add("appName");
    }
    if (missing.size() > 0) {
      throw new IllegalArgumentException(
          "Missing required values: " + Joiner.on(',').join(missing));
    }

    PathValidator validator = dataflowOptions.getPathValidator();
    validator.validateAndUpdateOptions();

    if (dataflowOptions.getFilesToStage() == null) {
      dataflowOptions.setFilesToStage(detectClassPathResourcesToStage(
          DataflowPipelineRunner.class.getClassLoader()));
      LOG.info("PipelineOptions.filesToStage was not specified. "
          + "Defaulting to files from the classpath: {}", dataflowOptions.getFilesToStage());
    }

    // Verify jobName according to service requirements.
    String jobName = dataflowOptions.getJobName().toLowerCase();
    Preconditions.checkArgument(
        jobName.matches("[a-z]([-a-z0-9]*[a-z0-9])?"),
        "JobName invalid; the name must consist of only the characters "
            + "[-a-z0-9], starting with a letter and ending with a letter "
            + "or number");
    Preconditions.checkArgument(jobName.length() <= 40,
        "JobName too long; must be no more than 40 characters in length");

    return new DataflowPipelineRunner(dataflowOptions);
  }

  private DataflowPipelineRunner(DataflowPipelineOptions options) {
    this.options = options;
    this.dataflowClient = options.getDataflowClient();
    this.translator = DataflowPipelineTranslator.fromOptions(options);

    // (Re-)register standard IO factories. Clobbers any prior credentials.
    IOChannelUtils.registerStandardIOFactories(options);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <Output extends POutput, Input extends PInput> Output apply(
      PTransform<Input, Output> transform, Input input) {
    if (transform instanceof Combine.GroupedValues) {
      // TODO: Redundant with translator registration?
      return (Output) PCollection.createPrimitiveOutputInternal(
          ((PCollection<?>) input).getWindowFn());
    } else if (transform instanceof GroupByKey) {
      // The DataflowPipelineRunner implementation of GroupByKey will sort values by timestamp,
      // so no need for an explicit sort transform.
      boolean runnerSortsByTimestamp = true;
      return (Output) ((GroupByKey) transform).applyHelper(
          (PCollection<?>) input, options.isStreaming(), runnerSortsByTimestamp);
    } else {
      return super.apply(transform, input);
    }
  }

  @Override
  public DataflowPipelineJob run(Pipeline pipeline) {
    LOG.info("Executing pipeline on the Dataflow Service, which will have billing implications "
        + "related to Google Compute Engine usage and other Google Cloud Services.");

    List<DataflowPackage> packages = options.getStager().stageFiles();
    Job newJob = translator.translate(pipeline, packages);

    String version = DataflowReleaseInfo.getReleaseInfo().getVersion();
    System.out.println("Dataflow SDK version: " + version);

    newJob.getEnvironment().setUserAgent(DataflowReleaseInfo.getReleaseInfo());
    // The Dataflow Service may write to the temporary directory directly, so
    // must be verified.
    if (!Strings.isNullOrEmpty(options.getTempLocation())) {
      DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
      newJob.getEnvironment().setTempStoragePrefix(
          dataflowOptions.getPathValidator().verifyGcsPath(options.getTempLocation()));
    }
    newJob.getEnvironment().setDataset(options.getTempDatasetId());
    newJob.getEnvironment().setClusterManagerApiService(
        options.getClusterManagerApi().getApiServiceName());
    newJob.getEnvironment().setExperiments(options.getExperiments());

    // Requirements about the service.
    Map<String, Object> environmentVersion = new HashMap<>();
    environmentVersion.put(PropertyNames.ENVIRONMENT_VERSION_MAJOR_KEY, ENVIRONMENT_MAJOR_VERSION);
    newJob.getEnvironment().setVersion(environmentVersion);
    // Default jobType is DATA_PARALLEL which is for java batch.
    String jobType = "DATA_PARALLEL";

    if (options.isStreaming()) {
      jobType = "STREAMING";
    }
    environmentVersion.put(PropertyNames.ENVIRONMENT_VERSION_JOB_TYPE_KEY, jobType);

    if (hooks != null) {
      hooks.modifyEnvironmentBeforeSubmission(newJob.getEnvironment());
    }

    if (!Strings.isNullOrEmpty(options.getDataflowJobFile())) {
      try (PrintWriter printWriter = new PrintWriter(
          new File(options.getDataflowJobFile()))) {
        String workSpecJson = DataflowPipelineTranslator.jobToString(newJob);
        printWriter.print(workSpecJson);
        LOG.info("Printed workflow specification to {}", options.getDataflowJobFile());
      } catch (JsonProcessingException ex) {
        LOG.warn("Cannot translate workflow spec to json for debug.");
      } catch (FileNotFoundException ex) {
        LOG.warn("Cannot create workflow spec output file.");
      }
    }

    Job jobResult;
    try {
      jobResult = dataflowClient.v1b3().projects().jobs()
          .create(options.getProject(), newJob)
          .execute();
    } catch (GoogleJsonResponseException e) {
      throw new RuntimeException(
          "Failed to create a workflow job: "
              + (e.getDetails() != null ? e.getDetails().getMessage() : e), e);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create a workflow job", e);
    }

    LOG.info("To access the Dataflow monitoring console, please navigate to {}",
        MonitoringUtil.getJobMonitoringPageURL(options.getProject(), jobResult.getId()));
    System.out.println("Submitted job: " + jobResult.getId());

    // Use a raw client for post-launch monitoring, as status calls may fail
    // regularly and need not be retried automatically.
    return new DataflowPipelineJob(options.getProject(), jobResult.getId(),
        Transport.newRawDataflowClient(options).build());
  }

  /**
   * Returns the DataflowPipelineTranslator associated with this object.
   */
  public DataflowPipelineTranslator getTranslator() {
    return translator;
  }

  /**
   * Sets callbacks to invoke during execution see {@code DataflowPipelineRunnerHooks}.
   * Important: setHooks is experimental. Please consult with the Dataflow team before using it.
   * You should expect this class to change significantly in future versions of the SDK or be
   * removed entirely.
   */
  public void setHooks(DataflowPipelineRunnerHooks hooks) {
    this.hooks = hooks;
  }


  /////////////////////////////////////////////////////////////////////////////

  @Override
  public String toString() { return "DataflowPipelineRunner#" + hashCode(); }

  /**
   * Attempts to detect all the resources the class loader has access to. This does not recurse
   * to class loader parents stopping it from pulling in resources from the system class loader.
   *
   * @param classLoader The URLClassLoader to use to detect resources to stage.
   * @throws IllegalArgumentException  If either the class loader is not a URLClassLoader or one
   * of the resources the class loader exposes is not a file resource.
   * @return A list of absolute paths to the resources the class loader uses.
   */
  protected static List<String> detectClassPathResourcesToStage(ClassLoader classLoader) {
    if (!(classLoader instanceof URLClassLoader)) {
      String message = String.format("Unable to use ClassLoader to detect classpath elements. "
          + "Current ClassLoader is %s, only URLClassLoaders are supported.", classLoader);
      LOG.error(message);
      throw new IllegalArgumentException(message);
    }

    List<String> files = new ArrayList<>();
    for (URL url : ((URLClassLoader) classLoader).getURLs()) {
      try {
        files.add(new File(url.toURI()).getAbsolutePath());
      } catch (IllegalArgumentException | URISyntaxException e) {
        String message = String.format("Unable to convert url (%s) to file.", url);
        LOG.error(message);
        throw new IllegalArgumentException(message, e);
      }
    }
    return files;
  }
}
