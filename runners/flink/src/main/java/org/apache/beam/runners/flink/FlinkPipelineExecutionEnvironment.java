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
package org.apache.beam.runners.flink;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.core.Base64Variants;
import com.google.common.base.Strings;
import com.google.common.hash.Funnels;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.util.ZipFiles;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The class that instantiates and manages the execution of a given job. Depending on if the job is
 * a Streaming or Batch processing one, it creates the adequate execution environment ({@link
 * ExecutionEnvironment} or {@link StreamExecutionEnvironment}), the necessary {@link
 * FlinkPipelineTranslator} ({@link FlinkBatchPipelineTranslator} or {@link
 * FlinkStreamingPipelineTranslator}) to transform the Beam job into a Flink one, and executes the
 * (translated) job.
 */
class FlinkPipelineExecutionEnvironment {

  private static final Logger LOG =
      LoggerFactory.getLogger(FlinkPipelineExecutionEnvironment.class);

  private final FlinkPipelineOptions options;

  /**
   * The Flink Batch execution environment. This is instantiated to either a {@link
   * org.apache.flink.api.java.CollectionEnvironment}, a {@link
   * org.apache.flink.api.java.LocalEnvironment} or a {@link
   * org.apache.flink.api.java.RemoteEnvironment}, depending on the configuration options.
   */
  private ExecutionEnvironment flinkBatchEnv;

  /**
   * The Flink Streaming execution environment. This is instantiated to either a {@link
   * org.apache.flink.streaming.api.environment.LocalStreamEnvironment} or a {@link
   * org.apache.flink.streaming.api.environment.RemoteStreamEnvironment}, depending on the
   * configuration options, and more specifically, the url of the master.
   */
  private StreamExecutionEnvironment flinkStreamEnv;

  /**
   * Creates a {@link FlinkPipelineExecutionEnvironment} with the user-specified parameters in the
   * provided {@link FlinkPipelineOptions}.
   *
   * @param options the user-defined pipeline options.
   */
  FlinkPipelineExecutionEnvironment(FlinkPipelineOptions options) {
    this.options = checkNotNull(options);
  }

  /**
   * Depending on if the job is a Streaming or a Batch one, this method creates the necessary
   * execution environment and pipeline translator, and translates the {@link
   * org.apache.beam.sdk.values.PCollection} program into a {@link
   * org.apache.flink.api.java.DataSet} or {@link
   * org.apache.flink.streaming.api.datastream.DataStream} one.
   */
  public void translate(Pipeline pipeline) {
    this.flinkBatchEnv = null;
    this.flinkStreamEnv = null;

    PipelineTranslationOptimizer optimizer =
        new PipelineTranslationOptimizer(TranslationMode.BATCH, options);

    optimizer.translate(pipeline);
    TranslationMode translationMode = optimizer.getTranslationMode();

    pipeline.replaceAll(
        FlinkTransformOverrides.getDefaultOverrides(translationMode == TranslationMode.STREAMING));

    // Local flink configurations work in the same JVM and have no problems with improperly
    // formatted files on classpath (eg. directories with .class files or empty directories).
    // prepareFilesToStage() only when using remote flink cluster.
    List<String> filesToStage;
    if (!options.getFlinkMaster().matches("\\[.*\\]")) {
      filesToStage = prepareFilesToStage();
    } else {
      filesToStage = options.getFilesToStage();
    }

    FlinkPipelineTranslator translator;
    if (translationMode == TranslationMode.STREAMING) {
      this.flinkStreamEnv =
          FlinkExecutionEnvironments.createStreamExecutionEnvironment(options, filesToStage);
      if (optimizer.hasUnboundedSources()
          && !flinkStreamEnv.getCheckpointConfig().isCheckpointingEnabled()) {
        LOG.warn(
            "UnboundedSources present which rely on checkpointing, but checkpointing is disabled.");
      }
      translator = new FlinkStreamingPipelineTranslator(flinkStreamEnv, options);
    } else {
      this.flinkBatchEnv =
          FlinkExecutionEnvironments.createBatchExecutionEnvironment(options, filesToStage);
      translator = new FlinkBatchPipelineTranslator(flinkBatchEnv, options);
    }

    translator.translate(pipeline);
  }

  private List<String> prepareFilesToStage() {
    return options
        .getFilesToStage()
        .stream()
        .map(File::new)
        .filter(File::exists)
        .map(file -> file.isDirectory() ? packageDirectoriesToStage(file) : file.getAbsolutePath())
        .collect(Collectors.toList());
  }

  private String packageDirectoriesToStage(File directoryToStage) {
    String hash = calculateDirectoryContentHash(directoryToStage);
    String pathForJar = getUniqueJarPath(hash);
    zipDirectory(directoryToStage, pathForJar);
    return pathForJar;
  }

  private String calculateDirectoryContentHash(File directoryToStage) {
    Hasher hasher = Hashing.md5().newHasher();
    try (OutputStream hashStream = Funnels.asOutputStream(hasher)) {
      ZipFiles.zipDirectory(directoryToStage, hashStream);
      return Base64Variants.MODIFIED_FOR_URL.encode(hasher.hash().asBytes());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private String getUniqueJarPath(String contentHash) {
    String tempLocation = options.getTempLocation();

    checkArgument(
        !Strings.isNullOrEmpty(tempLocation),
        "Please provide \"tempLocation\" pipeline option. Flink runner needs it to store jars "
            + "made of directories that were on classpath.");

    return String.format("%s%s.jar", tempLocation, contentHash);
  }

  private void zipDirectory(File directoryToStage, String uniqueDirectoryPath) {
    try {
      ZipFiles.zipDirectory(directoryToStage, new FileOutputStream(uniqueDirectoryPath));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Launches the program execution. */
  public JobExecutionResult executePipeline() throws Exception {
    final String jobName = options.getJobName();

    if (flinkBatchEnv != null) {
      return flinkBatchEnv.execute(jobName);
    } else if (flinkStreamEnv != null) {
      return flinkStreamEnv.execute(jobName);
    } else {
      throw new IllegalStateException("The Pipeline has not yet been translated.");
    }
  }
}
