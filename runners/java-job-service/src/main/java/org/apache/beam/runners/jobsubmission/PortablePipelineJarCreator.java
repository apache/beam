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
package org.apache.beam.runners.jobsubmission;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.jar.Attributes;
import java.util.jar.Attributes.Name;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import org.apache.beam.model.jobmanagement.v1.JobApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.model.pipeline.v1.RunnerApi.Pipeline;
import org.apache.beam.runners.core.construction.PipelineOptionsTranslation;
import org.apache.beam.runners.core.construction.resources.PipelineResources;
import org.apache.beam.runners.fnexecution.artifact.ArtifactRetrievalService;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.sdk.io.ClassLoaderFileSystem;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.MessageOrBuilder;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.util.JsonFormat;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.io.ByteStreams;
import org.apache.commons.compress.utils.IOUtils;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link PortablePipelineRunner} that bundles the input pipeline along with all dependencies,
 * artifacts, etc. required to run the pipeline into a jar that can be executed later.
 *
 * <p>Each {@link PortablePipelineJarCreator} instance is not threadsafe; a new instance is expected
 * to be constructed and {@link #run} once per job.
 */
public class PortablePipelineJarCreator implements PortablePipelineRunner {
  private static final Logger LOG = LoggerFactory.getLogger(PortablePipelineJarCreator.class);

  private final Class mainClass;

  @VisibleForTesting JarOutputStream outputStream;
  /** Wrapper over {@link #outputStream}. */
  @VisibleForTesting WritableByteChannel outputChannel;

  public PortablePipelineJarCreator(Class mainClass) {
    this.mainClass = mainClass;
  }

  /**
   * <em>Does not actually run the pipeline.</em> Instead bundles the input pipeline along with all
   * dependencies, artifacts, etc. required to run the pipeline into a jar that can be executed
   * later.
   */
  @Override
  public PortablePipelineResult run(Pipeline pipeline, JobInfo jobInfo) throws Exception {
    PortablePipelineOptions pipelineOptions =
        PipelineOptionsTranslation.fromProto(jobInfo.pipelineOptions())
            .as(PortablePipelineOptions.class);

    final String jobName = jobInfo.jobName();
    File outputFile = new File(pipelineOptions.getOutputExecutablePath());
    LOG.info("Creating jar {} for job {}", outputFile.getAbsolutePath(), jobName);
    outputStream =
        new JarOutputStream(new FileOutputStream(outputFile), createManifest(mainClass, jobName));
    outputChannel = Channels.newChannel(outputStream);
    PortablePipelineJarUtils.writeDefaultJobName(outputStream, jobName);
    writeClassPathResources(mainClass.getClassLoader(), pipelineOptions);
    writeAsJson(
        PipelineOptionsTranslation.toProto(pipelineOptions),
        PortablePipelineJarUtils.getPipelineOptionsUri(jobName));
    Pipeline pipelineWithClasspathArtifacts = writeArtifacts(pipeline, jobName);
    writeAsJson(pipelineWithClasspathArtifacts, PortablePipelineJarUtils.getPipelineUri(jobName));
    // Closing the channel also closes the underlying stream.
    outputChannel.close();

    LOG.info("Jar {} created successfully.", outputFile.getAbsolutePath());
    return new JarCreatorPipelineResult();
  }

  @VisibleForTesting
  Manifest createManifest(Class mainClass, String defaultJobName) {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    boolean classHasMainMethod = false;
    try {
      Class returnType = mainClass.getMethod("main", String[].class).getReturnType();
      if (returnType == Void.TYPE) {
        classHasMainMethod = true;
      } else {
        LOG.warn(
            "No Main-Class will be set in jar because main method in {} returns {}, expected void",
            mainClass,
            returnType);
      }
    } catch (NoSuchMethodException e) {
      LOG.warn("No Main-Class will be set in jar because {} lacks a main method.", mainClass);
    }
    if (classHasMainMethod) {
      manifest.getMainAttributes().put(Name.MAIN_CLASS, mainClass.getName());
    }
    return manifest;
  }

  /** Copy resources from {@code classLoader} to {@link #outputStream}. */
  private void writeClassPathResources(ClassLoader classLoader, PipelineOptions options)
      throws IOException {
    List<String> classPathResources =
        PipelineResources.detectClassPathResourcesToStage(classLoader, options);
    Preconditions.checkArgument(
        classPathResources.size() == 1, "Expected exactly one jar on " + classLoader.toString());
    copyResourcesFromJar(new JarFile(classPathResources.get(0)));
  }

  /** Copy resources from {@code inputJar} to {@link #outputStream}. */
  @VisibleForTesting
  protected void copyResourcesFromJar(JarFile inputJar) throws IOException {
    Enumeration<JarEntry> inputJarEntries = inputJar.entries();
    // The zip spec allows multiple files with the same name; the Java zip libraries do not.
    // Keep track of the files we've already written to filter out duplicates.
    // Also, ignore the old manifest; we want to write our own.
    Set<String> previousEntryNames = new HashSet<>(ImmutableList.of(JarFile.MANIFEST_NAME));
    while (inputJarEntries.hasMoreElements()) {
      JarEntry inputJarEntry = inputJarEntries.nextElement();
      InputStream inputStream = inputJar.getInputStream(inputJarEntry);
      String entryName = inputJarEntry.getName();
      if (previousEntryNames.contains(entryName)) {
        LOG.debug("Skipping duplicated file {}", entryName);
      } else {
        JarEntry outputJarEntry = new JarEntry(inputJarEntry);
        outputStream.putNextEntry(outputJarEntry);
        LOG.trace("Copying jar entry {}", inputJarEntry);
        IOUtils.copy(inputStream, outputStream);
        previousEntryNames.add(entryName);
      }
    }
  }

  /**
   * Stages all dependencies in pipeline into the jar file at outputStream, returning a new pipeline
   * that references these artifacts as classpath artifacts.
   */
  @VisibleForTesting
  protected Pipeline writeArtifacts(Pipeline pipeline, String jobName) throws IOException {
    Pipeline.Builder result = pipeline.toBuilder();
    for (Map.Entry<String, RunnerApi.Environment> env :
        pipeline.getComponents().getEnvironmentsMap().entrySet()) {
      result
          .getComponentsBuilder()
          .putEnvironments(env.getKey(), writeArtifacts(env.getValue(), jobName));
    }
    return result.build();
  }

  /**
   * Stages all dependencies in environment into the jar file at outputStream, returning a new
   * environment that references these artifacts as classpath artifacts.
   */
  private RunnerApi.Environment writeArtifacts(RunnerApi.Environment environment, String jobName)
      throws IOException {
    RunnerApi.Environment.Builder result = environment.toBuilder();
    result.clearDependencies();
    for (RunnerApi.ArtifactInformation artifact : environment.getDependenciesList()) {
      result.addDependencies(writeArtifact(artifact, jobName));
    }
    return result.build();
  }

  /**
   * Stages all artifact into the jar file at outputStream, returning a new artifact pointing to its
   * location in the classpath.
   */
  private RunnerApi.ArtifactInformation writeArtifact(
      RunnerApi.ArtifactInformation artifact, String jobName) throws IOException {
    String path = PortablePipelineJarUtils.getArtifactUri(jobName, UUID.randomUUID().toString());
    LOG.trace("Copying artifact {} to {}", artifact, path);
    outputStream.putNextEntry(new JarEntry(path));
    try (InputStream artifactStream = ArtifactRetrievalService.getArtifact(artifact)) {
      ByteStreams.copy(artifactStream, outputStream);
    }
    return artifact
        .toBuilder()
        .setTypeUrn(ArtifactRetrievalService.FILE_ARTIFACT_URN)
        .setTypePayload(
            RunnerApi.ArtifactFilePayload.newBuilder()
                .setPath(ClassLoaderFileSystem.SCHEMA + "://" + path)
                .build()
                .toByteString())
        .build();
  }

  /** Helper method for writing {@code message} in UTF-8 JSON format. */
  private void writeAsJson(MessageOrBuilder message, String outputPath) throws IOException {
    outputStream.putNextEntry(new JarEntry(outputPath));
    outputChannel.write(StandardCharsets.UTF_8.encode(JsonFormat.printer().print(message)));
  }

  private static class JarCreatorPipelineResult implements PortablePipelineResult {

    @Override
    public State getState() {
      return State.DONE;
    }

    @Override
    public State cancel() {
      return State.DONE;
    }

    @Override
    public State waitUntilFinish(Duration duration) {
      return State.DONE;
    }

    @Override
    public State waitUntilFinish() {
      return State.DONE;
    }

    @Override
    public MetricResults metrics() {
      throw new UnsupportedOperationException("Jar creation does not yield metrics.");
    }

    @Override
    public JobApi.MetricResults portableMetrics() throws UnsupportedOperationException {
      return JobApi.MetricResults.getDefaultInstance();
    }
  }
}
