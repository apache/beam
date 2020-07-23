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
package org.apache.beam.runners.twister2;

import static org.apache.beam.runners.core.construction.resources.PipelineResources.detectClassPathResourcesToStage;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.driver.DriverJobState;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.api.scheduler.Twister2JobState;
import edu.iu.dsc.tws.api.tset.sets.TSet;
import edu.iu.dsc.tws.api.tset.sets.batch.BatchTSet;
import edu.iu.dsc.tws.local.LocalSubmitter;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.job.Twister2Submitter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.beam.runners.core.construction.PTransformMatchers;
import org.apache.beam.runners.core.construction.PTransformTranslation;
import org.apache.beam.runners.core.construction.SplittableParDo;
import org.apache.beam.runners.core.construction.SplittableParDoNaiveBounded;
import org.apache.beam.runners.core.construction.resources.PipelineResources;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsValidator;
import org.apache.beam.sdk.runners.PTransformOverride;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

/**
 * A {@link PipelineRunner} that executes the operations in the pipeline by first translating them
 * to a Twister2 Plan and then executing them either locally or on a Twister2 cluster, depending on
 * the configuration.
 */
public class Twister2Runner extends PipelineRunner<PipelineResult> {

  private static final Logger LOG = Logger.getLogger(Twister2Runner.class.getName());
  private static final String SIDEINPUTS = "sideInputs";
  private static final String LEAVES = "leaves";
  private static final String GRAPH = "graph";
  /** Provided options. */
  private final Twister2PipelineOptions options;

  protected Twister2Runner(Twister2PipelineOptions options) {
    this.options = options;
  }

  public static Twister2Runner fromOptions(PipelineOptions options) {
    Twister2PipelineOptions pipelineOptions =
        PipelineOptionsValidator.validate(Twister2PipelineOptions.class, options);
    if (pipelineOptions.getFilesToStage() == null) {
      pipelineOptions.setFilesToStage(
          detectClassPathResourcesToStage(Twister2Runner.class.getClassLoader(), pipelineOptions));
      LOG.info(
          "PipelineOptions.filesToStage was not specified. "
              + "Defaulting to files from the classpath: will stage {} files. "
              + "Enable logging at DEBUG level to see which files will be staged"
              + pipelineOptions.getFilesToStage().size());
    }
    return new Twister2Runner(pipelineOptions);
  }

  @Override
  public PipelineResult run(Pipeline pipeline) {
    // create a worker and pass in the pipeline and then do the translation
    Twister2PipelineExecutionEnvironment env = new Twister2PipelineExecutionEnvironment(options);
    LOG.info("Translating pipeline to Twister2 program.");
    pipeline.replaceAll(getDefaultOverrides());
    env.translate(pipeline);
    setupSystem(options);

    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    JobConfig jobConfig = new JobConfig();
    jobConfig.put(SIDEINPUTS, extractNames(env.getSideInputs()));
    jobConfig.put(LEAVES, extractNames(env.getLeaves()));
    jobConfig.put(GRAPH, env.getTSetGraph());

    int workers = options.getParallelism();
    Twister2Job twister2Job =
        Twister2Job.newBuilder()
            .setJobName(options.getJobName())
            .setWorkerClass(BeamBatchWorker.class)
            .addComputeResource(options.getWorkerCPUs(), options.getRamMegaBytes(), workers)
            .setConfig(jobConfig)
            .build();
    Twister2JobState jobState = Twister2Submitter.submitJob(twister2Job, config);

    Twister2PipelineResult result = new Twister2PipelineResult();
    // TODO: Need to fix the check for "RUNNING" once fix for this is done on Twister2 end.
    if (jobState.getJobstate() == DriverJobState.FAILED
        || jobState.getJobstate() == DriverJobState.RUNNING) {
      throw new RuntimeException("Pipeline execution failed", jobState.getCause());
    } else {
      result.setState(PipelineResult.State.DONE);
    }
    return result;
  }

  public PipelineResult runTest(Pipeline pipeline) {
    // create a worker and pass in the pipeline and then do the translation
    Twister2PipelineExecutionEnvironment env = new Twister2PipelineExecutionEnvironment(options);
    LOG.info("Translating pipeline to Twister2 program.");
    pipeline.replaceAll(getDefaultOverrides());
    env.translate(pipeline);
    setupSystemTest(options);
    Map configMap = new HashMap();
    configMap.put(SIDEINPUTS, extractNames(env.getSideInputs()));
    configMap.put(LEAVES, extractNames(env.getLeaves()));
    configMap.put(GRAPH, env.getTSetGraph());
    configMap.put("twister2.network.buffer.size", 32000);
    configMap.put("twister2.network.sendBuffer.count", 1);
    Config config = ResourceAllocator.loadConfig(configMap);

    JobConfig jobConfig = new JobConfig();

    int workers = options.getParallelism();
    Twister2Job twister2Job =
        Twister2Job.newBuilder()
            .setJobName(options.getJobName())
            .setWorkerClass(BeamBatchWorker.class)
            .addComputeResource(options.getWorkerCPUs(), options.getRamMegaBytes(), workers)
            .setConfig(jobConfig)
            .build();
    Twister2JobState jobState = LocalSubmitter.submitJob(twister2Job, config);

    Twister2PipelineResult result = new Twister2PipelineResult();
    // TODO: Need to fix the check for "RUNNING" once fix for this is done on Twister2 end.
    if (jobState.getJobstate() == DriverJobState.FAILED
        || jobState.getJobstate() == DriverJobState.RUNNING) {
      throw new RuntimeException("Pipeline execution failed", jobState.getCause());
    } else {
      result.setState(PipelineResult.State.DONE);
    }
    return result;
  }

  private void setupSystem(Twister2PipelineOptions options) {
    prepareFilesToStage(options);
    zipFilesToStage(options);
    System.setProperty("cluster_type", options.getClusterType());
    System.setProperty("twister2_home", options.getTwister2Home());
    System.setProperty("job_file", options.getJobFileZip());
    System.setProperty("job_type", options.getJobType());
    // do a simple config dir validation
    System.setProperty("config_dir", options.getTwister2Home() + "/conf/");
    File cDir = new File(System.getProperty("config_dir"), options.getClusterType());

    String[] filesList =
        new String[] {
          "core.yaml", "network.yaml", "data.yaml", "resource.yaml", "task.yaml",
        };

    for (String file : filesList) {
      File toCheck = new File(cDir, file);
      if (!toCheck.exists()) {
        throw new Twister2RuntimeException(
            "Couldn't find " + file + " in config directory specified.");
      }
    }

    // setup logging
    FileInputStream fis = null;
    try {
      fis = new FileInputStream(new File(cDir, "logger.properties"));
      LogManager.getLogManager().readConfiguration(fis);
      fis.close();
    } catch (IOException e) {
      LOG.warning("Couldn't load logging configuration");
    } finally {
      if (fis != null) {
        try {
          fis.close();
        } catch (IOException e) {
          LOG.info(e.getMessage());
        }
      }
    }
  }

  private void setupSystemTest(Twister2PipelineOptions options) {
    prepareFilesToStage(options);
    zipFilesToStage(options);
    System.setProperty("job_file", options.getJobFileZip());
    System.setProperty("job_type", options.getJobType());
  }

  private Set<String> extractNames(Set<TSet> leaves) {
    Set<String> results = new HashSet<>();
    for (TSet leaf : leaves) {
      results.add(leaf.getId());
    }
    return results;
  }

  private Map<String, String> extractNames(Map<String, BatchTSet<?>> sideInputs) {
    Map<String, String> results = new LinkedHashMap<>();
    for (Map.Entry<String, BatchTSet<?>> entry : sideInputs.entrySet()) {
      results.put(entry.getKey(), entry.getValue().getId());
    }
    return results;
  }

  /**
   * Classpath contains non jar files (eg. directories with .class files or empty directories) will
   * cause exception in running log.
   */
  private void prepareFilesToStage(Twister2PipelineOptions options) {
    List<String> filesToStage =
        options.getFilesToStage().stream()
            .map(File::new)
            .filter(File::exists)
            .map(
                file -> {
                  return file.getAbsolutePath();
                })
            .collect(Collectors.toList());
    options.setFilesToStage(
        PipelineResources.prepareFilesForStaging(
            filesToStage,
            MoreObjects.firstNonNull(
                options.getTempLocation(), System.getProperty("java.io.tmpdir"))));
  }

  /**
   * creates a single zip file from all the jar files that are listed as files to stage in options.
   *
   * @param options
   */
  private void zipFilesToStage(Twister2PipelineOptions options) {
    File zipFile = null;
    Set<String> jarSet = new HashSet<>();
    // TODO figure out if we can remove all the dependencies that come with
    // the twister2 jars as well since they will be anyway provided from the
    // system we do not need to pack them
    List<String> filesToStage = options.getFilesToStage();
    List<String> trimmed = new ArrayList<>();
    // remove twister2 jars from the list
    for (String file : filesToStage) {
      if (!file.contains("/org/twister2")) {
        trimmed.add(file);
      }
    }

    FileInputStream fis = null;
    try {
      zipFile = File.createTempFile("twister2-", ".zip");
      FileOutputStream fos = new FileOutputStream(zipFile);
      ZipOutputStream zipOut = new ZipOutputStream(fos);
      zipOut.putNextEntry(new ZipEntry("lib/"));

      for (String srcFile : trimmed) {
        File fileToZip = new File(srcFile);
        if (!jarSet.contains(fileToZip.getName())) {
          jarSet.add(fileToZip.getName());
        } else {
          continue;
        }

        fis = new FileInputStream(fileToZip);
        ZipEntry zipEntry = new ZipEntry("lib/" + fileToZip.getName());
        zipOut.putNextEntry(zipEntry);

        byte[] bytes = new byte[1024];
        int length;
        while ((length = fis.read(bytes)) >= 0) {
          zipOut.write(bytes, 0, length);
        }
        fis.close();
      }
      zipOut.close();
      fos.close();
      zipFile.deleteOnExit();
    } catch (FileNotFoundException e) {
      LOG.info(e.getMessage());
    } catch (IOException e) {
      LOG.info(e.getMessage());
    } finally {
      if (fis != null) {
        try {
          fis.close();
        } catch (IOException e) {
          LOG.info(e.getMessage());
        }
      }
    }
    if (zipFile != null) {
      options.setJobFileZip(zipFile.getPath());
    }
  }

  private static List<PTransformOverride> getDefaultOverrides() {
    List<PTransformOverride> overrides =
        ImmutableList.<PTransformOverride>builder()
            .add(
                PTransformOverride.of(
                    PTransformMatchers.splittableParDo(), new SplittableParDo.OverrideFactory()))
            .add(
                PTransformOverride.of(
                    PTransformMatchers.urnEqualTo(
                        PTransformTranslation.SPLITTABLE_PROCESS_KEYED_URN),
                    new SplittableParDoNaiveBounded.OverrideFactory()))
            .build();

    return overrides;
  }
}
