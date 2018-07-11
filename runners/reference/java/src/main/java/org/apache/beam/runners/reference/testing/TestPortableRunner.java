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
package org.apache.beam.runners.reference.testing;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import org.apache.beam.runners.reference.PortableRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link TestPortableRunner} is a pipeline runner that wraps a {@link PortableRunner} when running
 * tests against the {@link TestPipeline}.
 *
 * @see TestPipeline
 */
public class TestPortableRunner extends PipelineRunner<PipelineResult> {
  private static final Logger LOG = LoggerFactory.getLogger(TestPortableRunner.class);
  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .registerModules(ObjectMapper.findModules(ReflectHelpers.findClassLoader()));
  private final PortablePipelineOptions options;

  private TestPortableRunner(PortablePipelineOptions options) {
    this.options = options;
  }

  public static TestPortableRunner fromOptions(PipelineOptions options) {
    return new TestPortableRunner(options.as(PortablePipelineOptions.class));
  }

  @Override
  public PipelineResult run(Pipeline pipeline) {
    TestPortablePipelineOptions testPortablePipelineOptions =
        options.as(TestPortablePipelineOptions.class);
    String jobServerHost = "localhost:8099";
    Object jobServerDriver;
    Class<?> jobServerDriverClass;
    try {
      jobServerDriverClass =
          Class.forName(
              testPortablePipelineOptions.getJobServerDriver(),
              true,
              ReflectHelpers.findClassLoader());
      String[] parameters;
      try {
        parameters =
            MAPPER.readValue(testPortablePipelineOptions.getJobServerConfig(), String[].class);
      } catch (IOException e) {
        throw new IllegalArgumentException(
            String.format(
                "Arguments to jobServers should be a comma separated list of strings. Provided %s",
                testPortablePipelineOptions.getJobServerDriver()));
      }
      try {
        jobServerDriver =
            jobServerDriverClass
                .getMethod("fromHostAndParams", String.class, String[].class)
                .invoke(null, jobServerHost, parameters);
        jobServerDriverClass.getMethod("start").invoke(jobServerDriver);
      } catch (IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
        throw new IllegalArgumentException(e);
      }
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(
          String.format(
              "Unknown 'TestJobServerDriver' specified '%s'",
              testPortablePipelineOptions.getJobServerDriver()),
          e);
    }
    try {
      PortablePipelineOptions portableOptions = options.as(PortablePipelineOptions.class);
      portableOptions.setRunner(PortableRunner.class);
      portableOptions.setJobEndpoint(jobServerHost);
      PortableRunner runner = PortableRunner.fromOptions(portableOptions);
      PipelineResult result = runner.run(pipeline);
      result.waitUntilFinish();
      return result;
    } finally {
      try {
        jobServerDriverClass.getMethod("stop").invoke(jobServerDriver);
      } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
        LOG.error(
            String.format(
                "Provided JobServiceDriver %s does not implement stop().", jobServerDriverClass),
            e);
      }
    }
  }
}
