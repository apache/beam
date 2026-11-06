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
package org.apache.beam.sdk.extensions.openlineage;

import com.fasterxml.jackson.core.type.TypeReference;
import io.openlineage.client.DefaultConfigPathProvider;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.job.JobConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves the effective {@link BeamOpenLineageConfig} for a pipeline, mirroring the Spark
 * integration's {@code ArgumentParser}: pipeline options take precedence over {@code
 * openlineage.yml}, which takes precedence over {@code OPENLINEAGE__} environment variables.
 */
class BeamOpenLineageConfigParser {

  private static final Logger LOG = LoggerFactory.getLogger(BeamOpenLineageConfigParser.class);
  private static final TypeReference<BeamOpenLineageConfig> TYPE_REFERENCE =
      new TypeReference<BeamOpenLineageConfig>() {};

  private BeamOpenLineageConfigParser() {}

  static BeamOpenLineageConfig parse(OpenLineagePipelineOptions options) {
    BeamOpenLineageConfig fromEnv = loadFromEnvVars();
    BeamOpenLineageConfig fromFile = loadFromFile();
    BeamOpenLineageConfig fromOptions = fromOptions(options);
    return fromEnv.mergeWith(fromFile).mergeWith(fromOptions);
  }

  private static BeamOpenLineageConfig loadFromFile() {
    try {
      return OpenLineageClientUtils.loadOpenLineageConfigYaml(
          new DefaultConfigPathProvider(), TYPE_REFERENCE);
    } catch (RuntimeException e) {
      LOG.debug("No openlineage.yml configuration found", e);
      return new BeamOpenLineageConfig();
    }
  }

  private static BeamOpenLineageConfig loadFromEnvVars() {
    try {
      return OpenLineageClientUtils.loadOpenLineageConfigFromEnvVars(TYPE_REFERENCE);
    } catch (RuntimeException e) {
      LOG.debug("No OPENLINEAGE__ environment configuration found", e);
      return new BeamOpenLineageConfig();
    }
  }

  private static BeamOpenLineageConfig fromOptions(OpenLineagePipelineOptions options) {
    BeamOpenLineageConfig config = new BeamOpenLineageConfig();
    String namespace = options.getOpenLineageNamespace();
    String jobName = options.getOpenLineageJobName();
    if (namespace != null || jobName != null) {
      JobConfig jobConfig = new JobConfig();
      if (namespace != null) {
        jobConfig.setNamespace(namespace);
      }
      if (jobName != null) {
        jobConfig.setName(jobName);
      }
      config.setJobConfig(jobConfig);
    }
    config.setTrackingIntervalInSeconds(options.getOpenLineageTrackingIntervalInSeconds());
    return config;
  }
}
