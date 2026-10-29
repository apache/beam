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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import io.openlineage.client.job.JobConfig;
import io.openlineage.client.transports.ConsoleConfig;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link BeamOpenLineageConfigParser} and {@link BeamOpenLineageConfig} merging. */
@RunWith(JUnit4.class)
public class BeamOpenLineageConfigParserTest {

  @Test
  public void testOptionsProvideJobIdentity() {
    OpenLineagePipelineOptions options =
        PipelineOptionsFactory.as(OpenLineagePipelineOptions.class);
    options.setOpenLineageNamespace("my_namespace");
    options.setOpenLineageJobName("my_job");
    options.setOpenLineageTrackingIntervalInSeconds(30);

    BeamOpenLineageConfig config = BeamOpenLineageConfigParser.parse(options);
    assertNotNull(config.getJobConfig());
    assertEquals("my_namespace", config.getJobConfig().getNamespace());
    assertEquals("my_job", config.getJobConfig().getName());
    assertEquals(Integer.valueOf(30), config.getTrackingIntervalInSeconds());
  }

  @Test
  public void testOptionsWinOverBaseConfigOnMerge() {
    BeamOpenLineageConfig base = new BeamOpenLineageConfig();
    JobConfig baseJob = new JobConfig();
    baseJob.setNamespace("from_file");
    baseJob.setName("file_job");
    base.setJobConfig(baseJob);
    base.setTransportConfig(new ConsoleConfig());
    base.setTrackingIntervalInSeconds(120);

    BeamOpenLineageConfig overrides = new BeamOpenLineageConfig();
    JobConfig overrideJob = new JobConfig();
    overrideJob.setNamespace("from_options");
    overrides.setJobConfig(overrideJob);

    BeamOpenLineageConfig merged = base.mergeWith(overrides);
    // The override's non-null values win; everything else survives from the base.
    assertNotNull(merged.getJobConfig());
    assertEquals("from_options", merged.getJobConfig().getNamespace());
    assertEquals("file_job", merged.getJobConfig().getName());
    assertEquals(Integer.valueOf(120), merged.getTrackingIntervalInSeconds());
    assertNotNull(merged.getTransportConfig());
  }

  @Test
  public void testEmptyOptionsYieldEmptyJobConfig() {
    OpenLineagePipelineOptions options =
        PipelineOptionsFactory.as(OpenLineagePipelineOptions.class);
    BeamOpenLineageConfig config = BeamOpenLineageConfigParser.parse(options);
    assertNull(config.getTrackingIntervalInSeconds());
  }
}
