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

package org.apache.beam.runners.spark.translation.streaming.utils;

import java.io.IOException;
import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.joda.time.Duration;
import org.junit.rules.TemporaryFolder;


/**
 * A rule to create a common {@link SparkPipelineOptions} for testing streaming pipelines.
 */
public class SparkTestPipelineOptionsForStreaming extends SparkTestPipelineOptions {

  private static final int DEFAULT_NUMBER_OF_BATCHES_TIMEOUT = 5;

  public SparkPipelineOptions withTmpCheckpointDir(TemporaryFolder parent)
      throws IOException {
    // tests use JUnit's TemporaryFolder path in the form of: /.../junit/...
    options.setCheckpointDir(parent.newFolder(options.getJobName()).toURI().toURL().toString());
    options.setForceStreaming(true);
    // set the default timeout to DEFAULT_NUMBER_OF_BATCHES_TIMEOUT x batchDuration
    // to allow pipelines to finish.
    Duration batchDuration = Duration.millis(options.getBatchIntervalMillis());
    // set the checkpoint duration to match interval.
    options.setCheckpointDurationMillis(batchDuration.getMillis());
    long forcedTimeout = batchDuration.multipliedBy(DEFAULT_NUMBER_OF_BATCHES_TIMEOUT).getMillis();
    options.setForcedTimeout(forcedTimeout);
    return options;
  }
}
