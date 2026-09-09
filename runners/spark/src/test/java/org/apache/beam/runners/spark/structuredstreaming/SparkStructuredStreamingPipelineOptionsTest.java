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
package org.apache.beam.runners.spark.structuredstreaming;

import static org.junit.Assert.assertEquals;

import org.apache.beam.runners.spark.SparkPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link SparkStructuredStreamingPipelineOptions}. */
@RunWith(JUnit4.class)
public class SparkStructuredStreamingPipelineOptionsTest {

  /**
   * {@code maxRecordsPerBatch} is declared by both {@link SparkPipelineOptions} and {@link
   * SparkStructuredStreamingPipelineOptions} with identical signatures, so one options proxy serves
   * both and the flag carries over when migrating between the runners.
   */
  @Test
  public void maxRecordsPerBatchIsSharedWithLegacyOptions() {
    PipelineOptions options = PipelineOptionsFactory.create();

    SparkStructuredStreamingPipelineOptions streamingOptions =
        options.as(SparkStructuredStreamingPipelineOptions.class);
    assertEquals(Long.valueOf(-1), streamingOptions.getMaxRecordsPerBatch());

    streamingOptions.setMaxRecordsPerBatch(500L);
    assertEquals(Long.valueOf(500), options.as(SparkPipelineOptions.class).getMaxRecordsPerBatch());
  }

  @Test
  public void maxRecordsPerBatchIsParsedFromArgs() {
    SparkStructuredStreamingPipelineOptions options =
        PipelineOptionsFactory.fromArgs("--maxRecordsPerBatch=42")
            .as(SparkStructuredStreamingPipelineOptions.class);
    assertEquals(Long.valueOf(42), options.getMaxRecordsPerBatch());
  }
}
