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
package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.runners.spark.structuredstreaming.SparkPipelineOptions;
import org.apache.beam.runners.spark.structuredstreaming.translation.TranslationContext;
import org.apache.beam.sdk.values.PValue;
import org.apache.spark.sql.Dataset;

/** This class contains only batch specific context components. */
public class BatchTranslationContext extends TranslationContext {

  /**
   * For keeping track about which DataSets don't have a successor. We need to terminate these with
   * a discarding sink because the Beam model allows dangling operations.
   */
  private final Map<PValue, Dataset<?>> danglingDataSets;

  public BatchTranslationContext(SparkPipelineOptions options) {
    super(options);
    this.danglingDataSets = new HashMap<>();
  }
}
