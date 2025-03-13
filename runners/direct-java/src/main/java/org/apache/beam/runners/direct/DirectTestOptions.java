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
package org.apache.beam.runners.direct;

import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Hidden;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Internal-only options for tweaking the behavior of the {@link DirectRunner} in ways that users
 * should never do.
 *
 * <p>Currently, the only use is to disable user-friendly overrides that prevent fully testing
 * certain composite transforms.
 */
@Hidden
public interface DirectTestOptions extends PipelineOptions, ApplicationNameOptions {
  @Default.Boolean(true)
  @Description("Indicates whether this is an automatically-run unit test.")
  boolean isRunnerDeterminedSharding();

  void setRunnerDeterminedSharding(boolean goAheadAndDetermineSharding);
}
