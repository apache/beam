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
package org.apache.beam.sdk.options;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;

/**
 * Apache Beam provides a number of experimental features that can
 * be enabled with this flag. If executing against a managed service, please contact the
 * service owners before enabling any experiments.
 */
@Experimental
@Hidden
public interface ExperimentalOptions extends PipelineOptions {
  @Description("[Experimental] Apache Beam provides a number of experimental features that can "
      + "be enabled with this flag. If executing against a managed service, please contact the "
      + "service owners before enabling any experiments.")
  @Nullable
  List<String> getExperiments();
  void setExperiments(@Nullable List<String> value);

  /**
   * Returns true iff the provided pipeline options has the specified experiment
   * enabled.
   */
  static boolean hasExperiment(PipelineOptions options, String experiment) {
    if (options == null) {
      return false;
    }

    List<String> experiments = options.as(ExperimentalOptions.class).getExperiments();
    return experiments != null
        && experiments.contains(experiment);
  }
}
