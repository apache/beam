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
package org.apache.beam.runners.samza;

import org.apache.samza.config.Config;
import org.apache.samza.context.ExternalContext;

/** Life cycle listener for a Samza pipeline during runtime. */
public interface SamzaPipelineLifeCycleListener {
  /** Callback when the pipeline options is created. */
  void onInit(Config config, SamzaPipelineOptions options);

  /** Callback when the pipeline is started. */
  ExternalContext onStart();

  /**
   * Callback after the pipeline is submmitted. This will be invoked only for Samza jobs submitted
   * to a cluster.
   */
  void onSubmit();

  /** Callback after the pipeline is finished. */
  void onFinish();

  /** A registrar for {@link SamzaPipelineLifeCycleListener}. */
  interface Registrar {
    SamzaPipelineLifeCycleListener getLifeCycleListener();
  }
}
