/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.runners.inprocess;

import com.google.cloud.dataflow.sdk.options.ApplicationNameOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;

/**
 * Options that can be used to configure the {@link InProcessPipelineRunner}.
 */
public interface InProcessPipelineOptions extends PipelineOptions, ApplicationNameOptions {
  @Default.InstanceFactory(NanosOffsetClock.Factory.class)
  Clock getClock();

  void setClock(Clock clock);

  boolean isShutdownUnboundedProducersWithMaxWatermark();

  void setShutdownUnboundedProducersWithMaxWatermark(boolean shutdown);
}
