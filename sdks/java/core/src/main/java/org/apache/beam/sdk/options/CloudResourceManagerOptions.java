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

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.beam.sdk.util.GcpProjectUtil;

/**
 * Properties needed when using Google CloudResourceManager with the Apache Beam SDK.
 */
@Description("Options that are used to configure Google CloudResourceManager. See "
    + "https://cloud.google.com/resource-manager/ for details on CloudResourceManager.")
public interface CloudResourceManagerOptions extends ApplicationNameOptions, GcpOptions,
    PipelineOptions, StreamingOptions {
  /**
   * The GcpProjectUtil instance that should be used to communicate with Google Cloud Storage.
   */
  @JsonIgnore
  @Description("The GcpProjectUtil instance that should be used to communicate"
               + " with Google Cloud Resource Manager.")
  @Default.InstanceFactory(GcpProjectUtil.GcpProjectUtilFactory.class)
  @Hidden
  GcpProjectUtil getGcpProjectUtil();
  void setGcpProjectUtil(GcpProjectUtil value);
}
