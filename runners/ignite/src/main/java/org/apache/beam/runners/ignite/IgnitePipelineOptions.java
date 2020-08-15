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
package org.apache.beam.runners.ignite;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

/** Pipeline options specific to the Ignite runner. */
public interface IgnitePipelineOptions extends PipelineOptions {

  @Description("Name of Ignite group")
  @Validation.Required
  @Default.String("ignite")
  String getIgniteGroupName();

  void setIgniteGroupName(String igniteGroupName);

  @Description("Specifies the addresses of the Ignite cluster; needed only with external clusters")
  @Validation.Required
  @Default.String("127.0.0.1:47500")
  String getIgniteServers();

  void setIgniteServers(String igniteServers);

  @Description(
      "Specifies where the fat-jar containing all the code is located; needed only with external clusters")
  String getCodeJarPathname();

  void setCodeJarPathname(String codeJarPathname);

  @Description("Local parallelism of Ignite nodes")
  @Validation.Required
  @Default.Integer(2)
  Integer getIgniteDefaultParallelism();

  void setIgniteDefaultParallelism(Integer localParallelism);

  @Description("Number of locally started Ignite Cluster Members")
  @Validation.Required
  @Default.Integer(0)
  Integer getIgniteLocalMode();

  void setIgniteLocalMode(Integer noOfLocalClusterMembers);

  @Description("Weather Ignite Processors for DoFns should use green threads or not")
  @Validation.Required
  @Default.Boolean(false)
  Boolean getIgniteProcessorsCooperative();

  void setIgniteProcessorsCooperative(Boolean cooperative);
}
