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
package org.apache.beam.runners.gearpump;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.gearpump.cluster.client.ClientContext;
import java.util.Map;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/** Options that configure the Gearpump pipeline. */
public interface GearpumpPipelineOptions extends PipelineOptions {

  @Description("set unique application name for Gearpump runner")
  void setApplicationName(String name);

  String getApplicationName();

  @Description("set parallelism for Gearpump processor")
  void setParallelism(int parallelism);

  @Default.Integer(1)
  int getParallelism();

  @Description("register Kryo serializers")
  void setSerializers(Map<String, String> serializers);

  @JsonIgnore
  Map<String, String> getSerializers();

  @Description(
      "Whether the pipeline will be run on a remote cluster. If false, it will be run on a EmbeddedCluster")
  void setRemote(Boolean remote);

  @Default.Boolean(false)
  Boolean getRemote();

  void setClientContext(ClientContext clientContext);

  @JsonIgnore
  @Description("get client context to query application status")
  ClientContext getClientContext();
}
