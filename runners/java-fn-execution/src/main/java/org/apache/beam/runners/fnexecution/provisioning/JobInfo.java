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
package org.apache.beam.runners.fnexecution.provisioning;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.apache.beam.model.fnexecution.v1.ProvisionApi;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.Struct;

/**
 * A subset of {@link org.apache.beam.model.fnexecution.v1.ProvisionApi.ProvisionInfo} that
 * specifies a unique job, while omitting fields that are not known to the runner operator.
 */
@AutoValue
public abstract class JobInfo implements Serializable {
  public static JobInfo create(
      String jobId, String jobName, String retrievalToken, Struct pipelineOptions) {
    return new AutoValue_JobInfo(jobId, jobName, retrievalToken, pipelineOptions);
  }

  public abstract String jobId();

  public abstract String jobName();

  public abstract String retrievalToken();

  public abstract Struct pipelineOptions();

  public ProvisionApi.ProvisionInfo toProvisionInfo() {
    return ProvisionApi.ProvisionInfo.newBuilder()
        .setRetrievalToken(retrievalToken())
        .setPipelineOptions(pipelineOptions())
        .build();
  }
}
