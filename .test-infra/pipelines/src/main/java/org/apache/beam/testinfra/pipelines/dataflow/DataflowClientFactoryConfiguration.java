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
package org.apache.beam.testinfra.pipelines.dataflow;

import com.google.auth.Credentials;
import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;

/** Configures the Dataflow API client. */
@Internal
@AutoValue
public abstract class DataflowClientFactoryConfiguration implements Serializable {

  public static Builder builder(DataflowJobsOptions options) {
    Credentials credentials = credentialsFrom(options);
    return new AutoValue_DataflowClientFactoryConfiguration.Builder()
        .setCredentials(credentials)
        .setDataflowTarget(options.getDataflowTarget());
  }

  static Credentials credentialsFrom(DataflowJobsOptions options) {
    return options.as(GcpOptions.class).getGcpCredential();
  }

  abstract Credentials getCredentials();

  abstract String getDataflowTarget();

  @AutoValue.Builder
  public abstract static class Builder {
    abstract Builder setCredentials(Credentials newCredentials);

    abstract Builder setDataflowTarget(String newDataflowRootUrl);

    public abstract DataflowClientFactoryConfiguration build();
  }
}
