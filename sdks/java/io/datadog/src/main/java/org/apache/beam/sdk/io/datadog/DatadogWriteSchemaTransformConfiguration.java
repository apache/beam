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
package org.apache.beam.sdk.io.datadog;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.io.payloads.AutoValuePayloads.AutoValueSchema;

@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class DatadogWriteSchemaTransformConfiguration {

  public void validate() {}

  public static Builder builder() {
    return new AutoValue_DatadogWriteSchemaTransformConfiguration.Builder();
  }

  @SchemaFieldDescription("The Datadog API URL.")
  public abstract String getUrl();

  @SchemaFieldDescription("The Datadog API key.")
  public abstract String getApiKey();

  @SchemaFieldDescription("The number of events to batch together for each write.")
  @Nullable
  public abstract Integer getBatchCount();

  @SchemaFieldDescription("The maximum buffer size in bytes.")
  @Nullable
  public abstract Long getMaxBufferSize();

  @SchemaFieldDescription("The degree of parallelism for writing.")
  @Nullable
  public abstract Integer getParallelism();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setUrl(String url);

    public abstract Builder setApiKey(String apiKey);

    public abstract Builder setBatchCount(Integer batchCount);

    public abstract Builder setMaxBufferSize(Long maxBufferSize);

    public abstract Builder setParallelism(Integer parallelism);

    public abstract DatadogWriteSchemaTransformConfiguration build();
  }
}
