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
package org.apache.beam.sdk.io.aws2.sqs.providers;

import com.google.auto.value.AutoValue;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

/** Configuration class for reading data from an AWS SQS queue. */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class SqsReadConfiguration {

  public static Builder builder() {
    return new AutoValue_SqsReadConfiguration.Builder()
        .setMaxReadTimeSecs(null)
        .setMaxNumRecords(Long.MAX_VALUE);
  }

  public abstract String getQueueUrl();

  @Nullable
  public abstract Long getMaxReadTimeSecs();

  @Nullable
  public abstract Long getMaxNumRecords();

  public long maxNumRecords() {
    return Optional.ofNullable(getMaxNumRecords()).orElse(Long.MAX_VALUE);
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setQueueUrl(String queueUrl);

    public abstract Builder setMaxReadTimeSecs(@Nullable Long maxReadTimeSecs);

    public abstract Builder setMaxNumRecords(Long maxNumRecords);

    public abstract SqsReadConfiguration build();
  }
}
