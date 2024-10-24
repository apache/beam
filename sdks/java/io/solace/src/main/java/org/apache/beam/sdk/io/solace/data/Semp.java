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
package org.apache.beam.sdk.io.solace.data;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.value.AutoValue;

public class Semp {

  @AutoValue
  @JsonSerialize(as = Queue.class)
  @JsonDeserialize(builder = AutoValue_Semp_Queue.Builder.class)
  public abstract static class Queue {

    public abstract QueueData data();

    public static Builder builder() {
      return new AutoValue_Semp_Queue.Builder();
    }

    public abstract Builder toBuilder();

    @AutoValue.Builder
    @JsonPOJOBuilder(withPrefix = "set")
    abstract static class Builder {

      public abstract Builder setData(QueueData queueData);

      public abstract Queue build();
    }
  }

  @AutoValue
  @JsonDeserialize(builder = AutoValue_Semp_QueueData.Builder.class)
  public abstract static class QueueData {
    public abstract String accessType();

    public abstract long msgSpoolUsage();

    public static Builder builder() {
      return new AutoValue_Semp_QueueData.Builder();
    }

    public abstract Builder toBuilder();

    @AutoValue.Builder
    @JsonPOJOBuilder(withPrefix = "set")
    abstract static class Builder {

      public abstract Builder setAccessType(String accessType);

      public abstract Builder setMsgSpoolUsage(long msgSpoolUsage);

      public abstract QueueData build();
    }
  }
}
