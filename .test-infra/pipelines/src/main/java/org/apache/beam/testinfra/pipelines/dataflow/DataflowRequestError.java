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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.google.auto.value.AutoValue;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCaseFormat;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.CaseFormat;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Stores errors related to Dataflow API request executions. */
@Internal
@DefaultSchema(AutoValueSchema.class)
@AutoValue
@SchemaCaseFormat(CaseFormat.LOWER_UNDERSCORE)
public abstract class DataflowRequestError {

  private static final Logger LOG = LoggerFactory.getLogger(DataflowRequestError.class);

  private static final AutoValueSchema SCHEMA_PROVIDER = new AutoValueSchema();

  private static final TypeDescriptor<DataflowRequestError> TYPE =
      TypeDescriptor.of(DataflowRequestError.class);

  public static final Schema SCHEMA = checkStateNotNull(SCHEMA_PROVIDER.schemaFor(TYPE));

  public static final SerializableFunction<DataflowRequestError, Row> TO_ROW_FN =
      SCHEMA_PROVIDER.toRowFunction(TYPE);

  public static Builder builder() {
    return new AutoValue_DataflowRequestError.Builder();
  }

  public static <RequestT extends GeneratedMessageV3> Builder fromRequest(
      RequestT request, Class<RequestT> clazz) {
    Builder builder = builder();
    try {
      String json = JsonFormat.printer().omittingInsignificantWhitespace().print(request);
      builder = builder.setRequest(json);
    } catch (InvalidProtocolBufferException e) {
      LOG.warn("error converting {} to json: {}", clazz, e.getMessage());
    }
    return builder;
  }

  public abstract Instant getObservedTime();

  public abstract String getRequest();

  public abstract String getMessage();

  public abstract String getStackTrace();

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setObservedTime(Instant value);

    public abstract Builder setRequest(String value);

    public abstract Builder setMessage(String value);

    public abstract Builder setStackTrace(String value);

    public abstract DataflowRequestError build();
  }
}
