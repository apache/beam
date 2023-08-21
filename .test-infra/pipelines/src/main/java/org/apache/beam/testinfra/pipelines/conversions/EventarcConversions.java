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
package org.apache.beam.testinfra.pipelines.conversions;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.events.cloud.dataflow.v1beta3.Job;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.util.Optional;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.joda.time.Instant;

/** Methods for converting from Eventarc JSON payloads. */
@Internal
public final class EventarcConversions {

  private static final String DATA_NODE_KEY = "data";
  private static final String TYPE_NODE_KEY = "@type";
  private static final String JOB_EVENT_DATA_TYPE =
      "type.googleapis.com/google.events.cloud.dataflow.v1beta3.JobEventData";
  private static final String PAYLOAD_NODE_KEY = "payload";

  /** Parses Eventarc JSON strings to {@link Job}s. */
  public static MapElements.MapWithFailures<String, Job, ConversionError> fromJson() {
    return MapElements.into(TypeDescriptor.of(Job.class))
        .via(new JsonToJobFn())
        .exceptionsInto(new TypeDescriptor<ConversionError>() {})
        .exceptionsVia(
            exceptionElement ->
                ConversionError.builder()
                    .setObservedTime(Instant.now())
                    .setMessage(
                        Optional.ofNullable(exceptionElement.exception().getMessage()).orElse(""))
                    .setStackTrace(Throwables.getStackTraceAsString(exceptionElement.exception()))
                    .build());
  }

  /** {@link SerializableFunction} that parses an Eventarc JSON string into a {@link Job}. */
  static class JsonToJobFn implements SerializableFunction<String, Job> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public Job apply(String json) {

      String safeJson = checkStateNotNull(json, "null json string input for %s", JsonToJobFn.class);

      Job.Builder builder = Job.newBuilder();

      try {

        JsonNode eventNode =
            checkStateNotNull(
                OBJECT_MAPPER.readTree(safeJson),
                "could not parse json input into %s",
                JsonNode.class);

        JsonNode dataNode =
            checkStateNotNull(
                eventNode.get(DATA_NODE_KEY), "json input missing path: $.%s", DATA_NODE_KEY);

        JsonNode typeNode =
            checkStateNotNull(
                dataNode.get(TYPE_NODE_KEY),
                "json input missing path: $.%s.%s",
                DATA_NODE_KEY,
                TYPE_NODE_KEY);

        JsonNode payloadNode =
            checkStateNotNull(
                dataNode.get(PAYLOAD_NODE_KEY),
                "json input missing path: $.%s.%s",
                DATA_NODE_KEY,
                PAYLOAD_NODE_KEY);

        checkState(
            typeNode.asText().equals(JOB_EVENT_DATA_TYPE),
            "expected %s=%s at json path: $.%s.%s, got: %s",
            TYPE_NODE_KEY,
            JOB_EVENT_DATA_TYPE,
            DATA_NODE_KEY,
            TYPE_NODE_KEY,
            typeNode.asText());

        JsonFormat.parser().merge(payloadNode.toString(), builder);
        return builder.build();

      } catch (InvalidProtocolBufferException | JsonProcessingException e) {
        throw new IllegalStateException(e);
      }
    }
  }
}
