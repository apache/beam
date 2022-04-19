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
package org.apache.beam.sdk.io.snowflake;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.util.AbstractMap;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;

class MapUserTypeToJsonFn<T> extends DoFn<T, String> {
  // FAIL_ON_EMPTY_BEANS is disabled in order to handle null values in
  // TableRow.
  private static final ObjectMapper MAPPER =
      new ObjectMapper().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

  private final SerializableFunction<T, AbstractMap<String, Object>> jsonFormatFunction;

  public MapUserTypeToJsonFn(
      SerializableFunction<T, AbstractMap<String, Object>> jsonFormatFunction) {
    this.jsonFormatFunction = jsonFormatFunction;
  }

  @ProcessElement
  public void processElement(ProcessContext context) throws JsonProcessingException {
    T element = context.element();
    AbstractMap<String, Object> row = this.jsonFormatFunction.apply(element);
    String strValue = MAPPER.writeValueAsString(row);
    context.output(strValue);
  }
}
