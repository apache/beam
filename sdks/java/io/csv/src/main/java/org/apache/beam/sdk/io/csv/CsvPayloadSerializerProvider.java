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
package org.apache.beam.sdk.io.csv;

import com.google.auto.service.AutoService;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializer;
import org.apache.beam.sdk.schemas.io.payloads.PayloadSerializerProvider;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.commons.csv.CSVFormat;

/** {@link PayloadSerializerProvider} implementation supporting CSV. */
@AutoService(PayloadSerializerProvider.class)
public class CsvPayloadSerializerProvider implements PayloadSerializerProvider {
  public static final String CSV_FORMAT_PARAMETER_KEY = "csv_format";
  private static final Set<String> ALLOWED_PARAMETERS = ImmutableSet.of(CSV_FORMAT_PARAMETER_KEY);

  @Override
  public String identifier() {
    return "csv";
  }

  @Override
  public PayloadSerializer getSerializer(Schema schema, Map<String, Object> params) {
    Set<String> misMatchedParameters = new HashSet<>();
    for (String param : params.keySet()) {
      if (!ALLOWED_PARAMETERS.contains(param)) {
        misMatchedParameters.add(param);
      }
    }
    if (!misMatchedParameters.isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "illegal parameters for %s: %s",
              CsvPayloadSerializerProvider.class.getName(),
              String.join(",", misMatchedParameters)));
    }
    CSVFormat csvFormat = CSVFormat.DEFAULT;
    if (params.containsKey(CSV_FORMAT_PARAMETER_KEY)) {
      csvFormat = (CSVFormat) params.get(CSV_FORMAT_PARAMETER_KEY);
    }
    return new CsvPayloadSerializer(schema, csvFormat);
  }
}
