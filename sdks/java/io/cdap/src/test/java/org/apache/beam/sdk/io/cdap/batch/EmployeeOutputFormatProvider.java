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
package org.apache.beam.sdk.io.cdap.batch;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import java.util.Map;
import org.apache.beam.sdk.io.cdap.CdapIO;
import org.apache.beam.sdk.io.cdap.EmployeeConfig;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;

/**
 * {@link OutputFormatProvider} for {@link EmployeeBatchSink} CDAP plugin. Used to test {@link
 * CdapIO#write()}.
 */
public class EmployeeOutputFormatProvider implements OutputFormatProvider {

  public static final String PROPERTY_CONFIG_JSON = "cdap.employee.config";
  public static final Gson GSON = new GsonBuilder().create();

  private final Map<String, String> configMap;

  EmployeeOutputFormatProvider(EmployeeConfig config) {
    this.configMap =
        new ImmutableMap.Builder<String, String>()
            .put(PROPERTY_CONFIG_JSON, GSON.toJson(config))
            .build();
  }

  @Override
  public String getOutputFormatClassName() {
    return EmployeeOutputFormat.class.getName();
  }

  @Override
  public Map<String, String> getOutputFormatConfiguration() {
    return configMap;
  }
}
