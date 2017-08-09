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
package org.apache.beam.sdk.extensions.sql;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.schema.BeamSqlUdf;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.transforms.SerializableFunction;

/**
 * {@link BeamSqlEnv} prepares the execution context for {@link BeamSql} and {@link BeamSqlCli}.
 */
public class BeamSqlEnv implements Serializable {
  private Map<String, Class<? extends BeamSqlUdf>> udfs = new HashMap<>();
  private Map<String, CombineFn> udafs = new HashMap<>();
  private Map<String, SerializableFunction> sfnUdfs = new HashMap<>();

  /**
   * Register a UDF function which can be used in SQL expression.
   */
  public synchronized void registerUdf(String functionName, Class<? extends BeamSqlUdf> clazz) {
    validateUdfNameUniqueness(functionName);
    udfs.put(functionName, clazz);
  }

  /**
   * register {@link SerializableFunction} as a UDF function which can be used in SQL expression.
   * Note, {@link SerializableFunction} must have a constructor without arguments.
   */
  public synchronized void registerUdf(String functionName, SerializableFunction sfn) {
    validateUdfNameUniqueness(functionName);
    sfnUdfs.put(functionName, sfn);
  }

  /**
   * Register a {@link CombineFn} as UDAF function which can be used in GROUP-BY expression.
   */
  public void registerUdaf(String functionName, CombineFn combineFn) {
    validateUdfNameUniqueness(functionName);
    udafs.put(functionName, combineFn);
  }

  private void validateUdfNameUniqueness(String name) {
    if (udfs.containsKey(name) || udafs.containsKey(name) || sfnUdfs.containsKey(name)) {
      throw new IllegalArgumentException("UDF named: " + name + " is already registered!");
    }
  }

  public Map<String, Class<? extends BeamSqlUdf>> getUdfs() {
    return udfs;
  }

  public Map<String, CombineFn> getUdafs() {
    return udafs;
  }

  public Map<String, SerializableFunction> getSfnUdfs() {
    return sfnUdfs;
  }
}
