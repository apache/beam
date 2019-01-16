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
package org.apache.beam.sdk.extensions.sql.meta.provider;

import java.util.Collections;
import java.util.Map;
import org.apache.beam.sdk.extensions.sql.BeamSqlUdf;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.SerializableFunction;

/** Provider for UDF and UDAF. */
public interface UdfUdafProvider {
  /** For UDFs implement {@link BeamSqlUdf}. */
  default Map<String, Class<? extends BeamSqlUdf>> getBeamSqlUdfs() {
    return Collections.emptyMap();
  }

  /** For UDFs implement {@link SerializableFunction}. */
  default Map<String, SerializableFunction<?, ?>> getSerializableFunctionUdfs() {
    return Collections.emptyMap();
  }

  default Map<String, Combine.CombineFn> getUdafs() {
    return Collections.emptyMap();
  }
}
