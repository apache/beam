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

package org.apache.beam.sdk.extensions.sql.impl.rel;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.impl.InnerBeamSqlEnv;
import org.apache.beam.sdk.values.BeamRecord;
import org.apache.beam.sdk.values.PCollection;

/**
 * Base class for rel test.
 */
public class BaseRelTest {
  public PCollection<BeamRecord> compilePipeline (
      String sql, Pipeline pipeline, InnerBeamSqlEnv sqlEnv) throws Exception {
    return sqlEnv.getPlanner().compileBeamPipeline(sql, pipeline, sqlEnv);
  }
}
