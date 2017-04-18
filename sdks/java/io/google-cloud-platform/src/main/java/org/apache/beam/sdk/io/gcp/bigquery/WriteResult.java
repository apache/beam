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
package org.apache.beam.sdk.io.gcp.bigquery;

import java.util.Collections;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.POutputValueBase;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * The result of a {@link BigQueryIO.Write} transform.
 */
final class WriteResult extends POutputValueBase {
  /**
   * Creates a {@link WriteResult} in the given {@link Pipeline}.
   */
  static WriteResult in(Pipeline pipeline) {
    return new WriteResult(pipeline);
  }

  @Override
  public Map<TupleTag<?>, PValue> expand() {
    return Collections.emptyMap();
  }

  private WriteResult(Pipeline pipeline) {
    super(pipeline);
  }
}
