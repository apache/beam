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
package org.apache.beam.sdk.extensions.euphoria.executor.local;

import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Operator;
import org.apache.beam.sdk.extensions.euphoria.core.executor.graph.DAG;

/** A series of transformations with single output operator. */
class ExecPath {

  /** A DAG of operators. */
  private final DAG<Operator<?, ?>> dag;

  private ExecPath(DAG<Operator<?, ?>> dag) {
    this.dag = dag;
  }

  /** Create new ExecPath. */
  static ExecPath of(DAG<Operator<?, ?>> dag) {
    return new ExecPath(dag);
  }

  public DAG<Operator<?, ?>> dag() {
    return dag;
  }
}
