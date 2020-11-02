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
import org.apache.beam.sdk.transforms.Combine;

/** An aggregate function that can be executed as part of a SQL query. */
public class AggregateFn implements Serializable {
  private final Combine.CombineFn combineFn;

  private AggregateFn(Combine.CombineFn combineFn) {
    this.combineFn = combineFn;
  }

  public static AggregateFn fromCombineFn(Combine.CombineFn combineFn) {
    return new AggregateFn(combineFn);
  }

  public Combine.CombineFn getCombineFn() {
    return combineFn;
  }
}
