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
package org.apache.beam.sdk.extensions.euphoria.core.client.operator;

import com.google.common.collect.Sets;
import java.util.Set;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;

/** This class contains various methods related to {@link Operator}. */
public class Operators {

  /**
   * Operators that are considered to be basic and expected to be natively supported by each runner
   * implementation.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static final Set<Class<? extends Operator<?, ?>>> BASIC_OPS =
      (Set) Sets.newHashSet(FlatMap.class, ReduceStateByKey.class, Union.class);

  /**
   * Operators that are considered to be basic and expected to be natively supported by each runner
   * implementation.
   */
  public static Set<Class<? extends Operator<?, ?>>> getBasicOps() {
    return BASIC_OPS;
  }
}
