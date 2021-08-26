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
package org.apache.beam.sdk.function;

import java.util.function.BiFunction;

/**
 * A {@link BiFunction} which can throw {@link Exception}s.
 *
 * <p>Used to expand the allowed set of method references to be used by Java 8 functional
 * interfaces.
 */
@FunctionalInterface
public interface ThrowingBiFunction<T1, T2, T3> {
  T3 apply(T1 t1, T2 t2) throws Exception;
}
