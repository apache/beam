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
package org.apache.beam.runners.direct;

import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

/**
 * A {@link PCollectionViewWriter} is responsible for writing contents of a {@link PCollection} to a
 * storage mechanism that can be read from while constructing a {@link PCollectionView}.
 *
 * @param <ElemT> the type of elements the input {@link PCollection} contains.
 * @param <ViewT> the type of the PCollectionView this writer writes to.
 */
@FunctionalInterface
interface PCollectionViewWriter<ElemT, ViewT> {
  void add(Iterable<WindowedValue<ElemT>> values);
}
