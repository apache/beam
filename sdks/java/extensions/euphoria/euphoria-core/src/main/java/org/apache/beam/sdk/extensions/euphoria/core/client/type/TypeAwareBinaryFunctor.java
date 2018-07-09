/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.beam.sdk.extensions.euphoria.core.client.type;

import org.apache.beam.sdk.extensions.euphoria.core.client.functional.BinaryFunctor;
import org.apache.beam.sdk.extensions.euphoria.core.client.io.Collector;
import org.apache.beam.sdk.values.TypeDescriptor;

/** Save output type of {@link BinaryFunctor}. */
public class TypeAwareBinaryFunctor<LeftT, RightT, OutT>
    extends AbstractTypeAware<BinaryFunctor<LeftT, RightT, OutT>, OutT>
    implements BinaryFunctor<LeftT, RightT, OutT> {

  private TypeAwareBinaryFunctor(
      BinaryFunctor<LeftT, RightT, OutT> function, TypeDescriptor<OutT> typeHint) {
    super(function, typeHint);
  }

  public static <LeftT, RightT, OutT> TypeAwareBinaryFunctor<LeftT, RightT, OutT> of(
      BinaryFunctor<LeftT, RightT, OutT> function, TypeDescriptor<OutT> typeHint) {
    return new TypeAwareBinaryFunctor<>(function, typeHint);
  }

  @Override
  public void apply(LeftT left, RightT right, Collector<OutT> collector) {
    getDelegate().apply(left, right, collector);
  }
}
