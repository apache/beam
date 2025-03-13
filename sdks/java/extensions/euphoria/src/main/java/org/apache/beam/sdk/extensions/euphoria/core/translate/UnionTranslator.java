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
package org.apache.beam.sdk.extensions.euphoria.core.translate;

import org.apache.beam.sdk.extensions.euphoria.core.client.operator.Union;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Euphoria to Beam translation of {@link Union} operator.
 *
 * @deprecated Use Java SDK directly, Euphoria is scheduled for removal in a future release.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
@Deprecated
public class UnionTranslator<InputT> implements OperatorTranslator<InputT, InputT, Union<InputT>> {

  @Override
  public PCollection<InputT> translate(Union<InputT> operator, PCollectionList<InputT> inputs) {
    final TypeDescriptor<InputT> outputType = operator.getOutputType().orElse(null);
    return operator
        .getName()
        .map(name -> inputs.apply(name, Flatten.pCollections()).setTypeDescriptor(outputType))
        .orElseGet(() -> inputs.apply(Flatten.pCollections()).setTypeDescriptor(outputType));
  }
}
