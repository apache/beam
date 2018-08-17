/**
 * Copyright 2016-2017 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cz.seznam.euphoria.beam;

import cz.seznam.euphoria.core.client.operator.Union;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

class UnionTranslator implements OperatorTranslator<Union> {

  @Override
  @SuppressWarnings("unchecked")
  public PCollection<?> translate(Union operator, BeamExecutorContext context) {
    return doTranslate(operator,context);
  }

  private static <T> PCollection<T> doTranslate(Union<T> operator, BeamExecutorContext context) {
    return PCollectionList.of(context.getInputs(operator)).apply(Flatten.pCollections());
  }

}
