/**
 * Copyright 2016 Seznam a.s.
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
package cz.seznam.euphoria.spark;

import cz.seznam.euphoria.core.client.operator.Union;
import org.apache.spark.api.java.JavaRDD;

import java.util.List;


class UnionTranslator implements SparkOperatorTranslator<Union> {

  @Override
  @SuppressWarnings("unchecked")
  public JavaRDD<?> translate(Union operator,
                              SparkExecutorContext context) {

    List<JavaRDD<?>> inputs = context.getInputs(operator);
    if (inputs.size() != 2) {
      throw new IllegalStateException("Union operator needs 2 inputs");
    }
    JavaRDD<?> left = inputs.get(0);
    JavaRDD<?> right = inputs.get(1);

    return left.union((JavaRDD) right);
  }
}
