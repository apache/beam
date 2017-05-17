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
package cz.seznam.euphoria.flink.batch;

import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.flink.FlinkOperator;
import cz.seznam.euphoria.flink.OperatorTranslator;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

interface BatchOperatorTranslator<T extends Operator>
    extends OperatorTranslator<T, FlinkOperator<T>,
                               ExecutionEnvironment, DataSet<?>,
                               BatchExecutorContext> {

  String CFG_LIST_STORAGE_MAX_MEMORY_ELEMS_KEY = "euphoria.flink.batch.list-storage.max-memory-elements";
  int CFG_LIST_STORAGE_MAX_MEMORY_ELEMS_DEFAULT = 1000;

}
