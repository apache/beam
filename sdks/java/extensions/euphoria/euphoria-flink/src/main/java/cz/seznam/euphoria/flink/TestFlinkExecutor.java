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
package cz.seznam.euphoria.flink;

import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.flink.accumulators.FlinkAccumulatorFactory;
import cz.seznam.euphoria.flink.batch.BatchFlowTranslator;
import cz.seznam.euphoria.flink.batch.BatchFlowTranslator.SplitAssignerFactory;

/**
 * Executor running Flink in "local environment". The local execution environment
 * will run the program in a multi-threaded fashion in the same JVM as the
 * environment was created in. The default parallelism of the local
 * environment is the number of hardware contexts (CPU cores / threads).
 */
public class TestFlinkExecutor extends FlinkExecutor {
  
  private final SplitAssignerFactory splitAssignerFactory;
  
  public TestFlinkExecutor() {
    this(BatchFlowTranslator.DEFAULT_SPLIT_ASSIGNER_FACTORY);
  }
  
  public TestFlinkExecutor(SplitAssignerFactory splitAssignerFactory) {
    super(true);
    this.splitAssignerFactory = splitAssignerFactory;
  }

  @Override
  protected FlowTranslator createBatchTranslator(Settings settings,
                                                 ExecutionEnvironment environment,
                                                 FlinkAccumulatorFactory accumulatorFactory) {
    return new BatchFlowTranslator(settings, environment.getBatchEnv(),
            accumulatorFactory, splitAssignerFactory);
  }
}
