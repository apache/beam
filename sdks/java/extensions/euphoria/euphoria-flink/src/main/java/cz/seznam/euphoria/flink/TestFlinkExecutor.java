package cz.seznam.euphoria.flink;

import cz.seznam.euphoria.core.util.Settings;
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
      ExecutionEnvironment environment) {
    return new BatchFlowTranslator(settings, environment.getBatchEnv(), splitAssignerFactory);
  }
}
