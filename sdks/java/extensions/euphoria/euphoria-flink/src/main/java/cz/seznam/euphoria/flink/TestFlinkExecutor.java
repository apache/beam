package cz.seznam.euphoria.flink;

/**
 * Executor running Flink in "local environment". The local execution environment
 * will run the program in a multi-threaded fashion in the same JVM as the
 * environment was created in. The default parallelism of the local
 * environment is the number of hardware contexts (CPU cores / threads).
 */
public class TestFlinkExecutor extends FlinkExecutor {

  public TestFlinkExecutor() {
    super(true);
  }
}
