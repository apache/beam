package cz.seznam.euphoria.core.executor.inmem;

/**
 * A triggering in local JVM based on processing time.
 */
public class ProcessingTimeTriggering extends AbstractTriggering {

  @Override
  public long getCurrentTimestamp() {
    return System.currentTimeMillis();
  }
}
