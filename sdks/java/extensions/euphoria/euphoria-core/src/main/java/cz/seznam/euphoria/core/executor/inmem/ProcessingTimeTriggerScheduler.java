package cz.seznam.euphoria.core.executor.inmem;

/**
 * Trigger scheduler based on real wall-clock time (processing time).
 */
public class ProcessingTimeTriggerScheduler extends AbstractTriggerScheduler {

  @Override
  public long getCurrentTimestamp() {
    return System.currentTimeMillis();
  }

}
