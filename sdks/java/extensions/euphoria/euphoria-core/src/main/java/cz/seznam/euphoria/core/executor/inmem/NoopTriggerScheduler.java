package cz.seznam.euphoria.core.executor.inmem;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;

public class NoopTriggerScheduler<W extends Window, K>
    implements TriggerScheduler<W, K> {

  @Override
  public boolean scheduleAt(long stamp, KeyedWindow<W, K> window, Triggerable<W, K> trigger) {
    return true;
  }

  @Override
  public long getCurrentTimestamp() {
    return 0;
  }

  @Override
  public void cancelAll() {

  }

  @Override
  public void cancel(long stamp, KeyedWindow<W, K> window) {

  }

  @Override
  public void close() {

  }

}
