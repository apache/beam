package cz.seznam.euphoria.inmem;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;

public class NoopTriggerScheduler<W extends Window, K>
    implements TriggerScheduler<W, K> {

  private volatile long currentWatermark;

  @Override
  public boolean scheduleAt(long stamp, KeyedWindow<W, K> window, Triggerable<W, K> trigger) {
    if (currentWatermark < stamp) {
      currentWatermark = stamp;
    }
    return true;
  }

  @Override
  public long getCurrentTimestamp() {
    return currentWatermark;
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
