

package cz.seznam.euphoria.core.executor;

import cz.seznam.euphoria.core.client.dataset.Trigger;
import cz.seznam.euphoria.core.client.dataset.Triggering;

import java.util.Timer;
import java.util.TimerTask;

/**
 * A triggering in local JVM.
 */
public class LocalTriggering implements Triggering {

  Timer timer = new Timer(true);
  
  @Override
  public void scheduleOnce(long duration, Trigger trigger) {
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        trigger.fire();
      }}, duration);
  }

  public void close() {
    timer.cancel();
  }

  @Override
  public void schedulePeriodic(long timeout, Trigger trigger) {
    timer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        trigger.fire();
      }}, timeout, timeout);
  }

}
