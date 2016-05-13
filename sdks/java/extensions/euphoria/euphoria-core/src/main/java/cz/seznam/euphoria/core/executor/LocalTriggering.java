

package cz.seznam.euphoria.core.executor;

import cz.seznam.euphoria.core.client.dataset.Trigger;
import cz.seznam.euphoria.core.client.dataset.Triggering;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;
import java.util.TimerTask;

/**
 * A triggering in local JVM.
 */
public class LocalTriggering implements Triggering {

  private static final Logger LOG = LoggerFactory.getLogger(LocalTriggering.class);
  Timer timer = new Timer(true);
  
  @Override
  public void scheduleOnce(long duration, Trigger trigger) {
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          trigger.fire();
        } catch (Exception e) {
          LOG.warn("Firing trigger " + trigger + " failed!", e);
        }
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
        try {
          trigger.fire();
        } catch (Exception e) {
          LOG.warn("Firing trigger " + trigger + " failed!", e);
        }
      }}, timeout, timeout);
  }

}
