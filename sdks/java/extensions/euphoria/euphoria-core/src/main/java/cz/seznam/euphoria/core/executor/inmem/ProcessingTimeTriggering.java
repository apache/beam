

package cz.seznam.euphoria.core.executor.inmem;

import cz.seznam.euphoria.core.client.dataset.Trigger;
import cz.seznam.euphoria.core.client.dataset.Triggering;
import java.io.IOException;
import java.io.ObjectInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;
import java.util.TimerTask;

/**
 * A triggering in local JVM based on processing time.
 */
public class ProcessingTimeTriggering implements Triggering {

  private static final Logger LOG = LoggerFactory.getLogger(ProcessingTimeTriggering.class);
  transient Timer timer = new Timer(true);
  
  @Override
  public boolean scheduleAt(long stamp, Trigger trigger) {
    long duration = stamp - System.currentTimeMillis();
    return scheduleAfter(duration, trigger);
  }


  private boolean scheduleAfter(long duration, Trigger trigger) {
    if (duration >= 0) {
      timer.schedule(new TimerTask() {
        @Override
        public void run() {
          try {
            trigger.fire();
          } catch (Exception e) {
            LOG.warn("Firing trigger " + trigger + " failed!", e);
          }
        }}, duration);
      return true;
    }
    return false;
  }


  @Override
  public void close() {
    timer.cancel();
  }

 private void readObject(ObjectInputStream stream)
     throws IOException, ClassNotFoundException {
   stream.defaultReadObject();
   this.timer = new Timer();
 }


}
