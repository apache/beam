
package cz.seznam.euphoria.core.executor.inmem;

import cz.seznam.euphoria.core.client.dataset.Trigger;
import cz.seznam.euphoria.core.client.dataset.Triggering;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Triggering based on watermarks.
 */
public class WatermarkTriggering implements Triggering {

  private static final Logger LOG = LoggerFactory.getLogger(WatermarkTriggering.class);

  private static final class Scheduled {
    final TimerTask tt;
    final long stamp;
    Scheduled(TimerTask tt, long stamp) {
      this.tt = tt;
      this.stamp = stamp;
    }
    void cancel() {
      tt.cancel();
    }
  }

  transient Timer timer = new Timer();
  transient Map<Trigger, Scheduled> activeTriggers = new HashMap<>();

  final long watermarkDuration;
  long currentWatermark;
 
  /**
   * Create the triggering with specified duration in ms.
   * @param duration duration of the watermark in ms.
   */
  public WatermarkTriggering(long duration) {
    this.watermarkDuration = duration;
    this.currentWatermark = -duration;
  }

  @Override
  public boolean scheduleAt(long stamp, Trigger trigger) {
    if (stamp >= currentWatermark) {
      TimerTask tt = new TimerTask() {
        @Override
        public void run() {
          synchronized (WatermarkTriggering.this) {
            activeTriggers.remove(trigger);
          }
          trigger.fire();
        }
      };
      synchronized (this) {
        timer.schedule(tt, stamp - currentWatermark);
        activeTriggers.put(trigger, new Scheduled(tt, stamp));
        return true;
      }
    }
    return false;
  }

  @Override
  public void close() {
    synchronized (this) {
      timer.cancel();
      activeTriggers.values().forEach(Scheduled::cancel);
      activeTriggers.clear();
      timer.purge();
    }
  }

  @Override
  public void updateProcessed(long stamp) {
    long newWatermark = stamp - watermarkDuration;
    if (currentWatermark < newWatermark) {
      final ArrayList<Map.Entry<Trigger, Scheduled>> active;
      synchronized (this) {
        // reschedule all active triggers
        active = new ArrayList<>(activeTriggers.entrySet());
        active.forEach(x -> x.getValue().cancel());
        activeTriggers.clear();
        timer.purge();
        currentWatermark = newWatermark;
      }
      for (Map.Entry<Trigger, Scheduled> e : active) {
        if (e.getValue().stamp > currentWatermark) {
          scheduleAt(e.getValue().stamp, e.getKey());
        } else {
          e.getKey().fire();
        }
      }
    }
  }

  private void readObject(ObjectInputStream stream)
      throws IOException, ClassNotFoundException {
    stream.defaultReadObject();
    this.timer = new Timer();
    this.activeTriggers = new HashMap<>();
  }

}
