
package cz.seznam.euphoria.core.executor;

import cz.seznam.euphoria.core.client.dataset.Trigger;
import cz.seznam.euphoria.core.client.dataset.Triggering;
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

  transient Timer timer = new Timer();
  transient Map<Trigger, Long> activeTriggers = new HashMap<>();

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
      timer.schedule(new TimerTask() {
        @Override
        public void run() {
          synchronized (WatermarkTriggering.this) {
            activeTriggers.remove(trigger);
          }
          trigger.fire();
        }
      }, stamp - currentWatermark);
      synchronized (this) {
        activeTriggers.put(trigger, stamp);
      }
      return true;
    }
    return false;
  }

  @Override
  public void close() {
    timer.cancel();
    activeTriggers.clear();
  }

  @Override
  public void updateProcessed(long stamp) {
    long newWatermark = stamp - watermarkDuration;
    if (currentWatermark < newWatermark) {
      final ArrayList<Map.Entry<Trigger, Long>> active;
      synchronized (this) {
        // reschedule all active triggers
        active = new ArrayList<>(activeTriggers.entrySet());
        timer.purge();
        activeTriggers.clear();
        currentWatermark = newWatermark;
      }
      for (Map.Entry<Trigger, Long> e : active) {
        if (e.getValue() > currentWatermark) {
          scheduleAt(e.getValue(), e.getKey());
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
