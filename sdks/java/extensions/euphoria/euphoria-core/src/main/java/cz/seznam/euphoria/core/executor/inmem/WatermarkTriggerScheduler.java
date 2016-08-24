package cz.seznam.euphoria.core.executor.inmem;

import java.util.List;

/**
 * Trigger scheduler based on watermarks. Uses event-time instead of real
 * wall-clock time.
 */
public class WatermarkTriggerScheduler extends AbstractTriggerScheduler {

  private final long watermarkDuration;
  private long currentWatermark;
 
  /**
   * Create the triggering with specified duration in ms.
   * @param duration duration of the watermark in ms.
   */
  public WatermarkTriggerScheduler(long duration) {
    this.watermarkDuration = duration;
    this.currentWatermark = -duration;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void updateStamp(long stamp) {
    long newWatermark = stamp - watermarkDuration;
    if (currentWatermark < newWatermark) {
      // reschedule all active triggers
      List<ScheduledTriggerTask> canceled = this.cancelAllImpl();
      currentWatermark = newWatermark;
      for (TriggerTask t : canceled) {
        if (t.getTimestamp() > currentWatermark) {
          scheduleAt(t.getTimestamp(), t.getWindow(), t.getTrigger());
        } else {
          t.getTrigger().fire(t.getTimestamp(), t.getWindow());
        }
      }
    }
  }

  @Override
  public long getCurrentTimestamp() {
    return currentWatermark;
  }
}
