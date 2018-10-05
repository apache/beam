package cz.seznam.euphoria.core.executor.inmem;

import java.util.List;

/**
 * Triggering based on watermarks.
 */
public class WatermarkTriggering extends AbstractTriggering {

  private final long watermarkDuration;
  private long currentWatermark;
 
  /**
   * Create the triggering with specified duration in ms.
   * @param duration duration of the watermark in ms.
   */
  public WatermarkTriggering(long duration) {
    super();
    this.watermarkDuration = duration;
    this.currentWatermark = -duration;
  }

  @Override
  public void updateProcessed(long stamp) {
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
