package cz.seznam.euphoria.core.executor.inmem;

import cz.seznam.euphoria.core.client.dataset.windowing.WindowContext;
import cz.seznam.euphoria.core.client.triggers.Triggerable;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.executor.TriggerScheduler;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.Iterables;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Trigger scheduler based on watermarks. Uses event-time instead of real
 * wall-clock time.
 */
public class WatermarkTriggerScheduler implements TriggerScheduler {

  private final long watermarkDuration;
  private volatile long currentWatermark;
  
  private final SortedMap<Long, List<Pair<WindowContext<?, ?>, Triggerable<?, ?>>>>
      scheduledEvents = new TreeMap<>();
  private final Map<WindowContext<?, ?>, Set<Long>> eventStampsForWindow
      = new HashMap<>();
 
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
    final long newWatermark = stamp - watermarkDuration;
    if (currentWatermark < newWatermark) {
      synchronized (this) {
        if (currentWatermark < newWatermark) {
          currentWatermark = newWatermark;
          
          // fire all triggers that passed
          
          SortedMap<Long, List<Pair<WindowContext<?, ?>, Triggerable<?, ?>>>> headMap;
          headMap = scheduledEvents.headMap(currentWatermark);

          headMap.entrySet().stream()
              .flatMap(e -> e.getValue().stream().map(p -> Pair.of(e.getKey(), p)))
              // need to collect to list to prevent ConcurrentModificationException
              .collect(Collectors.toList())
              .forEach(p -> {
                Triggerable<?, ?> purged = purge(p.getSecond().getFirst(), p.getFirst());
                purged.fire(stamp, (WindowContext) p.getSecond().getFirst());
              });
                        
          // remove all expired events
          headMap.keySet().stream().collect(Collectors.toList())
              .forEach(scheduledEvents::remove);

        }
      }
    }
  }

  @Override
  public long getCurrentTimestamp() {
    return currentWatermark;
  }

  @Override
  public synchronized boolean scheduleAt(
      long stamp, WindowContext<?, ?> w, Triggerable trigger) {
    if (stamp < currentWatermark) return false;
    purge(w, stamp);
    add(stamp, trigger, w);
    return true;
  }

  @Override
  public void cancelAll() {
    scheduledEvents.clear();
    eventStampsForWindow.clear();
  }

  @Override
  public void cancel(WindowContext w) {
    Set<Long> stamps = eventStampsForWindow.get(w);
    if (stamps != null) {
      for (long stamp : stamps) {
        purge(w, stamp);
      }
    }
  }

  @Override
  public void close() {
    cancelAll();
  }

  /** Purge the scheduled event from all indices and return it (if exists). */
  private Triggerable<?, ?> purge(WindowContext<?, ?> w, long stamp) {
    Set<Long> scheduled = eventStampsForWindow.get(w);
    if (scheduled == null || !scheduled.remove(stamp)) {
      return null;
    }

    List<Pair<WindowContext<?, ?>, Triggerable<?, ?>>> eventsForWindow;
    eventsForWindow = scheduledEvents.get(stamp);

    Pair<WindowContext<?, ?>, Triggerable<?, ?>> event
        = Iterables.getOnlyElement(eventsForWindow.stream()
            .filter(p -> p.getFirst() == w).collect(Collectors.toList()));
    eventsForWindow.remove(event);
    if (eventsForWindow.isEmpty()) {
      scheduledEvents.remove(stamp);
    }
    return event.getSecond();
  }


  private void add(long stamp, Triggerable<?, ?> trigger, WindowContext<?, ?> w) {
    List<Pair<WindowContext<?, ?>, Triggerable<?, ?>>> scheduledForTime
        = scheduledEvents.get(stamp);
    if (scheduledForTime == null) {
      scheduledEvents.put(stamp, scheduledForTime = new ArrayList<>());
    }
    scheduledForTime.add(Pair.of(w, trigger));
    Set<Long> stamps = eventStampsForWindow.get(w);
    if (stamps == null) {
      eventStampsForWindow.put(w, stamps = new HashSet<>());
    }
    stamps.add(stamp);
  }

}
