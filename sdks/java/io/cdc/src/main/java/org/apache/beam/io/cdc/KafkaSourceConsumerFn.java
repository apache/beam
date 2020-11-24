package org.apache.beam.io.cdc;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.joda.time.Duration;

public class KafkaSourceConsumerFn<T> extends DoFn<Map<String, String>, T> {

  static class OffsetHolder implements Serializable {
    @Nullable
    public final Map<String, ? extends Object> offset;
    public final List<? extends Object> history;
    OffsetHolder(@Nullable Map<String, ? extends Object> offset, @Nullable List<? extends Object> history) {
      this.offset = offset;
      this.history = history == null ? new ArrayList<>() : history;
    }
  }

  static class OffsetTracker extends RestrictionTracker<OffsetHolder, Map<String, Object>> {

    OffsetHolder restriction;
    boolean done = false;

    OffsetTracker(OffsetHolder holder) {
      this.restriction = holder;
    }

    @Override
    public boolean tryClaim(Map<String, Object> position) {
      System.out.println("Claiming " + position.toString() + " used to have: " + String.format("%s", restriction.offset));
      this.restriction = new OffsetHolder(position, this.restriction.history);
      return true;
    }

    @Override
    public OffsetHolder currentRestriction() {
      return restriction;
    }

    @Override
    public @org.checkerframework.checker.nullness.qual.Nullable SplitResult<OffsetHolder> trySplit(
        double fractionOfRemainder) {
      System.out.println("Trying to split");
      return SplitResult.of(new OffsetHolder(null, null), restriction);
    }

    @Override
    public void checkDone() throws IllegalStateException {
      return;
    }

    @Override
    public IsBounded isBounded() {
      return IsBounded.BOUNDED;
    }
  }

  public static final String BEAM_INSTANCE_PROPERTY = "beam.parent.instance";
  private final Class<? extends SourceConnector> connectorClass;
  private final SerializableFunction<SourceRecord, T> fn;
  protected static final Map<String, RestrictionTracker<OffsetHolder,  Map<String, Object>>>
      restrictionTrackers = new ConcurrentHashMap<>();

  KafkaSourceConsumerFn(Class<? extends SourceConnector> connectorClass,
                        SerializableFunction<SourceRecord, T> fn) {
    this.connectorClass = connectorClass;
    this.fn = fn;
  }

  @GetInitialRestriction
  public OffsetHolder getInitialRestriction(@Element Map<String, String> unused) throws IOException {
    return new OffsetHolder(null, null);
  }

  @NewTracker
  public RestrictionTracker<OffsetHolder, Map<String, Object>> newTracker(@Restriction OffsetHolder restriction) {
    return new OffsetTracker(restriction);
  }

  @GetRestrictionCoder
  public Coder<OffsetHolder> getRestrictionCoder() {
    return SerializableCoder.of(OffsetHolder.class);
  }

  @DoFn.ProcessElement
  public ProcessContinuation process(
      @Element Map<String, String> element,
      RestrictionTracker<OffsetHolder, Map<String, Object>> tracker,
      OutputReceiver<T> receiver)
      throws IllegalAccessException, InstantiationException, InterruptedException, NoSuchMethodException, InvocationTargetException {
    Map<String, String> configuration = new HashMap<>(element);
    // Adding the current restriction to the class object to be found by the database history
    restrictionTrackers.put(Integer.toString(System.identityHashCode(this)), tracker);
    configuration.put(BEAM_INSTANCE_PROPERTY,
        Integer.toString(System.identityHashCode(this)));
    SourceConnector conn = connectorClass.getDeclaredConstructor().newInstance();
    conn.start(configuration);

    SourceTask task = (SourceTask) conn.taskClass().getDeclaredConstructor().newInstance();
    Map<String, String> taskConfig = conn.taskConfigs(1).get(0);

    System.out.println("CALL PROCESS!!");
    task.initialize(new BeamSourceTaskContext(tracker.currentRestriction().offset));
    task.start(taskConfig);

    List<SourceRecord> records = task.poll();
    if (records == null) {
      // At this point, the source is done.
      System.out.println("DONESO BECAUSE NULL");
      restrictionTrackers.remove(Integer.toString(System.identityHashCode(this)));
      return ProcessContinuation.stop();
    }
    if (records.size() == 0) {
      restrictionTrackers.remove(Integer.toString(System.identityHashCode(this)));
      return ProcessContinuation.resume().withResumeDelay(Duration.standardSeconds(1));
    }
    for (SourceRecord record : records) {
      Map<String, Object> offset = (Map<String, Object>) record.sourceOffset();
      if (offset != null && tracker.tryClaim(offset)) {
        System.out.println("RECEIVED SOME " + record.toString());
        receiver.output(this.fn.apply(record));
      } else {
        System.out.println("DONE BECAUSE NULL OFFSET or CANT CLAIM!");
        // We're done.
        restrictionTrackers.remove(Integer.toString(System.identityHashCode(this)));
        return ProcessContinuation.stop();
      }
    }
    System.out.println("WE SHOULD RESUME IN A BIT!");
    restrictionTrackers.remove(Integer.toString(System.identityHashCode(this)));
    return ProcessContinuation.resume().withResumeDelay(Duration.standardSeconds(1));
  }

  private static class BeamSourceTaskContext implements SourceTaskContext {
    private Map<String, ? extends Object> initialOffset;
    BeamSourceTaskContext(@Nullable Map<String, ? extends Object> initialOffset) {
      this.initialOffset = initialOffset;
    }
    @Override
    public Map<String, String> configs() {
      // TODO(pabloem): Do we need to implement this?
      throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public OffsetStorageReader offsetStorageReader() {
      System.out.println("Creating an offset storage reader");
      return new SourceOffsetStorageReader(initialOffset);
    }
  }

  private static class SourceOffsetStorageReader implements OffsetStorageReader {
    private Map<String, ?> offset;
    SourceOffsetStorageReader(Map<String, ?> initialOffset) {
      this.offset = initialOffset;
    }
    @Override
    public <V> Map<String, Object> offset(Map<String, V> partition) {
      return offsets(Collections.singletonList(partition)).getOrDefault(partition, ImmutableMap.of());
    }

    @Override
    public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> partitions) {
      System.out.println("GETTING OFFSETS!");
      Map<Map<String, T>, Map<String, Object>> map = new HashMap<>();
      for (Map<String, T> p : partitions) {
        map.put(p, (Map<String, Object>) offset);
      }
      return map;
    }
  }
}
