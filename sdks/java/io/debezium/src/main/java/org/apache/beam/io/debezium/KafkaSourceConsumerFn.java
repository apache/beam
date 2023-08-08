/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.io.debezium;

import static org.apache.beam.io.debezium.KafkaConnectUtils.debeziumRecordInstant;

import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.history.AbstractDatabaseHistory;
import io.debezium.relational.history.DatabaseHistoryException;
import io.debezium.relational.history.HistoryRecord;
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
import java.util.function.Consumer;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 *
 * <h3>Quick Overview</h3>
 *
 * This is a Splittable {@link DoFn} used to process records fetched from supported Debezium
 * Connectors.
 *
 * <p>Currently it has a time limiter (see {@link OffsetTracker}) which, if set, it will stop
 * automatically after the specified elapsed minutes. Otherwise, it will keep running until the user
 * explicitly interrupts it.
 *
 * <p>It might be initialized either as:
 *
 * <pre>KafkaSourceConsumerFn(connectorClass, SourceRecordMapper, maxRecords, milisecondsToRun)
 * </pre>
 *
 * Or with a time limiter:
 *
 * <pre>KafkaSourceConsumerFn(connectorClass, SourceRecordMapper, minutesToRun)</pre>
 */
@SuppressWarnings({"nullness"})
public class KafkaSourceConsumerFn<T> extends DoFn<Map<String, String>, T> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceConsumerFn.class);
  public static final String BEAM_INSTANCE_PROPERTY = "beam.parent.instance";

  private final Class<? extends SourceConnector> connectorClass;
  private final SourceRecordMapper<T> fn;

  private final Long milisecondsToRun;
  private final Integer maxRecords;

  private static DateTime startTime;
  private static final Map<String, RestrictionTracker<OffsetHolder, Map<String, Object>>>
      restrictionTrackers = new ConcurrentHashMap<>();

  /**
   * Initializes the SDF with a time limit.
   *
   * @param connectorClass Supported Debezium connector class
   * @param fn a SourceRecordMapper
   * @param maxRecords Maximum number of records to fetch before finishing.
   * @param milisecondsToRun Maximum time to run (in milliseconds)
   */
  KafkaSourceConsumerFn(
      Class<?> connectorClass,
      SourceRecordMapper<T> fn,
      Integer maxRecords,
      Long milisecondsToRun) {
    this.connectorClass = (Class<? extends SourceConnector>) connectorClass;
    this.fn = fn;
    this.maxRecords = maxRecords;
    this.milisecondsToRun = milisecondsToRun;
  }

  /**
   * Initializes the SDF to be run indefinitely.
   *
   * @param connectorClass Supported Debezium connector class
   * @param fn a SourceRecordMapper
   * @param maxRecords Maximum number of records to fetch before finishing.
   */
  KafkaSourceConsumerFn(Class<?> connectorClass, SourceRecordMapper<T> fn, Integer maxRecords) {
    this(connectorClass, fn, maxRecords, null);
  }

  @GetInitialRestriction
  public OffsetHolder getInitialRestriction(@Element Map<String, String> unused)
      throws IOException {
    KafkaSourceConsumerFn.startTime = new DateTime();
    return new OffsetHolder(null, null, null, this.maxRecords, this.milisecondsToRun);
  }

  @NewTracker
  public RestrictionTracker<OffsetHolder, Map<String, Object>> newTracker(
      @Restriction OffsetHolder restriction) {
    return new OffsetTracker(restriction);
  }

  @GetRestrictionCoder
  public Coder<OffsetHolder> getRestrictionCoder() {
    return SerializableCoder.of(OffsetHolder.class);
  }

  protected SourceRecord getOneRecord(Map<String, String> configuration) {
    try {
      SourceConnector connector = connectorClass.getDeclaredConstructor().newInstance();
      connector.start(configuration);

      SourceTask task = (SourceTask) connector.taskClass().getDeclaredConstructor().newInstance();
      task.initialize(new BeamSourceTaskContext(null));
      task.start(connector.taskConfigs(1).get(0));
      List<SourceRecord> records = Lists.newArrayList();
      int loops = 0;
      while (records.size() == 0) {
        if (loops > 3) {
          throw new RuntimeException("could not fetch database schema");
        }
        records = task.poll();
        // Waiting for the Database snapshot to finish.
        Thread.sleep(2000);
        loops += 1;
      }
      task.stop();
      connector.stop();
      return records.get(0);
    } catch (NoSuchMethodException
        | InterruptedException
        | InvocationTargetException
        | IllegalAccessException
        | InstantiationException e) {
      throw new RuntimeException("Unexpected exception fetching database schema.", e);
    }
  }

  void register(RestrictionTracker<OffsetHolder, Map<String, Object>> tracker) {
    restrictionTrackers.put(this.getHashCode(), tracker);
  }

  void reset() {
    restrictionTrackers.remove(this.getHashCode());
  }

  @GetInitialWatermarkEstimatorState
  public Instant getInitialWatermarkEstimatorState(@Timestamp Instant currentElementTimestamp) {
    return currentElementTimestamp;
  }

  @NewWatermarkEstimator
  public WatermarkEstimator<Instant> newWatermarkEstimator(
      @WatermarkEstimatorState Instant watermarkEstimatorState) {
    return new WatermarkEstimators.MonotonicallyIncreasing(
        ensureTimestampWithinBounds(watermarkEstimatorState));
  }

  private static Instant ensureTimestampWithinBounds(Instant timestamp) {
    if (timestamp.isBefore(BoundedWindow.TIMESTAMP_MIN_VALUE)) {
      timestamp = BoundedWindow.TIMESTAMP_MIN_VALUE;
    } else if (timestamp.isAfter(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
      timestamp = BoundedWindow.TIMESTAMP_MAX_VALUE;
    }
    return timestamp;
  }

  /**
   * Process the retrieved element and format it for output. Update all pending
   *
   * @param element A descriptor for the configuration of the {@link SourceConnector} and {@link
   *     SourceTask} instances.
   * @param tracker Restriction Tracker
   * @param receiver Output Receiver
   * @return {@link org.apache.beam.sdk.transforms.DoFn.ProcessContinuation} in most cases, to
   *     continue processing after 1 second. Otherwise, if we've reached a limit of elements, to
   *     stop processing.
   */
  @DoFn.ProcessElement
  public ProcessContinuation process(
      @Element Map<String, String> element,
      RestrictionTracker<OffsetHolder, Map<String, Object>> tracker,
      OutputReceiver<T> receiver)
      throws Exception {
    Map<String, String> configuration = new HashMap<>(element);

    // Adding the current restriction to the class object to be found by the database history
    register(tracker);
    configuration.put(BEAM_INSTANCE_PROPERTY, this.getHashCode());

    SourceConnector connector = connectorClass.getDeclaredConstructor().newInstance();
    connector.start(configuration);

    SourceTask task = (SourceTask) connector.taskClass().getDeclaredConstructor().newInstance();

    try {
      Map<String, ?> consumerOffset = tracker.currentRestriction().offset;
      LOG.debug("--------- Consumer offset from Debezium Tracker: {}", consumerOffset);

      task.initialize(new BeamSourceTaskContext(tracker.currentRestriction().offset));
      task.start(connector.taskConfigs(1).get(0));

      List<SourceRecord> records = task.poll();

      if (records == null) {
        LOG.debug("-------- Pulled records null");
        return ProcessContinuation.stop();
      }

      LOG.debug("-------- {} records found", records.size());
      while (records != null && !records.isEmpty()) {
        for (SourceRecord record : records) {
          LOG.debug("-------- Record found: {}", record);

          Map<String, Object> offset = (Map<String, Object>) record.sourceOffset();

          if (offset == null || !tracker.tryClaim(offset)) {
            LOG.debug("-------- Offset null or could not be claimed");
            return ProcessContinuation.stop();
          }

          T json = this.fn.mapSourceRecord(record);
          LOG.debug("****************** RECEIVED SOURCE AS JSON: {}", json);

          Instant recordInstant = debeziumRecordInstant(record);
          receiver.outputWithTimestamp(json, recordInstant);
        }
        task.commit();
        records = task.poll();
      }
    } catch (Exception ex) {
      throw new RuntimeException("Error occurred when consuming changes from Database. ", ex);
    } finally {
      reset();

      LOG.debug("------- Stopping SourceTask");
      task.stop();
    }

    long elapsedTime = System.currentTimeMillis() - KafkaSourceConsumerFn.startTime.getMillis();
    if (milisecondsToRun != null && milisecondsToRun > 0 && elapsedTime >= milisecondsToRun) {
      return ProcessContinuation.stop();
    } else {
      return ProcessContinuation.resume().withResumeDelay(Duration.standardSeconds(1));
    }
  }

  public String getHashCode() {
    return Integer.toString(System.identityHashCode(this));
  }

  private static class BeamSourceTaskContext implements SourceTaskContext {
    private final @Nullable Map<String, ?> initialOffset;

    BeamSourceTaskContext(@Nullable Map<String, ?> initialOffset) {
      this.initialOffset = initialOffset;
    }

    @Override
    public Map<String, String> configs() {
      // TODO(pabloem): Do we need to implement this?
      throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public OffsetStorageReader offsetStorageReader() {
      LOG.debug("------------- Creating an offset storage reader");
      return new DebeziumSourceOffsetStorageReader(initialOffset);
    }
  }

  private static class DebeziumSourceOffsetStorageReader implements OffsetStorageReader {
    private final Map<String, ?> offset;

    DebeziumSourceOffsetStorageReader(Map<String, ?> initialOffset) {
      this.offset = initialOffset;
    }

    @Override
    public <V> Map<String, Object> offset(Map<String, V> partition) {
      return offsets(Collections.singletonList(partition))
          .getOrDefault(partition, ImmutableMap.of());
    }

    @Override
    public <T> Map<Map<String, T>, Map<String, Object>> offsets(
        Collection<Map<String, T>> partitions) {
      LOG.debug("-------------- GETTING OFFSETS!");

      Map<Map<String, T>, Map<String, Object>> map = new HashMap<>();
      for (Map<String, T> partition : partitions) {
        map.put(partition, (Map<String, Object>) offset);
      }

      LOG.debug("-------------- OFFSETS: {}", map);
      return map;
    }
  }

  static class OffsetHolder implements Serializable {
    public @Nullable Map<String, ?> offset;
    public @Nullable List<?> history;
    public @Nullable Integer fetchedRecords;
    public @Nullable Integer maxRecords;
    public final @Nullable Long milisToRun;

    OffsetHolder(
        @Nullable Map<String, ?> offset,
        @Nullable List<?> history,
        @Nullable Integer fetchedRecords,
        @Nullable Integer maxRecords,
        @Nullable Long milisToRun) {
      this.offset = offset;
      this.history = history == null ? new ArrayList<>() : history;
      this.fetchedRecords = fetchedRecords;
      this.maxRecords = maxRecords;
      this.milisToRun = milisToRun;
    }

    OffsetHolder(
        @Nullable Map<String, ?> offset,
        @Nullable List<?> history,
        @Nullable Integer fetchedRecords) {
      this(offset, history, fetchedRecords, null, -1L);
    }
  }

  /** {@link RestrictionTracker} for Debezium connectors. */
  static class OffsetTracker extends RestrictionTracker<OffsetHolder, Map<String, Object>> {
    private OffsetHolder restriction;

    OffsetTracker(OffsetHolder holder) {
      this.restriction = holder;
    }

    /**
     * Overriding {@link #tryClaim} in order to stop fetching records from the database.
     *
     * <p>This works on two different ways:
     *
     * <h3>Number of records</h3>
     *
     * <p>This is the default behavior. Once the specified number of records has been reached, it
     * will stop fetching them.
     *
     * <h3>Time based</h3>
     *
     * User may specify the amount of time the connector to be kept alive. Please see {@link
     * KafkaSourceConsumerFn} for more details on this.
     *
     * @param position Currently not used
     * @return boolean
     */
    @Override
    public boolean tryClaim(Map<String, Object> position) {
      LOG.debug("-------------- Claiming {} used to have: {}", position, restriction.offset);
      long elapsedTime = System.currentTimeMillis() - startTime.getMillis();
      int fetchedRecords =
          this.restriction.fetchedRecords == null ? 0 : this.restriction.fetchedRecords + 1;
      LOG.debug("------------Fetched records {} / {}", fetchedRecords, this.restriction.maxRecords);
      LOG.debug("-------------- Time running: {} / {}", elapsedTime, (this.restriction.milisToRun));
      this.restriction.offset = position;
      this.restriction.fetchedRecords = fetchedRecords;
      LOG.debug("-------------- History: {}", this.restriction.history);

      // If we've reached the maximum number of records OR the maximum time, we reject
      // the attempt to claim.
      // If we've reached neither, then we continue approve the claim.
      return (this.restriction.maxRecords == null || fetchedRecords < this.restriction.maxRecords)
          && (this.restriction.milisToRun == null
              || this.restriction.milisToRun == -1
              || elapsedTime < this.restriction.milisToRun);
    }

    @Override
    public OffsetHolder currentRestriction() {
      return restriction;
    }

    @Override
    public SplitResult<OffsetHolder> trySplit(double fractionOfRemainder) {
      LOG.debug("-------------- Trying to split: fractionOfRemainder={}", fractionOfRemainder);
      return SplitResult.of(new OffsetHolder(null, null, null), restriction);
    }

    @Override
    public void checkDone() throws IllegalStateException {}

    @Override
    public IsBounded isBounded() {
      // TODO(pabloem): Implement truncate call that allows us to restart the state
      return IsBounded.UNBOUNDED;
    }
  }

  public static class DebeziumSDFDatabaseHistory extends AbstractDatabaseHistory {
    private List<byte[]> history;

    public DebeziumSDFDatabaseHistory() {
      this.history = new ArrayList<>();
    }

    @Override
    public void start() {
      super.start();
      LOG.debug(
          "------------ STARTING THE DATABASE HISTORY! - trackers: {} - config: {}",
          restrictionTrackers,
          config.asMap());

      // We fetch the first key to get the first restriction tracker.
      // TODO(BEAM-11737): This may cause issues with multiple trackers in the future.
      RestrictionTracker<OffsetHolder, ?> tracker =
          restrictionTrackers.get(restrictionTrackers.keySet().iterator().next());
      this.history = (List<byte[]>) tracker.currentRestriction().history;
    }

    @Override
    protected void storeRecord(HistoryRecord record) throws DatabaseHistoryException {
      LOG.debug("------------- Adding history! {}", record);

      history.add(DocumentWriter.defaultWriter().writeAsBytes(record.document()));
    }

    @Override
    protected void recoverRecords(Consumer<HistoryRecord> consumer) {
      LOG.debug("------------- Trying to recover!");

      try {
        for (byte[] record : history) {
          Document doc = DocumentReader.defaultReader().read(record);
          consumer.accept(new HistoryRecord(doc));
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public boolean exists() {
      return history != null && !history.isEmpty();
    }

    @Override
    public boolean storageExists() {
      return history != null && !history.isEmpty();
    }
  }
}
