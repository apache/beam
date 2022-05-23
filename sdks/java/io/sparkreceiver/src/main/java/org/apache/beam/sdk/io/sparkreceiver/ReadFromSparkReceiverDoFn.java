package org.apache.beam.sdk.io.sparkreceiver;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.UnboundedPerElement;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.splittabledofn.*;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.receiver.Receiver;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.apache.beam.sdk.io.sparkreceiver.SparkReceiverUtils.getOffsetByRecord;

@UnboundedPerElement
@SuppressWarnings({
        "rawtypes",
        "nullness",
        "UnusedVariable"
})
public class ReadFromSparkReceiverDoFn<V> extends DoFn<SparkReceiverSourceDescriptor, V> {

    private static final Logger LOG = LoggerFactory.getLogger(ReadFromSparkReceiverDoFn.class);

    private final SerializableFunction<Instant, WatermarkEstimator<Instant>>
            createWatermarkEstimatorFn;
    private Queue<V> availableRecordsQueue;
    private AtomicLong recordsRead;
    private ProxyReceiverBuilder<V, ? extends Receiver<V>> sparkReceiverBuilder;
    private Receiver<V> sparkReceiver;

    public ReadFromSparkReceiverDoFn(SparkReceiverIO.ReadFromSparkReceiverViaSdf<V> transform) {
        createWatermarkEstimatorFn = WatermarkEstimators.Manual::new;
        sparkReceiverBuilder = transform.sparkReceiverRead.getSparkReceiverBuilder();
    }

    private void initReceiver(Consumer<Object[]> storeConsumer, Receiver<V> sparkReceiver) {
        try {
            new WrappedSupervisor(sparkReceiver, new SparkConf(), storeConsumer);
        } catch (Exception e) {
            LOG.error("Can not init Spark Receiver!", e);
        }
    }

    @GetInitialRestriction
    public OffsetRange initialRestriction(@Element SparkReceiverSourceDescriptor sourceDescriptor) {
        return new OffsetRange(0, Long.MAX_VALUE);
    }

    @GetInitialWatermarkEstimatorState
    public Instant getInitialWatermarkEstimatorState(@Timestamp Instant currentElementTimestamp) {
        return currentElementTimestamp;
    }

    @NewWatermarkEstimator
    public WatermarkEstimator<Instant> newWatermarkEstimator(
            @WatermarkEstimatorState Instant watermarkEstimatorState) {
        return createWatermarkEstimatorFn.apply(ensureTimestampWithinBounds(watermarkEstimatorState));
    }

    @GetSize
    public double getSize(
            @Element SparkReceiverSourceDescriptor sourceDescriptor, @Restriction OffsetRange offsetRange) {
        return restrictionTracker(sourceDescriptor, offsetRange).getProgress().getWorkRemaining();
        // Before processing elements, we don't have a good estimated size of records and offset gap.
    }

    private class SparkReceiverLatestOffsetEstimator
            implements GrowableOffsetRangeTracker.RangeEndEstimator {


        public SparkReceiverLatestOffsetEstimator() {
        }

        @Override
        public long estimate() {
            if (sparkReceiver instanceof HubspotCustomReceiver) {
                return ((HubspotCustomReceiver) sparkReceiver).getEndOffset();
            }
            return Long.MAX_VALUE;
        }
    }

    @NewTracker
    public OffsetRangeTracker restrictionTracker(
            @Element SparkReceiverSourceDescriptor sourceDescriptor, @Restriction OffsetRange restriction) {
        return new GrowableOffsetRangeTracker(restriction.getFrom(), new SparkReceiverLatestOffsetEstimator());
    }

    @GetRestrictionCoder
    public Coder<OffsetRange> restrictionCoder() {
        return new OffsetRange.Coder();
    }

    @Setup
    public void setup() throws Exception {
        // Start to track record size and offset gap per bundle.
    }

    @Teardown
    public void teardown() throws Exception {
        //Closeables close
//        if (sparkReceiver.isStarted()) {
//            sparkReceiver.stop("Teardown");
//            availableRecordsQueue.clear();
//        }
    }

    @ProcessElement
    public ProcessContinuation processElement(
            @Element SparkReceiverSourceDescriptor sourceDescriptor,
            RestrictionTracker<OffsetRange, Long> tracker,
            WatermarkEstimator watermarkEstimator,
            OutputReceiver<V> receiver) {

        recordsRead = new AtomicLong(0);
        AtomicLong recordsOut = new AtomicLong(0);
        availableRecordsQueue = new ArrayBlockingQueue<>(1000);
        try {
            this.sparkReceiver = sparkReceiverBuilder.build();
            initReceiver(objects -> {
                V obj = (V) objects[0];
                availableRecordsQueue.offer(obj);
                long read = recordsRead.getAndIncrement();
//                if (read == 0) {
//                    LOG.info("First read = {}", obj);
//                } else if (read == 1) {
//                    LOG.info("Second read = {}", obj);
//                }
                if (read % 200 == 0) {
                    LOG.info("Records read = {}", read);
                }
            }, sparkReceiver);
        } catch (Exception e) {
            LOG.error("Can not create new Hubspot Receiver", e);
        }
        if (sparkReceiver instanceof HubspotCustomReceiver) {
            long from = tracker.currentRestriction().getFrom();
            ((HubspotCustomReceiver) sparkReceiver).setStartOffset(
                    String.valueOf(from == 0L ? 0 : from - 1));
        }
        sparkReceiver.onStart();
        LOG.info("Restriction: {}, {}", tracker.currentRestriction().getFrom(), tracker.currentRestriction().getTo());
        try {
            TimeUnit.MILLISECONDS.sleep(500);
        } catch (InterruptedException e) {
            LOG.error("Interrupted", e);
        }

        Long prevOffset = null;
        while (true) {
//            try {
//                TimeUnit.MILLISECONDS.sleep(100);
//            } catch (InterruptedException e) {
//                LOG.error("Interrupted", e);
//            }
            while (!availableRecordsQueue.isEmpty()) {
                V record = availableRecordsQueue.poll();
//                if (record == null) {
//                    LOG.info("RESUME");
//                    sparkReceiver.onStop();
//                    availableRecordsQueue.clear();
//                    return ProcessContinuation.resume();
//                }
                Integer offset = getOffsetByRecord(record.toString());
                if (prevOffset != null && offset <= prevOffset) {
                    //FIXME
                    LOG.info("OFFSET {} IS LESS THAN PREVIOUS {}, ({}, {}), recordsOut = {}", offset, prevOffset,
                            tracker.currentRestriction().getFrom(), tracker.currentRestriction().getTo(), recordsOut);
                    continue;
                }
                if (!tracker.tryClaim(offset.longValue())) {
                    LOG.info("TRY CLAIM FALSE, PREV = {}", prevOffset);
                    sparkReceiver.stop("Stopped");
                    availableRecordsQueue.clear();
                    return ProcessContinuation.stop();
                }
                ((ManualWatermarkEstimator) watermarkEstimator).setWatermark(Instant.now());
                receiver.outputWithTimestamp(record, Instant.now());
                prevOffset = offset.longValue();
                long out = recordsOut.incrementAndGet();
                if (out % 100 == 0) {
                    LOG.info("Records out = {}", out);
                }
            }
            LOG.info("RESUME");
            sparkReceiver.stop("Stopped");
            availableRecordsQueue.clear();
            return ProcessContinuation.resume();
        }
    }

    private static Instant ensureTimestampWithinBounds(Instant timestamp) {
        if (timestamp.isBefore(BoundedWindow.TIMESTAMP_MIN_VALUE)) {
            timestamp = BoundedWindow.TIMESTAMP_MIN_VALUE;
        } else if (timestamp.isAfter(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
            timestamp = BoundedWindow.TIMESTAMP_MAX_VALUE;
        }
        return timestamp;
    }
}
