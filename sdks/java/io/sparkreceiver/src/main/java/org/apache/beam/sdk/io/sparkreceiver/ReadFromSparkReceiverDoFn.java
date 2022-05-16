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
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

@UnboundedPerElement
@SuppressWarnings({
        "rawtypes",
        "nullness"
})
public class ReadFromSparkReceiverDoFn<V> extends DoFn<SparkReceiverSourceDescriptor, V> {

    private static final Logger LOG = LoggerFactory.getLogger(ReadFromSparkReceiverDoFn.class);

    private final SerializableFunction<Instant, WatermarkEstimator<Instant>>
            createWatermarkEstimatorFn;
    private final Queue<V> availableRecordsQueue;
    private final AtomicLong recordsRead;
    private final AtomicLong recordsWrite;

    public ReadFromSparkReceiverDoFn(SparkReceiverIO.ReadFromSparkReceiverViaSdf<V> transform) {
        createWatermarkEstimatorFn = WatermarkEstimators.MonotonicallyIncreasing::new;
        availableRecordsQueue = new PriorityBlockingQueue<>();
        recordsRead = new AtomicLong(0);
        recordsWrite = new AtomicLong(0);
        initReceiver(objects -> {
            availableRecordsQueue.offer((V) objects[0]);
            long read = recordsRead.getAndIncrement();
            if (read % 100 == 0) {
                LOG.info("[{}], records read = {}", 0, recordsRead);
            }
        }, transform.sparkReceiverRead.getSparkReceiver());
    }

    private void initReceiver(Consumer<Object[]> storeConsumer, Receiver<V> receiver) {
        try {
            new WrappedSupervisor(receiver, new SparkConf(), storeConsumer);

            receiver.onStart();
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

    private static class SparkReceiverLatestOffsetEstimator
            implements GrowableOffsetRangeTracker.RangeEndEstimator {
        @Override
        public long estimate() {
            //TODO:
            return 0;
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
    }

    @ProcessElement
    public ProcessContinuation processElement(
            @Element SparkReceiverSourceDescriptor sourceDescriptor,
            RestrictionTracker<OffsetRange, Long> tracker,
            WatermarkEstimator watermarkEstimator,
            OutputReceiver<V> receiver) {
        //TODO:
        while (true) {
            V record = availableRecordsQueue.poll();
            if (record == null) {
                return ProcessContinuation.stop();
            }
            if (!tracker.tryClaim(recordsWrite.get())) {
                return ProcessContinuation.stop();
            }
            receiver.output(record);
            recordsWrite.getAndIncrement();
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
