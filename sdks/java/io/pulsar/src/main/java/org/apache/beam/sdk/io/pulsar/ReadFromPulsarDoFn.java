package org.apache.beam.sdk.io.pulsar;

import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.splittabledofn.*;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.util.MessageIdUtils;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Suppliers;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

@DoFn.UnboundedPerElement
public class ReadFromPulsarDoFn extends DoFn<PulsarSourceDescriptor, Message> {

    private PulsarClient client;
    private String topic;
    private List<String> topics;
    private String clientUrl;

    private final SerializableFunction<Message, Instant> extractOutputTimestampFn;

    public ReadFromPulsarDoFn(PulsarIO.Read transform) {
        this.extractOutputTimestampFn = transform.getExtractOutputTimestampFn();
        this.clientUrl = transform.getClientUrl();
        this.topic = transform.getTopic();

    }

    // Open connection to Pulsar clients
    @Setup
    private void initPulsarClients() throws PulsarClientException {
        if(this.clientUrl == null) {
            this.clientUrl = PulsarIOUtils.SERVICE_URL;
        }
        this.client = PulsarClient.builder()
                .serviceUrl(clientUrl)
                .build();
    }

    // Close connection to Pulsar clients
    @Teardown
    public void teardown() throws PulsarClientException {
        this.client.close();
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction(@Element PulsarSourceDescriptor pulsarSource) {
        // Reading a topic from starting point with offset 0
        long startOffset = 0;
        if(pulsarSource.getStartOffset() != null) {
            startOffset = pulsarSource.getStartOffset();
        }

        return new OffsetRange(startOffset, Long.MAX_VALUE);
    }

    /*
    It may define a DoFn.GetSize method or ensure that the RestrictionTracker implements
    RestrictionTracker.HasProgress. Poor auto-scaling of workers and/or splitting may result
     if size or progress is an inaccurate representation of work.
     See DoFn.GetSize and RestrictionTracker.HasProgress for further details.
     */
    @GetSize
    public double getSize(@Element PulsarSourceDescriptor pulsarSource, @Restriction OffsetRange offsetRange) throws PulsarClientException {
        //TODO improve getsize estiamate, check pulsar stats to improve get size estimate
        // https://pulsar.apache.org/docs/en/admin-api-topics/#get-stats
        double estimateRecords = restrictionTracker(pulsarSource, offsetRange).getProgress().getWorkRemaining();
        return estimateRecords;
    }

    private Reader<byte[]> newReader(PulsarClient client, String topicPartition) throws PulsarClientException {
        ReaderBuilder<byte[]> builder = client.newReader().topic(topicPartition).startMessageId(MessageId.earliest);
        return builder.create();
    }

    @ProcessElement
    public ProcessContinuation processElement(
            @Element PulsarSourceDescriptor pulsarSourceDescriptor,
            RestrictionTracker<OffsetRange, Long> tracker,
            WatermarkEstimator watermarkEstimator,
            OutputReceiver<Message> output) throws IOException {

        long startTimestamp = tracker.currentRestriction().getFrom();
        String topicPartition = pulsarSourceDescriptor.getTopic();
        //List<String> topicPartitions = client.getPartitionsForTopic(pulsarRecord.getTopic()).join();
        //for(String topicPartition:topicPartitions) {
            try(Reader<byte[]> reader = newReader(client, topicPartition)) {
                reader.seek(startTimestamp);
                while (true) {
                    Message<byte[]> message = reader.readNext();
                    MessageId messageId = message.getMessageId();
                    long currentTimestampOffset = message.getPublishTime();
                    // if tracker.tryclaim() return true, sdf must execute work otherwise
                    // doFn must exit processElement() without doing any work associated
                    // or claiming more work
                    if (!tracker.tryClaim(currentTimestampOffset)) {
                        return ProcessContinuation.stop();
                    }

                    Instant outputTimestamp = extractOutputTimestampFn.apply(message);
                    output.outputWithTimestamp(message, outputTimestamp);

                }
            }
        //}
    }

    @GetInitialWatermarkEstimatorState
    public Instant getInitialWatermarkEstimatorState(@Timestamp Instant currentElementTimestamp) {
        return currentElementTimestamp;
    }

    @NewWatermarkEstimator
    public WatermarkEstimator<Instant> newWatermarkEstimator(
            @WatermarkEstimatorState Instant watermarkEstimatorState) {
        return new WatermarkEstimators.MonotonicallyIncreasing(ensureTimestampWithinBounds(watermarkEstimatorState));
    }

    @NewTracker
    public GrowableOffsetRangeTracker restrictionTracker(@Element PulsarSourceDescriptor pulsarSource,
                                                         @Restriction OffsetRange restriction) throws PulsarClientException {

        PulsarLatestOffsetEstimator offsetEstimator =  new PulsarLatestOffsetEstimator(client, pulsarSource.getTopic());
        return new GrowableOffsetRangeTracker(restriction.getFrom(), offsetEstimator);

    }

    private static class PulsarLatestOffsetEstimator implements GrowableOffsetRangeTracker.RangeEndEstimator {

        private final Supplier<Message> memoizedBacklog;
        private final Reader readerLatestMsg;

        private PulsarLatestOffsetEstimator(PulsarClient client, String topic) throws PulsarClientException {
            this.readerLatestMsg = client.newReader()
                                        .startMessageId(MessageId.latest)
                                        .subscriptionName("PulsarLatestOffsetEstimatorReader")
                                        .topic(topic)
                                        .create();
            this.memoizedBacklog = Suppliers.memoizeWithExpiration(
                    () -> {
                        Message<byte[]> latestMessage = null;
                        try {
                            latestMessage = readerLatestMsg.readNext();
                        } catch (PulsarClientException e) {
                            //TODO change error log
                            e.printStackTrace();
                        }
                        return latestMessage;
                    }, 1,
                    TimeUnit.SECONDS);
        }

        @Override
        protected void finalize() {
            try {
                this.readerLatestMsg.close();
            } catch (IOException e) {
                //TODO add error log
                e.printStackTrace();
            }
        }

        @Override
        public long estimate() {
            Message msg = memoizedBacklog.get();
            return msg.getPublishTime();
        }
    }


    private static Instant ensureTimestampWithinBounds(Instant timestamp) {
        if(timestamp.isBefore(BoundedWindow.TIMESTAMP_MIN_VALUE)) {
            timestamp = BoundedWindow.TIMESTAMP_MIN_VALUE;
        } else if(timestamp.isAfter(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
            timestamp = BoundedWindow.TIMESTAMP_MAX_VALUE;
        }
        return timestamp;
    }

}
