package org.apache.beam.sdk.io.pulsar;

import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.GrowableOffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.util.MessageIdUtils;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Supplier;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Suppliers;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

@DoFn.UnboundedPerElement
public class ReadFromPulsarDoFn<K, V> extends DoFn<PulsarSource, PulsarRecord<K,V>> {

    final PulsarClient client;
    private String topic;
    private List<String> topics;
    final PulsarAdmin admin;

    public ReadFromPulsarDoFn(PulsarClient client, PulsarAdmin admin, String topic) {
        this.client = client;
        this.admin = admin;
        this.topic = topic;
    }

    public ReadFromPulsarDoFn(PulsarClient client, PulsarAdmin admin, List<String> topics) {
        this.client = client;
        this.admin = admin;
        this.topics = topics;
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction(@Element PulsarSource pulsarSource) {
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
     *
    @GetSize
    public double getSize(@Element PulsarSource pulsarSource, @Restriction OffsetRange restriction) {
        double estimateRecords =
    }*/

    @ProcessElement
    public ProcessContinuation processElement(
            @Element PulsarRecord pulsarRecord,
            RestrictionTracker<OffsetRange, MessageId> tracker,
            OutputReceiver<PulsarRecord> output) throws IOException {

        long startOffset = tracker.currentRestriction().getFrom();
        long expectedOffset = startOffset;
        MessageId startMessageId = (startOffset != 0) ?
                        MessageIdUtils.getMessageId(startOffset) : MessageId.earliest;

        try(Reader<byte[]> reader = client.newReader().topic(topic).create()) {

            reader.seek(startMessageId);

            while (true) {
                //for()
                Message<byte[]> message = reader.readNext();
                //try {
                    MessageId messageId = message.getMessageId();
                    long currentOffset = MessageIdUtils.getOffset(messageId);
                    // if tracker.tryclaim() return true, sdf must execute work otherwise
                    // doFn must exit processElement() without doing any work associated
                    // or claiming more work
                    if (!tracker.tryClaim(messageId)) {
                        return ProcessContinuation.stop();
                    }
                    PulsarRecord<K, V> newPulsarRecord =
                            new PulsarRecord<>(
                                    message.getTopicName(),
                                    message.getMessageId(),
                                    message.getSequenceId());

                    expectedOffset = currentOffset + 1;

                output.output(newPulsarRecord);
            }
        }

    }

    @NewTracker
    public GrowableOffsetRangeTracker restrictionTracker(@Element PulsarSource pulsarSource,
                                                         @Restriction OffsetRange restriction) {

        PulsarLatestOffsetEstimator offsetEstimator =  new PulsarLatestOffsetEstimator(admin, pulsarSource.getTopic());
        return new GrowableOffsetRangeTracker(restriction.getFrom(), offsetEstimator);

    }

    private static class PulsarLatestOffsetEstimator implements GrowableOffsetRangeTracker.RangeEndEstimator {

        private final PulsarAdmin admin;
        private final Supplier<MessageId> memoizedBacklog;

        private PulsarLatestOffsetEstimator(PulsarAdmin admin,
                                            String topic) {
            this.admin = admin;
            this.memoizedBacklog = Suppliers.memoizeWithExpiration(
                    () -> {
                        MessageId latestMessageIdCommitted = null;
                        try {
                            latestMessageIdCommitted =  admin.topics().getLastMessageId(topic);
                        } catch (PulsarAdminException e) {
                            //TODO change error log
                            e.printStackTrace();
                        }
                        return latestMessageIdCommitted;
                    }, 1,
                    TimeUnit.SECONDS);
        }

        @Override
        public long estimate() {
            long offset = MessageIdUtils.getOffset(memoizedBacklog.get());
            return offset;
        }
    }

}
