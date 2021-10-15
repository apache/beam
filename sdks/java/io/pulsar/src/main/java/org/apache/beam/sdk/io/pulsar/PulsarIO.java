package org.apache.beam.sdk.io.pulsar;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.pulsar.client.api.Message;

import org.checkerframework.checker.nullness.qual.Nullable;

public class PulsarIO {

    private PulsarIO() {}

    public static Read read() {
        return new AutoValue_PulsarIO_Read.Builder()
                .setClientUrl(PulsarIOUtils.SERVICE_URL)
                .build();
    }

    @AutoValue
    public abstract static class Read extends PTransform<PBegin, PCollection<Message>> {

        abstract @Nullable String getClientUrl();
        abstract String getTopic();
        abstract long getStartTimestamp();
        abstract Builder builder();

        @AutoValue.Builder
        abstract static class Builder {
            abstract Builder setClientUrl(String url);
            abstract Builder setTopic(String topic);
            abstract Builder setStartTimestamp(Long timestamp);
            abstract Read build();
        }

        public Read withUrl(String url) {
            return builder().setClientUrl(url).build();
        }

        public Read withTopic(String topic) {
            return builder().setTopic(topic).build();
        }

        public Read withStartTimestamp(Long timestamp) {
            return builder().setStartTimestamp(timestamp).build();
        }

        @Override
        public PCollection<Message> expand(PBegin input) {
            return input
                    .apply(
                            Create.of(
                                    PulsarSourceDescriptor.of(getTopic(), getStartTimestamp())))
                    .apply(
                            ParDo.of(
                                    new ReadFromPulsarDoFn()));
        }
    }
}