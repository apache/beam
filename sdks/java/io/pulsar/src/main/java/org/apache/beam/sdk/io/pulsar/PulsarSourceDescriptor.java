package org.apache.beam.sdk.io.pulsar;

import javax.annotation.Nullable;
import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldName;
import org.apache.beam.sdk.values.PInput;
import org.apache.pulsar.client.api.MessageId;

import java.io.Serializable;

@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class PulsarSourceDescriptor implements Serializable {

    @SchemaFieldName("topic")
    abstract String getTopic();

    @SchemaFieldName("start_offset")
    @Nullable
    abstract Long getStartOffset();

    @SchemaFieldName("client_url")
    abstract String getClientUrl();

    public static PulsarSourceDescriptor of(
            String topic,
            Long startOffsetTimestamp,
            String clientUrl
    ) {
        return new AutoValue_PulsarSourceDescriptor(
                topic,
                startOffsetTimestamp,
                clientUrl
        );
    }

}
