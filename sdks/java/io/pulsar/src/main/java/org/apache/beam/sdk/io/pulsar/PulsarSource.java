package org.apache.beam.sdk.io.pulsar;

import javax.annotation.Nullable;
import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldName;
import org.apache.pulsar.client.api.MessageId;

import java.io.Serializable;

@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class PulsarSource implements Serializable {

    @SchemaFieldName("start_offset")
    @Nullable
    abstract Long getStartOffset();

    @SchemaFieldName("topic")
    abstract String getTopic();

    @SchemaFieldName("startReadMessageId")
    abstract MessageId getStartMessageId();


}
