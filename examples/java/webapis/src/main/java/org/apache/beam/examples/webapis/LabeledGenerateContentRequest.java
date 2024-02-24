package org.apache.beam.examples.webapis;

import com.google.auto.value.AutoValue;
import com.google.cloud.vertexai.api.GenerateContentRequest;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

import java.util.Map;

@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class LabeledGenerateContentRequest {

    public abstract Map<String, String> getLabels();

    public abstract GenerateContentRequest getRequest();

    @AutoValue.Builder
    public abstract static class Builder {

        public abstract Builder setLabels(Map<String, String> value);
        public abstract Builder setRequest(GenerateContentRequest value);
        public abstract LabeledGenerateContentRequest build();
    }
}
