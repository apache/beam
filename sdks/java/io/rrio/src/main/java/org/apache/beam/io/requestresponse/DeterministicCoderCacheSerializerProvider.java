package org.apache.beam.io.requestresponse;

import org.apache.beam.sdk.schemas.Schema;

public class DeterministicCoderCacheSerializerProvider implements CacheSerializerProvider {
    @Override
    public String identifier() {
        return null;
    }

    @Override
    public Schema getConfigurationSchema() {
        return null;
    }

    @Override
    public <T> CacheSerializer<T> getSerializer(Class<T> clazz) {
        return null;
    }
}
