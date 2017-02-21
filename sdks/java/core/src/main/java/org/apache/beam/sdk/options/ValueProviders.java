package org.apache.beam.sdk.options;

import javax.annotation.Nullable;

/**
 * Created by rfernand on 2/21/17.
 */
public final class ValueProviders {

    // Prevent instantiation.
    private ValueProviders() {}

    /**
     * Null-safe version of {@link ValueProvider#get()}.
     */
    @Nullable
    public static <T> T getValueOrNull(@Nullable ValueProvider<T> provider) {
       return provider == null ? null : provider.get();
    }
}
