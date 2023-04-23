package org.apache.beam.sdk.io.fileschematransform;

import com.google.auto.service.AutoService;
import org.apache.beam.sdk.expansion.ExternalTransformRegistrar;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.ExternalTransformBuilder;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@AutoService(ExternalTransformRegistrar.class)
public class FileWriteTransformRegistrar implements ExternalTransformRegistrar {

    private static final String URN = "beam:transform:org.apache.beam:file_write:v1";

    @Override
    public Map<String, ExternalTransformBuilder<?, ?, ?>> knownBuilderInstances() {
        return ImmutableMap.of(
                URN,
                new Builder()
        );
    }

    static class Builder implements ExternalTransformBuilder<FileWriteSchemaTransformConfiguration, PCollectionRowTuple, PCollectionRowTuple> {

        @Override
        public PTransform<PCollectionRowTuple, PCollectionRowTuple> buildExternal(FileWriteSchemaTransformConfiguration configuration) {
            FileWriteSchemaTransformProvider provider = new FileWriteSchemaTransformProvider();
            return provider.from(configuration).buildTransform();
        }

    }
}
