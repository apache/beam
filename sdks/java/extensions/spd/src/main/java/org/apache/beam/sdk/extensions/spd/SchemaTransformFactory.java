package org.apache.beam.sdk.extensions.spd;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public interface SchemaTransformFactory {
    Schema getConfigurationSchema();
    PTransform<PCollection<Row>, PCollection<Row>> createTransform(Row config);
}
