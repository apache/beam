package org.apache.beam.sdk.extensions.sql.meta.provider.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;

import java.io.Serializable;

public class GenericRecordToRowTest implements Serializable {
    @Rule
    public transient TestPipeline pipeline = TestPipeline.create();

    private static final Schema 

    GenericRecord g1 = new GenericRecordBuilder(SCHEMA);

    @Test
    public void testConvertsGenericRecordToRow() {
        PCollection<Row> rows = pipeline.apply(Create.of());
    }
}
