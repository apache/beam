package org.apache.beam.sdk.extensions.sql.meta.provider.parquet;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.coders.AvroCoder;
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

    private static final Schema sc =
        SchemaBuilder
            .record("HandshakeRequest")
            .namespace("org.apache.avro.ipc")
            .fields()
            .name("clientProtocol")
            .type()
            .nullable()
            .stringType()
            .noDefault()
            .endRecord();

    GenericRecord g1 = new GenericRecordBuilder(sc).set("clientProtocol", "http").build();

    @Test
    public void testConvertsGenericRecordToRow() {
        PCollection<Row> rows =
            pipeline
                .apply("original PCollection<GenericRecord>", Create.of(g1).withCoder(AvroCoder.of(sc)))
                .apply("convert", new ParquetTableProvider.GenericRecordReadConverter());

//        PAssert.that(rows).satisfies()
        pipeline.run();
    }
}
